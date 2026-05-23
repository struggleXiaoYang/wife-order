require('dotenv').config();

const express = require('express');
const path = require('path');
const mysql = require('mysql2/promise');
const cron = require('node-cron');
const bcrypt = require('bcryptjs');
const { sendSms } = require('./services/smsService');
const multer = require('multer');
const crypto = require('crypto');

const compression = require('compression');
const helmet = require('helmet');
const rateLimit = require('express-rate-limit');
const session = require('express-session');
const cacheService = require('./services/cacheService');
const http = require('http');
const { Server } = require('socket.io');

// 图片上传配置
const upload = multer({
  storage: multer.diskStorage({
    destination: path.join(__dirname, 'public', 'uploads'),
    filename: function(req, file, cb) {
      var ext = path.extname(file.originalname).toLowerCase();
      cb(null, Date.now() + '-' + crypto.randomBytes(4).toString('hex') + ext);
    },
  }),
  fileFilter: function(req, file, cb) {
    var ext = path.extname(file.originalname).toLowerCase();
    var allowedExt = /\.(jpg|jpeg|png|gif)$/i.test(ext);
    var allowedMime = ['image/jpeg','image/png','image/gif'].includes(file.mimetype);
    cb(null, allowedExt && allowedMime);
  },
  limits: { fileSize: 5 * 1024 * 1024 },
});

const app = express();
app.set('trust proxy', 1);
var serverStartTime = new Date();
var SITE_LAUNCH_DATE = process.env.SITE_LAUNCH_DATE || '2026-05-15T00:00:00';

// 数据库连接池
const pool = mysql.createPool({
  host: process.env.DB_HOST,
  port: parseInt(process.env.DB_PORT, 10) || 3306,
  user: process.env.DB_USER,
  password: process.env.DB_PASSWORD,
  database: process.env.DB_NAME,
  charset: 'utf8mb4',
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
  enableKeepAlive: true,
  keepAliveInitialDelay: 0,
});

// ===== 通用工具函数 =====
function sanitizeSearch(input, maxLen) {
  maxLen = maxLen || 100;
  if (!input) return '';
  var s = String(input).trim().slice(0, maxLen);
  // 转义 LIKE 通配符
  return s.replace(/[%_\\]/g, '\\$&');
}

function validateScopeCol(col) {
  if (col === 'family_group_id' || col === 'user_id') return col;
  return 'user_id';
}

// ===== 数据库自愈：确保表结构存在 =====
(async function runMigrations() {
  // 确保 orders 表有 archived_at 和 deleted_at 列
  try {
    await pool.execute('ALTER TABLE orders ADD COLUMN archived_at DATETIME NULL');
  } catch (e) {
    if (e.code !== 'ER_DUP_FIELDNAME') console.warn(ts() + ' [MIGRATE] archived_at:', e.message);
  }
  try {
    await pool.execute('ALTER TABLE orders ADD COLUMN deleted_at DATETIME NULL');
  } catch (e) {
    if (e.code !== 'ER_DUP_FIELDNAME') console.warn(ts() + ' [MIGRATE] deleted_at:', e.message);
  }
  try { await pool.execute('ALTER TABLE orders ADD INDEX idx_archived_at (archived_at)'); } catch (e) {}
  try { await pool.execute('ALTER TABLE orders ADD INDEX idx_deleted_at (deleted_at)'); } catch (e) {}
  console.log(ts() + ' [MIGRATE] orders columns ensured');

  try {
    await pool.execute(`CREATE TABLE IF NOT EXISTS dashboard_stats (
      id INT PRIMARY KEY DEFAULT 1,
      total_users INT NOT NULL DEFAULT 0,
      total_families INT NOT NULL DEFAULT 0,
      today_orders INT NOT NULL DEFAULT 0,
      today_completed INT NOT NULL DEFAULT 0,
      active_families INT NOT NULL DEFAULT 0,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
    console.log(ts() + ' [MIGRATE] dashboard_stats table ensured');
  } catch (e) { console.error(ts() + ' [MIGRATE] dashboard_stats error:', e.message); }

  // 管理员密码持久化表
  try {
    await pool.execute(`CREATE TABLE IF NOT EXISTS admin_settings (
      setting_key VARCHAR(64) PRIMARY KEY,
      setting_value TEXT NOT NULL,
      updated_at DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`);
    console.log(ts() + ' [MIGRATE] admin_settings table ensured');
  } catch (e) { console.error(ts() + ' [MIGRATE] admin_settings error:', e.message); }
})();

// ===== 订单生命周期：定时归档任务 =====
function ts() { return new Date().toISOString(); }

// 每小时：completed/rejected 超过 48 小时 → archived
cron.schedule('0 * * * *', async () => {
  console.log(ts() + ' [CRON] archive start');
  try {
    var [r] = await pool.execute(
      `UPDATE orders SET archived_at = NOW()
       WHERE status IN ('completed','rejected')
         AND deleted_at IS NULL
         AND archived_at IS NULL
         AND completed_at IS NOT NULL
         AND completed_at < DATE_SUB(NOW(), INTERVAL 48 HOUR)`
    );
    console.log(ts() + ' [CRON] archive done: ' + r.affectedRows + ' rows');
  } catch (e) { console.error(ts() + ' [CRON] archive ERROR:', e.message); }
});

// 每天凌晨 3 点：archived 超过 90 天 → soft delete
cron.schedule('0 3 * * *', async () => {
  console.log(ts() + ' [CRON] soft-delete start');
  try {
    var [r] = await pool.execute(
      `UPDATE orders SET deleted_at = NOW()
       WHERE archived_at IS NOT NULL
         AND deleted_at IS NULL
         AND archived_at < DATE_SUB(NOW(), INTERVAL 90 DAY)`
    );
    console.log(ts() + ' [CRON] soft-delete done: ' + r.affectedRows + ' rows');
  } catch (e) { console.error(ts() + ' [CRON] soft-delete ERROR:', e.message); }
});

// 每天凌晨 2 点：汇总昨日菜品销量到 dish_daily_stats
cron.schedule('0 2 * * *', async () => {
  console.log(ts() + ' [CRON] stats-rollup start');
  try {
    await pool.execute(
      `INSERT INTO dish_daily_stats (dish_id, stat_date, order_count)
       SELECT oi.dish_id, DATE_SUB(CURDATE(), INTERVAL 1 DAY), COUNT(*)
       FROM order_items oi JOIN orders o ON oi.order_id = o.id
       WHERE o.created_at >= DATE_SUB(CURDATE(), INTERVAL 1 DAY) AND o.created_at < CURDATE() AND o.deleted_at IS NULL
       GROUP BY oi.dish_id ON DUPLICATE KEY UPDATE order_count = VALUES(order_count)`
    );
    console.log(ts() + ' [CRON] stats-rollup done');
  } catch (e) { console.error(ts() + ' [CRON] stats-rollup ERROR:', e.message); }
});

// 每5分钟：刷新 dashboard_stats 聚合表
cron.schedule('*/5 * * * *', async () => {
  try {
    await pool.execute('INSERT INTO dashboard_stats (id, total_users, total_families, today_orders, today_completed, active_families) VALUES (1, (SELECT COUNT(*) FROM users), (SELECT COUNT(*) FROM family_groups), (SELECT COUNT(*) FROM orders WHERE deleted_at IS NULL AND DATE(created_at)=CURDATE()), (SELECT COUNT(*) FROM orders WHERE deleted_at IS NULL AND status=\"completed\" AND DATE(created_at)=CURDATE()), (SELECT COUNT(DISTINCT family_group_id) FROM orders WHERE deleted_at IS NULL AND family_group_id IS NOT NULL AND created_at>=DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY))) ON DUPLICATE KEY UPDATE total_users=VALUES(total_users), total_families=VALUES(total_families), today_orders=VALUES(today_orders), today_completed=VALUES(today_completed), active_families=VALUES(active_families), updated_at=NOW()');
    cacheService.del('dash_stats');
  } catch (e) { console.error(ts() + ' [CRON] stats-refresh ERROR:', e.message); }
});

// 启动时初始化：如果聚合表为空，立即填充一次
setTimeout(async () => {
  try {
    var [[row]] = await pool.execute('SELECT total_users FROM dashboard_stats WHERE id=1');
    if (!row || row.total_users === null) {
      console.log(ts() + ' [INIT] dashboard_stats empty, populating...');
      await pool.execute('INSERT INTO dashboard_stats (id, total_users, total_families, today_orders, today_completed, active_families) VALUES (1, (SELECT COUNT(*) FROM users), (SELECT COUNT(*) FROM family_groups), (SELECT COUNT(*) FROM orders WHERE deleted_at IS NULL AND DATE(created_at)=CURDATE()), (SELECT COUNT(*) FROM orders WHERE deleted_at IS NULL AND status=\"completed\" AND DATE(created_at)=CURDATE()), (SELECT COUNT(DISTINCT family_group_id) FROM orders WHERE deleted_at IS NULL AND family_group_id IS NOT NULL AND created_at>=DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY))) ON DUPLICATE KEY UPDATE total_users=VALUES(total_users), total_families=VALUES(total_families), today_orders=VALUES(today_orders), today_completed=VALUES(today_completed), active_families=VALUES(active_families)');
      console.log(ts() + ' [INIT] dashboard_stats populated');
    }
  } catch (e) { /* table may not exist yet */ }
}, 3000);

// ===== 系统级错误监控 =====
process.on('uncaughtException', function(err) {
  console.error(ts() + ' [FATAL] uncaughtException:', err.message);
  console.error(err.stack);
});

process.on('unhandledRejection', function(reason) {
  console.error(ts() + ' [FATAL] unhandledRejection:', reason);
  if (reason && reason.stack) console.error(reason.stack);
  process.exitCode = 1;
  setTimeout(function() { process.exit(1); }, 1000);
});

// 1. HTTP 压缩（第一个中间件，阈值 1KB）
app.use(compression({ threshold: 1024 }));

// 2. 安全响应头 + CSP（过渡期保留 unsafe-inline，后续迁移至 nonce）
app.use(helmet({
  contentSecurityPolicy: {
    directives: {
      defaultSrc: ["'self'"],
      scriptSrc: ["'self'", "'unsafe-inline'"],
      styleSrc: ["'self'", "'unsafe-inline'"],
      imgSrc: ["'self'", "data:"],
      connectSrc: ["'self'"],
    },
  },
}));
app.disable('x-powered-by');

// 3. 静态资源缓存
app.use(express.static(path.join(__dirname, 'public'), {
  maxAge: '7d',
  etag: true,
  lastModified: true,
  setHeaders: function(res, filePath) {
    if (/\.(jpg|jpeg|png|gif|svg|webp)$/i.test(filePath)) {
      res.setHeader('Cache-Control', 'public, max-age=2592000'); // 图片 30 天
    } else if (/\.(html|ejs)$/i.test(filePath)) {
      res.setHeader('Cache-Control', 'no-cache'); // 模板不缓存
    }
  },
}));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));

// 4. API 限流
var loginLimiter = rateLimit({ windowMs: 60000, max: 10, message: { error: '请求过于频繁，请稍后重试' } });
var registerLimiter = rateLimit({ windowMs: 60000, max: 5, message: { error: '请求过于频繁，请稍后重试' } });
var dashboardLimiter = rateLimit({ windowMs: 60000, max: 5, message: { error: '请求过于频繁，请稍后重试' } });
var dashboardApiLimiter = rateLimit({ windowMs: 60000, max: 30, message: { error: '请求过于频繁，请稍后重试' } });
var forgotPasswordLimiter = rateLimit({ windowMs: 60000, max: 3, message: { success: false, message: '请求过于频繁，请稍后重试' } });
var bindApplyLimiter = rateLimit({ windowMs: 60000, max: 5, message: { error: '请求过于频繁，请稍后重试' } });
var smsLimiter = rateLimit({ windowMs: 60000, max: 1, message: { success: false, message: '请求过于频繁，请稍后重试' } });
// 会话安全：SECRET 必须由环境变量提供，不允许硬编码兜底
if (!process.env.SESSION_SECRET) {
  console.error('致命错误：缺少 SESSION_SECRET 环境变量，服务拒绝启动');
  process.exit(1);
}
var sessionMiddleware = session({
  secret: process.env.SESSION_SECRET,
  resave: false,
  saveUninitialized: false,
  cookie: {
    maxAge: 7 * 24 * 60 * 60 * 1000,
    secure: process.env.NODE_ENV === 'production',
    sameSite: 'lax',
    httpOnly: true,
  },
  rolling: true,
});
app.use(sessionMiddleware);

// CSRF 防护中间件
app.use(function(req, res, next) {
  if (!req.session.csrfToken) {
    req.session.csrfToken = crypto.randomBytes(32).toString('hex');
  }
  res.locals.csrfToken = req.session.csrfToken;
  res.setHeader('X-CSRF-Token', req.session.csrfToken);
  next();
});

function csrfCheck(req, res, next) {
  var token = req.headers['x-csrf-token'] || req.body._csrf;
  if (!token || token !== req.session.csrfToken) {
    return res.status(403).json({ error: 'CSRF 校验失败' });
  }
  next();
}

// 用户信息注入模板
app.use((req, res, next) => {
  if (req.session && req.session.userId) {
    var phone = req.session.userPhone || '';
    res.locals.user = {
      phone: phone,
      phoneMasked: phone.length >= 11
        ? phone.substring(0, 3) + '****' + phone.substring(7)
        : phone,
      isAdmin: !!req.session.isAdmin,
      role: req.session.userRole || 'member',
      familyGroupId: req.session.familyGroupId || null,
    };
  }
  next();
});

// 用户鉴权 + 数据范围中间件
async function requireAuth(req, res, next) {
  if (!req.session || !req.session.userId) {
    return res.status(401).json({ error: '请先登录', code: 'UNAUTHORIZED' });
  }
  // 实时查 DB 获取 family_group_id，确保绑定后立即生效
  try {
    var [[userRow]] = await pool.execute('SELECT family_group_id FROM users WHERE id = ?', [req.session.userId]);
    var fgId = userRow ? userRow.family_group_id : null;
    if (fgId) req.session.familyGroupId = fgId;
    req.scopeColumn = fgId ? 'family_group_id' : 'user_id';
    req.scopeValue = fgId || req.session.userId;
  } catch (_) {
    req.scopeColumn = 'user_id';
    req.scopeValue = req.session.userId;
  }
  next();
}

function requireChef(req, res, next) {
  if (req.session.userRole !== 'chef') {
    return res.status(403).json({ error: '仅大厨可执行此操作' });
  }
  next();
}

app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// ========== 发送短信验证码 ==========
app.post('/api/sendSms', smsLimiter, async (req, res) => {
  try {
    const { phone } = req.body;

    if (!phone || !/^1\d{10}$/.test(phone)) {
      return res.json({ success: false, message: '手机号格式错误' });
    }

    // 60秒内仅可发送一次
    const [latestRows] = await pool.execute(
      'SELECT created_at FROM sms_codes WHERE phone = ? ORDER BY created_at DESC LIMIT 1',
      [phone]
    );
    if (latestRows.length > 0) {
      var elapsed = Date.now() - new Date(latestRows[0].created_at).getTime();
      if (elapsed < 60000) {
        var remain = Math.ceil((60000 - elapsed) / 1000);
        return res.json({ success: false, message: '请' + remain + '秒后再试' });
      }
    }

    // 每日每号最多5条
    const [cntRows] = await pool.execute(
      'SELECT COUNT(*) AS cnt FROM sms_codes WHERE phone = ? AND created_at >= CURDATE()',
      [phone]
    );
    if (cntRows[0].cnt >= 5) {
      return res.json({ success: false, message: '今日验证码发送已达上限（5条）' });
    }

    // 生成6位随机验证码
    var code = String(Math.floor(Math.random() * 900000 + 100000));

    // 调用互亿无线发送（测试模式跳过）
    if (process.env.TEST_SKIP_SMS === 'true') {
      console.log('[测试] 跳过短信发送，验证码长度:' + (code ? code.length : 0) + ' 手机尾号:' + (phone ? '****' + phone.slice(-4) : '***'));
    } else {
      var smsResult = await sendSms(phone, code);
      if (!smsResult.success) {
        return res.json({ success: false, message: smsResult.error || '短信发送失败' });
      }
    }

    // 写入数据库
    await pool.execute(
      'INSERT INTO sms_codes (phone, code, expires_at, created_at) VALUES (?, ?, DATE_ADD(NOW(), INTERVAL 5 MINUTE), NOW())',
      [phone, code]
    );

    res.json({ success: true, message: '验证码已发送' });
  } catch (err) {
    console.error('/api/sendSms error:', err);
    res.status(500).json({ success: false, message: '服务器错误' });
  }
});

// ========== 用户注册 ==========
app.post('/api/register', registerLimiter, async (req, res) => {
  try {
    const { phone, password, code, role, inviteCode } = req.body;
    var userRole = (role === 'chef' || role === 'member') ? role : 'member';
    var inviteCodeTrim = (inviteCode || '').trim();

    if (!phone || !/^1\d{10}$/.test(phone)) {
      return res.json({ success: false, message: '手机号格式错误，请输入11位以1开头的手机号' });
    }
    if (!password || password.length < 6) {
      return res.json({ success: false, message: '密码至少需要6位' });
    }
    if (!code || code.length !== 6) {
      return res.json({ success: false, message: '请输入6位验证码' });
    }

    // 校验验证码（事务 + FOR UPDATE 防止并发 race）
    var smsConn = await pool.getConnection();
    try {
      await smsConn.beginTransaction();
      const [codeRows] = await smsConn.execute(
        'SELECT id, code FROM sms_codes WHERE phone = ? AND used = 0 AND expires_at > NOW() ORDER BY created_at DESC LIMIT 1 FOR UPDATE',
        [phone]
      );
      if (codeRows.length === 0) {
        await smsConn.rollback();
        smsConn.release();
        return res.json({ success: false, message: '验证码错误或已过期' });
      }
      if (codeRows[0].code !== code) {
        await smsConn.rollback();
        smsConn.release();
        return res.json({ success: false, message: '验证码错误或已过期' });
      }
      await smsConn.execute('UPDATE sms_codes SET used = 1 WHERE id = ?', [codeRows[0].id]);
      await smsConn.commit();
      smsConn.release();
    } catch (e) {
      await smsConn.rollback().catch(() => {});
      smsConn.release();
      throw e;
    }

    const [rows] = await pool.execute(
      'SELECT id FROM users WHERE phone = ?',
      [phone]
    );
    if (rows.length > 0) {
      return res.json({ success: false, message: '手机号已注册' });
    }

    // 邀请码校验：如果填了，必须能找到对应家庭组
    var targetFgId = null;
    if (inviteCodeTrim) {
      var [fgCheck] = await pool.execute('SELECT id FROM family_groups WHERE invite_code = ?', [inviteCodeTrim]);
      if (fgCheck.length === 0) {
        return res.json({ success: false, message: '邀请码无效，请确认后再试' });
      }
      targetFgId = fgCheck[0].id;
    }

    const password_hash = await bcrypt.hash(password, 10);

    const conn = await pool.getConnection();
    await conn.beginTransaction();
    try {
      var familyGroupId = targetFgId || null;

      const [userResult] = await conn.execute(
        'INSERT INTO users (phone, password_hash, role, family_group_id, created_at) VALUES (?, ?, ?, ?, NOW())',
        [phone, password_hash, userRole, familyGroupId]
      );
      const userId = userResult.insertId;

      // chef 注册（无邀请码）→ 自动创建家庭组 + 默认数据
      if (!inviteCodeTrim) {
        var randomCode = crypto.randomBytes(4).toString('hex').toUpperCase();
        var [fgResult] = await conn.execute(
          'INSERT INTO family_groups (owner_id, name, invite_code, created_at) VALUES (?, ?, ?, NOW())',
          [userId, '我们的家', randomCode]
        );
        familyGroupId = fgResult.insertId;
        await conn.execute('UPDATE users SET family_group_id = ? WHERE id = ?', [familyGroupId, userId]);
      }

      // 只有创建者才插入默认分类和菜品（加入已有家庭组则共享）
      if (!inviteCodeTrim) {

      // 插入3条默认分类
      const catResult1 = await conn.execute(
        'INSERT INTO categories (name, user_id, family_group_id, sort_order) VALUES (?, ?, ?, ?)',
        ['家常菜', userId, familyGroupId, 0]
      );
      const catResult2 = await conn.execute(
        'INSERT INTO categories (name, user_id, family_group_id, sort_order) VALUES (?, ?, ?, ?)',
        ['海鲜', userId, familyGroupId, 1]
      );
      const catResult3 = await conn.execute(
        'INSERT INTO categories (name, user_id, family_group_id, sort_order) VALUES (?, ?, ?, ?)',
        ['素菜', userId, familyGroupId, 2]
      );

      const catIds = {
        '家常菜': catResult1[0].insertId,
        '海鲜': catResult2[0].insertId,
        '素菜': catResult3[0].insertId,
      };

      // 插入5条示例菜品
      const dishes = [
        { name: '红烧肉', category: '家常菜', ingredients: ['五花肉', '冰糖', '八角', '桂皮', '生抽', '老抽', '料酒', '葱姜'], steps: ['五花肉洗净切块，冷水下锅焯水捞出', '锅中放少许油，加冰糖小火炒至焦糖色', '放入五花肉翻炒上色', '加八角、桂皮、生抽、老抽、料酒', '加开水没过肉块，放葱姜', '小火炖40分钟，大火收汁'] },
        { name: '番茄炒蛋', category: '家常菜', ingredients: ['番茄', '鸡蛋', '葱花', '盐', '糖', '食用油'], steps: ['番茄切块，鸡蛋打散加少许盐', '热锅倒油，炒熟鸡蛋盛出', '锅中加油，炒软番茄', '倒入鸡蛋翻炒，加盐和糖调味', '撒葱花出锅'] },
        { name: '清蒸鲈鱼', category: '海鲜', ingredients: ['鲈鱼', '姜', '葱', '蒸鱼豉油', '料酒', '盐'], steps: ['鲈鱼处理干净，鱼身划几刀', '抹盐和料酒腌制10分钟', '盘底铺姜片，放上鱼', '蒸锅上汽后蒸8-10分钟', '倒掉蒸出的汁水', '铺上葱丝，淋热油和蒸鱼豉油'] },
        { name: '酸辣土豆丝', category: '素菜', ingredients: ['土豆', '干辣椒', '花椒', '醋', '盐', '蒜', '食用油'], steps: ['土豆切细丝，清水浸泡去淀粉', '热锅倒油，爆香花椒干辣椒和蒜末', '下土豆丝大火快炒', '加盐和醋调味', '翻炒均匀即可出锅'] },
        { name: '蒜蓉西兰花', category: '素菜', ingredients: ['西兰花', '蒜', '蚝油', '盐', '食用油'], steps: ['西兰花掰小朵，焯水1分钟捞出', '蒜切末', '热锅倒油，炒香蒜末', '放入西兰花翻炒', '加蚝油和盐调味，翻炒均匀'] },
      ];

      for (var i = 0; i < dishes.length; i++) {
        var d = dishes[i];
        await conn.execute(
          'INSERT INTO dishes (name, category_id, image, ingredients, steps, is_available, user_id, family_group_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
          [d.name, catIds[d.category], '', JSON.stringify(d.ingredients), JSON.stringify(d.steps), 1, userId, familyGroupId]
        );
      }

      } // end if (!inviteCodeTrim)

      await conn.commit();
      res.json({ success: true, message: '注册成功' });
    } catch (err) {
      await conn.rollback();
      throw err;
    } finally {
      conn.release();
    }
  } catch (err) {
    console.error('/api/register error:', err);
    res.status(500).json({ success: false, message: '服务器错误' });
  }
});

// ========== 用户登录 ==========
app.post('/api/login', loginLimiter, async (req, res) => {
  try {
    const { phone, password } = req.body;

    if (!phone || !password) {
      return res.json({ success: false, message: '手机号或密码错误' });
    }

    const [rows] = await pool.execute(
      'SELECT id, phone, password_hash, role, family_group_id FROM users WHERE phone = ?',
      [phone]
    );
    if (rows.length === 0) {
      return res.json({ success: false, message: '手机号或密码错误' });
    }

    const user = rows[0];
    const match = await bcrypt.compare(password, user.password_hash);
    if (!match) {
      return res.json({ success: false, message: '手机号或密码错误' });
    }

    await pool.execute(
      'UPDATE users SET last_login_at = NOW() WHERE id = ?',
      [user.id]
    );

    // 防止会话固定：登录后重新生成 session ID
    req.session.regenerate(function(err) {
      if (err) {
        console.error('Session regenerate error:', err);
        return res.status(500).json({ success: false, message: '服务器错误' });
      }
      req.session.userId = user.id;
      req.session.userPhone = user.phone;
      req.session.userRole = user.role || 'member';
      req.session.familyGroupId = user.family_group_id || null;

      res.json({ success: true, message: '登录成功' });
    });
  } catch (err) {
    console.error('/api/login error:', err);
    res.status(500).json({ success: false, message: '服务器错误' });
  }
});

// ========== 首页 ==========
app.get('/', async (req, res) => {
  if (!req.session.userId) {
    return res.render('welcome');
  }
  try {
    const userRole = req.session.userRole || 'member';

    // 并行：取 family_group_id + scope 基础查询
    var [[fgUser]] = await pool.execute('SELECT family_group_id FROM users WHERE id = ?', [req.session.userId]);
    var familyGroupId = fgUser.family_group_id;
    if (familyGroupId) req.session.familyGroupId = familyGroupId;
    var scopeColumn = familyGroupId ? 'family_group_id' : 'user_id';
    var scopeValue = familyGroupId || req.session.userId;

    // 并行：dishes + inventory + 家庭成员（互不依赖）
    var parallelResults = await Promise.all([
      pool.execute(
        'SELECT d.id, d.name, d.image, d.ingredients, d.steps, d.is_available, d.avg_rating, COALESCE(c.name, \'\') AS category FROM dishes d LEFT JOIN categories c ON d.category_id = c.id WHERE d.' + scopeColumn + ' = ? ORDER BY d.avg_rating DESC, d.id',
        [scopeValue]
      ),
      pool.execute(
        'SELECT ingredient_name, quantity FROM inventory WHERE ' + scopeColumn + ' = ?',
        [scopeValue]
      ),
      familyGroupId
        ? Promise.all([
            pool.execute('SELECT phone, role FROM users WHERE family_group_id = ? ORDER BY created_at', [familyGroupId]),
            pool.execute('SELECT name FROM family_groups WHERE id = ?', [familyGroupId])
          ])
        : Promise.resolve([[[],[]], [[{},{}]]]) // 无家庭组时返回空结构
    ]);

    var [rows] = parallelResults[0];
    var [invRows] = parallelResults[1];
    var famResult = parallelResults[2]; // [memberRows, fgRow]

    var stockMap = {};
    invRows.forEach(function(inv) { stockMap[inv.ingredient_name] = inv.quantity; });

    var dishes = rows.map(function(r) {
      var ingredients = [], steps = [];
      try { ingredients = JSON.parse(r.ingredients || '[]'); } catch (_) {}
      try { steps = JSON.parse(r.steps || '[]'); } catch (_) {}
      var isSoldOut = false;
      if (ingredients.length > 0 && Object.keys(stockMap).length > 0) {
        isSoldOut = ingredients.some(function(ing) { return stockMap.hasOwnProperty(ing) && stockMap[ing] <= 0; });
      }
      return { id: r.id, name: r.name, category: r.category, image: r.image || '', ingredients: ingredients, steps: steps, isAvailable: !!r.is_available && !isSoldOut, isSoldOut: isSoldOut, avgRating: Number(r.avg_rating) || 0 };
    });

    // 家庭成员数据
    var familyMembers = [];
    var familyGroupName = '';
    if (familyGroupId && famResult) {
      var [memberRows] = famResult[0];
      familyMembers = memberRows.map(function(r) { return { phone: r.phone, role: r.role }; });
      var [[fgRow]] = famResult[1];
      familyGroupName = fgRow ? fgRow.name : '';
    }

    // 大厨额外数据：并行 orders + dishCount + monthOrders + categories，然后串行 order_items
    var chefData = {};
    if (userRole === 'chef') {
      var chefParallel = await Promise.all([
        pool.execute('SELECT o.id, o.status, o.reject_reason, o.created_at FROM orders o WHERE o.' + scopeColumn + ' = ? AND o.deleted_at IS NULL ORDER BY o.created_at DESC', [scopeValue]),
        pool.execute('SELECT COUNT(*) AS cnt FROM dishes WHERE ' + scopeColumn + ' = ?', [scopeValue]),
        pool.execute('SELECT COUNT(*) AS cnt FROM orders WHERE ' + scopeColumn + ' = ? AND deleted_at IS NULL AND YEAR(created_at)=YEAR(NOW()) AND MONTH(created_at)=MONTH(NOW())', [scopeValue]),
        pool.execute('SELECT name FROM categories WHERE ' + scopeColumn + ' = ? ORDER BY sort_order, id', [scopeValue])
      ]);

      var [orderRows] = chefParallel[0];
      var [[{ cnt: dishCount }]] = chefParallel[1];
      var [[{ cnt: monthOrders }]] = chefParallel[2];
      var [catRows] = chefParallel[3];

      var orderIds = orderRows.map(function(o) { return o.id; });
      var itemsMap = {};
      if (orderIds.length > 0) {
        var [itemRows] = await pool.execute(
          'SELECT order_id, dish_name, remark FROM order_items WHERE order_id IN (' + orderIds.map(function() { return '?'; }).join(',') + ')',
          orderIds
        );
        itemRows.forEach(function(it) { if (!itemsMap[it.order_id]) itemsMap[it.order_id] = []; itemsMap[it.order_id].push(it); });
      }
      var orders = orderRows.map(function(o) {
        var items = itemsMap[o.id] || [];
        return { id: o.id.toString(), status: o.status, dishNames: items.map(function(i) { return i.dish_name; }), remarks: items.map(function(i) { return i.remark || ''; }), rejectReason: o.reject_reason || '', createdAt: new Date(o.created_at).toISOString() };
      });

      chefData = { orders: orders, dishCount: dishCount, monthOrders: monthOrders, categories: catRows.map(function(c) { return c.name; }), dishes: dishes };
    }

    res.render('index', { dishes: dishes, chefData: chefData, familyMembers: familyMembers, familyGroupName: familyGroupName });
  } catch (err) {
    console.error('GET / error:', err);
    res.status(500).send('服务器错误');
  }
});

// ========== 菜单数据（分类列表） ==========
app.get('/api/menu', requireAuth, async (req, res) => {
  try {
    var scCol = req.scopeColumn || 'user_id';
    var scVal = req.scopeValue || req.session.userId;
    // 缓存：key 按 scope 区分
    var cacheKey = 'menu_' + scVal;
    var cached = cacheService.get(cacheKey);
    if (cached) return res.json(cached);

    const [rows] = await pool.execute(
      `SELECT name FROM categories WHERE ${scCol} = ? ORDER BY sort_order, id`,
      [scVal]
    );
    var data = { categories: rows.map(function(r) { return r.name; }) };
    cacheService.set(cacheKey, data, 30);
    res.json(data);
  } catch (err) {
    console.error('GET /api/menu error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// ========== 提交订单（下单） ==========
app.post('/api/order', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;
    const { items, dishIds } = req.body;

    // 查 DB 获取最新 family_group_id
    var [[fgRow]] = await pool.execute('SELECT family_group_id FROM users WHERE id = ?', [userId]);
    var familyGroupId = fgRow.family_group_id;
    if (familyGroupId) req.session.familyGroupId = familyGroupId;
    var scopeColumn = familyGroupId ? 'family_group_id' : 'user_id';
    var scopeValue = familyGroupId || userId;

    var orderItems;
    if (Array.isArray(items) && items.length > 0) {
      orderItems = items;
    } else if (Array.isArray(dishIds) && dishIds.length > 0) {
      orderItems = dishIds.map(function(id) { return { dishId: id, remark: '' }; });
    } else {
      return res.status(400).json({ error: '请至少选一道菜' });
    }

    var ids = orderItems.map(function(it) { return parseInt(it.dishId, 10); });

    const [dishes] = await pool.execute(
      `SELECT id, name, ingredients FROM dishes WHERE id IN (${ids.map(function() { return '?'; }).join(',')}) AND ${scopeColumn} = ?`,
      ids.concat([scopeValue])
    );

    const [invRows] = await pool.execute(
      `SELECT ingredient_name, quantity FROM inventory WHERE ${scopeColumn} = ?`,
      [scopeValue]
    );
    var stockMap = {};
    invRows.forEach(function(inv) { stockMap[inv.ingredient_name] = inv.quantity; });

    if (Object.keys(stockMap).length > 0) {
      for (var k = 0; k < dishes.length; k++) {
        var d = dishes[k];
        var ings = [];
        try { ings = JSON.parse(d.ingredients || '[]'); } catch (_) {}
        for (var m = 0; m < ings.length; m++) {
          if (stockMap.hasOwnProperty(ings[m]) && stockMap[ings[m]] <= 0) {
            return res.json({ success: false, message: '「' + d.name + '」已售罄（' + ings[m] + '库存不足）' });
          }
        }
      }
    }

    // 事务：下单 + 扣库存原子执行
    var conn = await pool.getConnection();
    try {
      await conn.beginTransaction();

      // 插入订单
      const [result] = await conn.execute(
        'INSERT INTO orders (user_id, family_group_id, status) VALUES (?, ?, ?)',
        [req.session.userId, familyGroupId || null, 'pending']
      );
      const orderId = result.insertId;

      // 批量插入订单项
      const dishMap = {};
      dishes.forEach(function(d) { dishMap[d.id] = d.name; });

      for (var i = 0; i < orderItems.length; i++) {
        var it = orderItems[i];
        var dishId = parseInt(it.dishId, 10);
        await conn.execute(
          'INSERT INTO order_items (order_id, dish_id, dish_name, remark) VALUES (?, ?, ?, ?)',
          [orderId, dishId, dishMap[dishId] || '未知菜品', (it.remark || '').substring(0, 200)]
        );
      }

      // 扣减库存
      if (Object.keys(stockMap).length > 0) {
        var deductSet = {};
        dishes.forEach(function(dd) {
          var ings = [];
          try { ings = JSON.parse(dd.ingredients || '[]'); } catch (_) {}
          ings.forEach(function(ing) { deductSet[ing] = true; });
        });
        for (var ingName in deductSet) {
          if (stockMap.hasOwnProperty(ingName)) {
            await conn.execute(
              `UPDATE inventory SET quantity = GREATEST(quantity - 1, 0) WHERE ${scopeColumn} = ? AND ingredient_name = ?`,
              [scopeValue, ingName]
            );
          }
        }
      }

      await conn.commit();

      const [[orderRow]] = await pool.execute(
        'SELECT id, created_at FROM orders WHERE id = ?',
        [orderId]
      );

      // 解析食材，去重合并
      var ingredientSet = {};
      dishes.forEach(function(d) {
        if (!d.ingredients) return;
        var raw = '';
        try { raw = JSON.parse(d.ingredients).join('、'); } catch (_) { raw = d.ingredients; }
        raw.split(/[、,，]/).forEach(function(s) {
          var t = s.trim();
          if (t) ingredientSet[t] = true;
        });
      });
      var shoppingList = Object.keys(ingredientSet);

      res.json({
        success: true,
        order: {
          id: orderRow.id.toString(),
          dishIds: ids.map(function(id) { return id.toString(); }),
          items: orderItems,
          createdAt: new Date(orderRow.created_at).toISOString(),
          status: 'pending',
        },
        shoppingList: shoppingList,
      });
      // Socket.IO: 通知家庭组成员有新订单
      if (familyGroupId) {
        io.to('fg_' + familyGroupId).emit('new_order', { orderId: orderId.toString() });
        invalidateCache(scopeValue);
      }
    } catch (txErr) {
      await conn.rollback();
      throw txErr;
    } finally {
      conn.release();
    }
  } catch (err) {
    console.error('POST /api/order error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// ========== 客户端历史订单 API ==========
app.get('/api/orders', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;
    var [[fgUser]] = await pool.execute('SELECT family_group_id FROM users WHERE id = ?', [userId]);
    var scCol = fgUser.family_group_id ? 'family_group_id' : 'user_id';
    var scVal = fgUser.family_group_id || userId;

    var page = parseInt(req.query.page, 10) || 1;
    var limit = parseInt(req.query.limit, 10) || 10;
    var offset = (page - 1) * limit;
    var days = parseInt(req.query.days, 10) || 0;
    var search = sanitizeSearch(req.query.search, 100);

    // 构建 WHERE 条件：若有家庭组，同时包含加入前的个人订单
    var where;
    var params = [];
    if (fgUser.family_group_id) {
      where = 'WHERE (o.family_group_id = ? OR (o.user_id = ? AND o.family_group_id IS NULL)) AND o.deleted_at IS NULL';
      params = [fgUser.family_group_id, userId];
    } else {
      where = 'WHERE o.user_id = ? AND o.deleted_at IS NULL';
      params = [userId];
    }

    if (days > 0) {
      where += ' AND o.created_at >= DATE_SUB(NOW(), INTERVAL ' + days + ' DAY)';
    }

    // 按菜品名搜索：需要 JOIN order_items
    var needJoin = search.length > 0;
    if (needJoin) {
      where += ' AND o.id IN (SELECT DISTINCT oi.order_id FROM order_items oi WHERE oi.dish_name LIKE ?)';
      params.push('%' + search + '%');
    }

    // 总数
    var [[{ cnt: total }]] = await pool.execute(
      'SELECT COUNT(*) AS cnt FROM orders o ' + where, params
    );

    // 分页查询
    var [orderRows] = await pool.execute(
      'SELECT o.id, o.status, o.reject_reason, o.accepted_at, o.completed_at, o.created_at FROM orders o '
      + where + ' ORDER BY o.created_at DESC LIMIT ' + limit + ' OFFSET ' + offset,
      params
    );

    if (orderRows.length === 0) {
      return res.json({ orders: [], total: total, page: page, limit: limit, totalPages: Math.ceil(total / limit), ratedMap: {} });
    }

    var orderIds = orderRows.map(function(o) { return o.id; });
    var [itemRows] = await pool.execute(
      'SELECT order_id, dish_id, dish_name, remark FROM order_items WHERE order_id IN (' +
        orderIds.map(function() { return '?'; }).join(',') + ')',
      orderIds
    );

    var itemsMap = {};
    itemRows.forEach(function(item) {
      if (!itemsMap[item.order_id]) itemsMap[item.order_id] = [];
      itemsMap[item.order_id].push(item);
    });

    var orders = orderRows.map(function(o) {
      var items = itemsMap[o.id] || [];
      return {
        id: o.id.toString(),
        dishIds: items.map(function(i) { return i.dish_id !== null ? i.dish_id.toString() : ''; }),
        dishNames: items.map(function(i) { return i.dish_name; }),
        remarks: items.map(function(i) { return i.remark || ''; }),
        createdAt: new Date(o.created_at).toISOString(),
        status: o.status,
        rejectReason: o.reject_reason || '',
        acceptedAt: o.accepted_at ? new Date(o.accepted_at).toISOString() : '',
        completedAt: o.completed_at ? new Date(o.completed_at).toISOString() : '',
      };
    });

    // 查询用户已评分的菜品（按订单+菜品唯一）
    var [ratedRows] = await pool.execute(
      'SELECT dish_id, order_id, rating FROM ratings WHERE user_id = ?',
      [req.session.userId]
    );
    var ratedMap = {};
    ratedRows.forEach(function(r) { ratedMap[r.order_id + '_' + r.dish_id] = r.rating; });

    res.json({ orders: orders, total: total, page: page, limit: limit, totalPages: Math.ceil(total / limit), ratedMap: ratedMap });
  } catch (err) {
    console.error('GET /api/orders error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// ========== 库存管理 ==========
app.get('/api/inventory', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    const [rows] = await pool.execute(
      `SELECT id, ingredient_name, quantity, unit FROM inventory WHERE ${scopeCol} = ? ORDER BY ingredient_name`,
      [scopeVal]
    );
    res.json({ items: rows });
  } catch (err) {
    console.error('GET /api/inventory error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.post('/api/inventory', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;
    var name = (req.body.ingredient_name || '').trim();
    if (!name) return res.json({ success: false, message: '食材名不能为空' });
    var fgId = (scopeCol === 'family_group_id') ? scopeVal : null;
    await pool.execute(
      'INSERT INTO inventory (user_id, ingredient_name, family_group_id, quantity, unit) VALUES (?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE quantity = quantity + VALUES(quantity)',
      [userId, name.slice(0, 100), fgId, parseFloat(req.body.quantity) || 0, req.body.unit || '份']
    );
    res.json({ success: true });
  } catch (err) {
    console.error('POST /api/inventory error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.put('/api/inventory/:id', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    const [result] = await pool.execute(
      `UPDATE inventory SET quantity = ?, unit = ? WHERE id = ? AND ${scopeCol} = ?`,
      [parseFloat(req.body.quantity) || 0, req.body.unit || '份', req.params.id, scopeVal]
    );
    if (result.affectedRows === 0) return res.json({ success: false, message: '记录不存在' });
    res.json({ success: true });
  } catch (err) {
    console.error('PUT /api/inventory/:id error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.delete('/api/inventory/:id', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    const [result] = await pool.execute(
      `DELETE FROM inventory WHERE id = ? AND ${scopeCol} = ?`,
      [req.params.id, scopeVal]
    );
    if (result.affectedRows === 0) return res.json({ success: false, message: '记录不存在' });
    res.json({ success: true });
  } catch (err) {
    console.error('DELETE /api/inventory/:id error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// ========== 菜品评分 ==========
app.post('/api/dishes/:id/rate', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;
    const dishId = parseInt(req.params.id, 10);
    const score = parseInt(req.body.score, 10);
    const orderId = parseInt(req.body.orderId, 10) || 0;

    if (!score || score < 1 || score > 5) {
      return res.json({ success: false, message: '评分需在1-5之间' });
    }

    // 插入或更新评分：联合唯一键 (user_id, order_id, dish_id)
    await pool.execute(
      'INSERT INTO ratings (user_id, dish_id, order_id, rating, created_at) VALUES (?, ?, ?, ?, NOW()) ON DUPLICATE KEY UPDATE rating = VALUES(rating)',
      [userId, dishId, orderId, score]
    );

    // 更新菜品平均评分 + 评分计数（预计算，避免查询时实时 COUNT）
    await pool.execute(
      'UPDATE dishes SET avg_rating = (SELECT ROUND(AVG(rating), 1) FROM ratings WHERE dish_id = ?), rating_count = (SELECT COUNT(*) FROM ratings WHERE dish_id = ?) WHERE id = ?',
      [dishId, dishId, dishId]
    );

    res.json({ success: true, message: '评分成功' });
  } catch (err) {
    console.error('/api/dishes/:id/rate error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// ========== 客户端删除订单 ==========
app.delete('/api/orders/:id', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    const [result] = await pool.execute(
      `UPDATE orders SET deleted_at = NOW() WHERE id = ? AND ${scopeCol} = ? AND deleted_at IS NULL`,
      [req.params.id, scopeVal]
    );

    if (result.affectedRows === 0) {
      return res.status(404).json({ error: '订单不存在' });
    }

    res.json({ success: true });
  } catch (err) {
    console.error('DELETE /api/orders/:id error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// ========== 历史记录页 ==========
app.get('/history', (req, res) => {
  if (!req.session || !req.session.userId) return res.redirect('/login');
  res.render('history');
});

// ========== 客户端页面路由 ==========

app.get('/login', (req, res) => {
  if (req.session.userId) return res.redirect('/');
  res.render('login', { error: '' });
});

app.get('/register', (req, res) => {
  if (req.session.userId) return res.redirect('/');
  res.render('register', { error: '' });
});

app.post('/api/logout', (req, res) => {
  req.session.destroy(function(err) {
    if (err) return res.status(500).json({ error: '退出失败' });
    res.json({ success: true });
  });
});

// ========== 家庭绑定 ==========

app.get('/api/bind/status', requireAuth, async function(req, res) {
  try {
    var userId = req.session.userId;

    // 从 DB 查最新 family_group_id（不依赖 session，绑定后立即可见）
    var [[userRow]] = await pool.execute('SELECT family_group_id FROM users WHERE id = ?', [userId]);
    var familyGroupId = userRow.family_group_id;
    if (familyGroupId) {
      req.session.familyGroupId = familyGroupId;
    }

    if (familyGroupId) {
      var [[fg]] = await pool.execute('SELECT id, name, invite_code, owner_id FROM family_groups WHERE id = ?', [familyGroupId]);
      var [[owner]] = await pool.execute('SELECT phone FROM users WHERE id = ?', [fg ? fg.owner_id : 0]);
      return res.json({
        success: true, bound: true,
        familyGroup: fg ? { id: fg.id, name: fg.name, inviteCode: fg.invite_code, ownerPhone: owner ? owner.phone : '' } : null,
      });
    }

    var [[latest]] = await pool.execute(
      'SELECT id, target_phone, status, created_at FROM bind_requests WHERE applicant_id = ? ORDER BY created_at DESC LIMIT 1',
      [userId]
    );
    var isPending = latest && (latest.status === 'pending' || latest.status === 'generated');
    var isRejected = latest && latest.status === 'rejected';
    res.json({
      success: true, bound: false,
      pendingRequest: isPending ? { id: latest.id, targetPhone: latest.target_phone, createdAt: latest.created_at } : null,
      rejected: isRejected,
      rejectMessage: isRejected ? '申请已被拒绝' : '',
      lastTargetPhone: latest ? latest.target_phone : '',
    });
  } catch (err) {
    console.error('/api/bind/status error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.post('/api/bind/apply', requireAuth, bindApplyLimiter, async function(req, res) {
  try {
    var userId = req.session.userId;
    var phone = (req.body.phone || '').trim();

    console.log('[bind/apply] 绑定申请手机号:', phone ? '****' + phone.slice(-4) : '***');

    if (!phone || !/^1\d{10}$/.test(phone)) {
      return res.json({ success: false, message: '请输入正确的手机号' });
    }

    // 检查自己是否已绑定
    var [[self]] = await pool.execute('SELECT phone, family_group_id FROM users WHERE id = ?', [userId]);
    if (self.family_group_id) {
      return res.json({ success: false, message: '你已绑定家庭组，请先解绑' });
    }
    if (self.phone === phone) {
      return res.json({ success: false, message: '不能绑定自己的手机号' });
    }

    // 用手机号直接查 family_groups 的 invite_code
    var [fgRows] = await pool.execute(
      'SELECT id FROM family_groups WHERE invite_code = ?',
      [phone]
    );

    if (fgRows.length === 0) {
      return res.json({ success: false, message: '该用户不是家庭组主理人' });
    }

    // 检查是否有重复申请
    var [[dup]] = await pool.execute(
      'SELECT id FROM bind_requests WHERE applicant_id = ? AND status IN (?, ?)',
      [userId, 'pending', 'generated']
    );
    if (dup) {
      return res.json({ success: false, message: '已有待处理的绑定申请' });
    }

    await pool.execute('INSERT INTO bind_requests (applicant_id, target_phone) VALUES (?, ?)', [userId, phone]);
    console.log('[bind/apply] 绑定申请已写入，applicant:', userId, 'target_phone:', phone ? '****' + phone.slice(-4) : '***');
    res.json({ success: true, message: '申请已提交，等待大厨确认' });
  } catch (err) {
    console.error('/api/bind/apply error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.post('/api/bind/unbind', requireAuth, async function(req, res) {
  try {
    var userId = req.session.userId;

    var [[self]] = await pool.execute('SELECT family_group_id FROM users WHERE id = ?', [userId]);
    if (!self.family_group_id) {
      return res.json({ success: false, message: '你当前未绑定任何家庭组' });
    }

    var [[fg]] = await pool.execute('SELECT owner_id FROM family_groups WHERE id = ?', [self.family_group_id]);
    if (fg && fg.owner_id === userId) {
      return res.json({ success: false, message: '家庭组主理人不能解绑' });
    }

    await pool.execute('UPDATE users SET family_group_id = NULL WHERE id = ?', [userId]);
    req.session.familyGroupId = null;
    res.json({ success: true, message: '已解绑' });
  } catch (err) {
    console.error('/api/bind/unbind error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// ========== 管理端绑定管理 ==========

app.get('/api/family/bind/requests', requireAuth, async function(req, res) {
  try {
    var chefPhone = req.session.userPhone;

    var [rows] = await pool.execute(
      'SELECT br.id, u.phone AS applicant_phone, br.target_phone, br.status, br.created_at FROM bind_requests br JOIN users u ON br.applicant_id = u.id WHERE br.target_phone = ? AND br.status = ? ORDER BY br.created_at DESC',
      [chefPhone, 'pending']
    );

    res.json({ requests: rows.map(function(r) { return { id: r.id, applicantPhone: r.applicant_phone, targetPhone: r.target_phone, status: r.status, createdAt: r.created_at }; }) });
  } catch (err) { console.error('/api/family/bind/requests error:', err); res.status(500).json({ error: '服务器错误' }); }
});

app.put('/api/family/bind/approve', requireAuth, async function(req, res) {
  try {
    var chefId = req.session.userId;
    var chefPhone = req.session.userPhone;
    var requestId = req.body.requestId;

    console.log('[bind/approve] chefId:', chefId, 'requestId:', requestId);

    // 用 chefId 查家庭组
    var [fgRows] = await pool.execute('SELECT * FROM family_groups WHERE owner_id = ?', [chefId]);
    if (fgRows.length === 0) {
      console.log('[bind/approve] 未找到家庭组，owner_id:', chefId);
      return res.json({ success: false, message: '您不是家庭组主理人' });
    }
    var fg = fgRows[0];
    console.log('[bind/approve] 家庭组:', JSON.stringify({ id: fg.id, name: fg.name, owner_id: fg.owner_id }));

    // 查绑定申请
    var [reqRows] = await pool.execute('SELECT id, applicant_id, target_phone FROM bind_requests WHERE id = ? AND status = ?', [requestId, 'pending']);
    if (reqRows.length === 0) {
      console.log('[bind/approve] 申请不存在或已处理, requestId:', requestId);
      return res.json({ success: false, message: '申请不存在或已处理' });
    }
    var reqRow = reqRows[0];
    console.log('[bind/approve] 申请 target_phone:', reqRow.target_phone ? '****' + reqRow.target_phone.slice(-4) : '***');

    // 权限校验：target_phone 必须等于大厨手机号
    if (reqRow.target_phone !== chefPhone) {
      console.log('[bind/approve] 手机号不匹配，拒绝操作');
      return res.json({ success: false, message: '无权操作该申请' });
    }

    var conn = await pool.getConnection();
    await conn.beginTransaction();
    try {
      await conn.execute('UPDATE bind_requests SET status = ? WHERE id = ?', ['approved', requestId]);
      await conn.execute('UPDATE users SET family_group_id = ? WHERE id = ?', [fg.id, reqRow.applicant_id]);

      // 回填大厨已有的菜品和分类（补上可能缺失的 family_group_id）
      await conn.execute('UPDATE categories SET family_group_id = ? WHERE user_id = ? AND family_group_id IS NULL', [fg.id, chefId]);
      await conn.execute('UPDATE dishes SET family_group_id = ? WHERE user_id = ? AND family_group_id IS NULL', [fg.id, chefId]);
      await conn.execute('UPDATE inventory SET family_group_id = ? WHERE user_id = ? AND family_group_id IS NULL', [fg.id, chefId]);

      await conn.commit();
      conn.release();
      console.log('[bind/approve] 绑定成功, applicant:', reqRow.applicant_id, '→ family_group_id:', fg.id);
      res.json({ success: true, message: '绑定成功' });
    } catch (err) {
      await conn.rollback();
      conn.release();
      throw err;
    }
  } catch (err) { console.error('/api/family/bind/approve error:', err); res.status(500).json({ error: '服务器错误' }); }
});


app.put('/api/family/bind/reject', requireAuth, async function(req, res) {
  try {
    var userId = req.session.userId;
    var requestId = req.body.requestId;

    var [[fg]] = await pool.execute('SELECT id FROM family_groups WHERE owner_id = ?', [userId]);
    if (!fg) return res.json({ success: false, message: '你还没有创建家庭组' });

    var [[reqRow]] = await pool.execute('SELECT id FROM bind_requests WHERE id = ? AND status = ?', [requestId, 'pending']);
    if (!reqRow) return res.json({ success: false, message: '申请不存在或已处理' });

    await pool.execute('UPDATE bind_requests SET status = ? WHERE id = ?', ['rejected', requestId]);
    res.json({ success: true, message: '已拒绝' });
  } catch (err) { console.error('/api/family/bind/reject error:', err); res.status(500).json({ error: '服务器错误' }); }
});

app.delete('/api/family/bind/requests/:id', requireAuth, async function(req, res) {
  try {
    var chefPhone = req.session.userPhone;
    var requestId = req.params.id;

    // 查绑定申请
    var [reqRows] = await pool.execute('SELECT id, target_phone, status FROM bind_requests WHERE id = ?', [requestId]);
    if (reqRows.length === 0) {
      return res.json({ success: false, message: '记录不存在' });
    }
    var reqRow = reqRows[0];

    // 权限校验：target_phone 必须等于大厨手机号
    if (reqRow.target_phone !== chefPhone) {
      return res.json({ success: false, message: '无权操作该申请' });
    }

    // 只允许删除 approved 或 rejected 状态的申请
    if (reqRow.status !== 'approved' && reqRow.status !== 'rejected') {
      return res.json({ success: false, message: '只能删除已处理的申请' });
    }

    await pool.execute('DELETE FROM bind_requests WHERE id = ?', [requestId]);
    res.json({ success: true, message: '已删除' });
  } catch (err) { console.error('DELETE /api/family/bind/requests/:id error:', err); res.status(500).json({ error: '服务器错误' }); }
});

// ========== 解除家庭关联 ==========
app.post('/api/family/unbind', requireAuth, async function(req, res) {
  try {
    var userId = req.session.userId;

    var [[userRow]] = await pool.execute('SELECT family_group_id, role FROM users WHERE id = ?', [userId]);

    // 同时查用户是否是某个家庭组的 owner（大厨的 family_group_id 可能为 NULL）
    var [[fgByOwner]] = await pool.execute('SELECT id FROM family_groups WHERE owner_id = ?', [userId]);

    if (!userRow.family_group_id && !fgByOwner) {
      return res.json({ success: false, message: '你未绑定任何家庭组' });
    }

    if (fgByOwner) {
      // 当前用户是家庭组 owner
      var fgId = fgByOwner.id;
      var [members] = await pool.execute('SELECT id FROM users WHERE family_group_id = ? AND id != ? LIMIT 1', [fgId, userId]);
      if (members.length > 0) {
        // 转移所有权给第一个组内成员
        await pool.execute('UPDATE family_groups SET owner_id = ? WHERE id = ?', [members[0].id, fgId]);
        await pool.execute('UPDATE users SET family_group_id = NULL WHERE id = ?', [userId]);
      } else {
        // 没有其他成员，解散家庭组
        await pool.execute('UPDATE users SET family_group_id = NULL WHERE family_group_id = ?', [fgId]);
        await pool.execute('DELETE FROM family_groups WHERE id = ?', [fgId]);
      }
    } else {
      // 普通成员直接退出
      await pool.execute('UPDATE users SET family_group_id = NULL WHERE id = ?', [userId]);
    }

    req.session.familyGroupId = null;
    res.json({ success: true, message: '已解除家庭关联' });
  } catch (err) { console.error('/api/family/unbind error:', err); res.status(500).json({ error: '服务器错误' }); }
});

// ========== 忘记密码 ==========

app.get('/forgot-password', function(req, res) {
  if (req.session.userId) return res.redirect('/');
  res.render('forgot-password', { error: '' });
});

app.post('/api/forgot-password/send-code', forgotPasswordLimiter, async function(req, res) {
  try {
    var phone = (req.body.phone || '').trim();
    if (!phone || !/^1\d{10}$/.test(phone)) {
      return res.json({ success: false, message: '手机号格式错误' });
    }

    // 手机号必须已注册
    var [userRows] = await pool.execute('SELECT id FROM users WHERE phone = ?', [phone]);
    if (userRows.length === 0) {
      return res.json({ success: true, message: '验证码已发送' });
    }

    // 60秒冷却
    var [latestRows] = await pool.execute(
      'SELECT created_at FROM sms_codes WHERE phone = ? ORDER BY created_at DESC LIMIT 1',
      [phone]
    );
    if (latestRows.length > 0) {
      var elapsed = Date.now() - new Date(latestRows[0].created_at).getTime();
      if (elapsed < 60000) {
        var remain = Math.ceil((60000 - elapsed) / 1000);
        return res.json({ success: false, message: '请' + remain + '秒后再试' });
      }
    }

    // 日限5条
    var [cntRows] = await pool.execute(
      'SELECT COUNT(*) AS cnt FROM sms_codes WHERE phone = ? AND created_at >= CURDATE()',
      [phone]
    );
    if (cntRows[0].cnt >= 5) {
      return res.json({ success: false, message: '今日验证码发送已达上限（5条）' });
    }

    var code = String(Math.floor(Math.random() * 900000 + 100000));

    if (process.env.TEST_SKIP_SMS === 'true') {
      console.log('[测试] 忘记密码验证码长度:' + (code ? code.length : 0) + ' 手机尾号:' + (phone ? '****' + phone.slice(-4) : '***'));
    } else {
      var smsResult = await sendSms(phone, code);
      if (!smsResult.success) {
        return res.json({ success: false, message: smsResult.error || '短信发送失败' });
      }
    }

    await pool.execute(
      'INSERT INTO sms_codes (phone, code, expires_at, created_at) VALUES (?, ?, DATE_ADD(NOW(), INTERVAL 5 MINUTE), NOW())',
      [phone, code]
    );

    res.json({ success: true, message: '验证码已发送' });
  } catch (err) {
    console.error('/api/forgot-password/send-code error:', err);
    res.status(500).json({ success: false, message: '服务器错误' });
  }
});

app.post('/api/forgot-password/reset', forgotPasswordLimiter, async function(req, res) {
  try {
    var phone = (req.body.phone || '').trim();
    var code = (req.body.code || '').trim();
    var password = req.body.password || '';

    if (!phone || !/^1\d{10}$/.test(phone)) {
      return res.json({ success: false, message: '手机号格式错误' });
    }
    if (!code || code.length !== 6) {
      return res.json({ success: false, message: '请输入6位验证码' });
    }
    if (!password || password.length < 6) {
      return res.json({ success: false, message: '新密码至少6位' });
    }

    // 校验验证码（事务 + FOR UPDATE 防止并发 race）
    var smsConn = await pool.getConnection();
    try {
      await smsConn.beginTransaction();
      var [codeRows] = await smsConn.execute(
        'SELECT id, code FROM sms_codes WHERE phone = ? AND used = 0 AND expires_at > NOW() ORDER BY created_at DESC LIMIT 1 FOR UPDATE',
        [phone]
      );
      if (codeRows.length === 0) {
        await smsConn.rollback();
        smsConn.release();
        return res.json({ success: false, message: '验证码错误或已过期' });
      }
      if (codeRows[0].code !== code) {
        await smsConn.rollback();
        smsConn.release();
        return res.json({ success: false, message: '验证码错误或已过期' });
      }
      await smsConn.execute('UPDATE sms_codes SET used = 1 WHERE id = ?', [codeRows[0].id]);
      await smsConn.commit();
      smsConn.release();
    } catch (e) {
      await smsConn.rollback().catch(() => {});
      smsConn.release();
      throw e;
    }

    // 更新密码
    var password_hash = await bcrypt.hash(password, 10);
    await pool.execute('UPDATE users SET password_hash = ? WHERE phone = ?', [password_hash, phone]);

    res.json({ success: true, message: '密码重置成功，请登录' });
  } catch (err) {
    console.error('/api/forgot-password/reset error:', err);
    res.status(500).json({ success: false, message: '服务器错误' });
  }
});

// ========== 旧管理端兼容（重定向到首页） ==========
app.get('/admin', (req, res) => {
  res.redirect('/');
});

// ========== 家庭组数据 API ==========

app.get('/api/family/orders', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;

    var page = parseInt(req.query.page, 10) || 1;
    var limit = Math.min(parseInt(req.query.limit, 10) || 10, 100);
    var offset = (page - 1) * limit;
    var days = parseInt(req.query.days, 10) || 0;
    var search = sanitizeSearch(req.query.search, 100);

    var where = 'WHERE o.' + scopeCol + ' = ? AND o.deleted_at IS NULL';
    var params = [scopeVal];

    if (days > 0) {
      where += ' AND o.created_at >= DATE_SUB(NOW(), INTERVAL ' + days + ' DAY)';
    }
    if (search) {
      where += ' AND o.id IN (SELECT DISTINCT oi.order_id FROM order_items oi WHERE oi.dish_name LIKE ?)';
      params.push('%' + search + '%');
    }

    var [[{ cnt: total }]] = await pool.execute(
      'SELECT COUNT(*) AS cnt FROM orders o ' + where, params
    );

    var [orderRows] = await pool.execute(
      'SELECT o.id, o.status, o.reject_reason, o.accepted_at, o.completed_at, o.created_at FROM orders o '
      + where + ' ORDER BY o.created_at DESC LIMIT ' + limit + ' OFFSET ' + offset,
      params
    );

    if (orderRows.length === 0) {
      return res.json({ orders: [], total: total, page: page, limit: limit, totalPages: Math.ceil(total / limit) });
    }

    var orderIds = orderRows.map(function(o) { return o.id; });
    var [itemRows] = await pool.execute(
      'SELECT order_id, dish_id, dish_name, remark FROM order_items WHERE order_id IN (' +
        orderIds.map(function() { return '?'; }).join(',') + ')',
      orderIds
    );

    var itemsMap = {};
    itemRows.forEach(function(item) {
      if (!itemsMap[item.order_id]) itemsMap[item.order_id] = [];
      itemsMap[item.order_id].push(item);
    });

    var orders = orderRows.map(function(o) {
      var items = itemsMap[o.id] || [];
      return {
        id: o.id.toString(),
        dishIds: items.map(function(i) { return i.dish_id !== null ? i.dish_id.toString() : ''; }),
        dishNames: items.map(function(i) { return i.dish_name; }),
        remarks: items.map(function(i) { return i.remark || ''; }),
        createdAt: new Date(o.created_at).toISOString(),
        status: o.status,
        rejectReason: o.reject_reason || '',
        acceptedAt: o.accepted_at ? new Date(o.accepted_at).toISOString() : '',
        completedAt: o.completed_at ? new Date(o.completed_at).toISOString() : '',
      };
    });

    res.json({ orders: orders, total: total, page: page, limit: limit, totalPages: Math.ceil(total / limit) });
  } catch (err) {
    console.error('GET /api/family/orders error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.put('/api/family/orders/:id/accept', requireAuth, requireChef, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;
    const [result] = await pool.execute(
      `UPDATE orders SET status = ?, accepted_at = NOW() WHERE id = ? AND ${scopeCol} = ? AND deleted_at IS NULL`,
      ['accepted', req.params.id, scopeVal]
    );
    if (result.affectedRows === 0) return res.status(404).json({ error: '订单不存在' });
    invalidateCache(scopeVal);
    // Socket.IO: 通知家庭组成员订单状态变化
    var fgId = req.session.familyGroupId;
    if (fgId) io.to('fg_' + fgId).emit('order_update', { orderId: String(req.params.id) });
    res.json({ success: true });
  } catch (err) {
    console.error('PUT /api/family/orders/:id/accept error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.put('/api/family/orders/:id/reject', requireAuth, requireChef, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;
    const [result] = await pool.execute(
      `UPDATE orders SET status = ?, reject_reason = ? WHERE id = ? AND ${scopeCol} = ? AND deleted_at IS NULL`,
      ['rejected', req.body.reason || '', req.params.id, scopeVal]
    );
    if (result.affectedRows === 0) return res.status(404).json({ error: '订单不存在' });
    invalidateCache(scopeVal);
    // Socket.IO: 通知家庭组成员订单状态变化
    var fgId = req.session.familyGroupId;
    if (fgId) io.to('fg_' + fgId).emit('order_update', { orderId: String(req.params.id) });
    res.json({ success: true });
  } catch (err) {
    console.error('PUT /api/family/orders/:id/reject error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.put('/api/family/orders/:id/cook', requireAuth, requireChef, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;
    const [result] = await pool.execute(
      `UPDATE orders SET status = ? WHERE id = ? AND ${scopeCol} = ? AND deleted_at IS NULL`,
      ['cooking', req.params.id, scopeVal]
    );
    if (result.affectedRows === 0) return res.status(404).json({ error: '订单不存在' });
    invalidateCache(scopeVal);
    // Socket.IO: 通知家庭组成员订单状态变化
    var fgId = req.session.familyGroupId;
    if (fgId) io.to('fg_' + fgId).emit('order_update', { orderId: String(req.params.id) });
    res.json({ success: true });
  } catch (err) {
    console.error('PUT /api/family/orders/:id/cook error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.put('/api/family/orders/:id/complete', requireAuth, requireChef, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;
    const [result] = await pool.execute(
      `UPDATE orders SET status = ?, completed_at = NOW() WHERE id = ? AND ${scopeCol} = ? AND deleted_at IS NULL`,
      ['completed', req.params.id, scopeVal]
    );
    if (result.affectedRows === 0) return res.status(404).json({ error: '订单不存在' });
    invalidateCache(scopeVal);
    // Socket.IO: 通知家庭组成员订单状态变化
    var fgId = req.session.familyGroupId;
    if (fgId) io.to('fg_' + fgId).emit('order_update', { orderId: String(req.params.id) });
    res.json({ success: true });
  } catch (err) {
    console.error('PUT /api/family/orders/:id/complete error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.delete('/api/family/orders/:id', requireAuth, requireChef, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;
    const [result] = await pool.execute(
      `UPDATE orders SET deleted_at = NOW() WHERE id = ? AND ${scopeCol} = ? AND deleted_at IS NULL`,
      [req.params.id, scopeVal]
    );
    if (result.affectedRows === 0) return res.status(404).json({ error: '订单不存在' });
    res.json({ success: true });
  } catch (err) {
    console.error('DELETE /api/family/orders/:id error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});


// ========== 家庭组数据统计 ==========
app.get('/api/family/stats', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;

    var cacheKey = 'family_stats_' + scopeVal;
    var cached = cacheService.get(cacheKey);
    if (cached) return res.json(cached);

    // 并行：4 个独立查询同时执行
    var parallelResults = await Promise.all([
      pool.execute('SELECT COUNT(*) AS cnt FROM orders WHERE ' + scopeCol + ' = ? AND deleted_at IS NULL AND YEAR(created_at) = YEAR(NOW()) AND MONTH(created_at) = MONTH(NOW())', [scopeVal]),
      pool.execute('SELECT COUNT(*) AS cnt FROM dishes WHERE ' + scopeCol + ' = ?', [scopeVal]),
      pool.execute('SELECT name, avg_rating FROM dishes WHERE ' + scopeCol + ' = ? AND avg_rating > 0 ORDER BY avg_rating DESC LIMIT 1', [scopeVal]),
      pool.execute('SELECT d.name, COUNT(oi.id) AS cnt FROM order_items oi JOIN orders o ON oi.order_id = o.id JOIN dishes d ON oi.dish_id = d.id WHERE o.' + scopeCol + ' = ? AND o.deleted_at IS NULL AND YEAR(o.created_at) = YEAR(NOW()) AND MONTH(o.created_at) = MONTH(NOW()) GROUP BY d.id, d.name ORDER BY cnt DESC LIMIT 5', [scopeVal])
    ]);

    var [[{ cnt: orderCount }]] = parallelResults[0];
    var [[{ cnt: dishCount }]] = parallelResults[1];
    var [topRatedRows] = parallelResults[2];
    var [topDishes] = parallelResults[3];

    var topRated = '暂无';
    if (topRatedRows.length > 0) {
      topRated = topRatedRows[0].name + ' ⭐' + Number(topRatedRows[0].avg_rating).toFixed(1);
    }

    var data = {
      orderCount: orderCount,
      dishCount: dishCount,
      topRated: topRated,
      topDishes: topDishes.map(function(r) { return { name: r.name, count: r.cnt }; }),
    };
    cacheService.set(cacheKey, data, 15);
    res.json(data);
  } catch (err) {
    console.error('GET /api/family/stats error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// ========== 家庭组菜品 API ==========

app.get('/api/family/dishes', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    const [rows] = await pool.execute(
      `SELECT d.id, d.name, d.image, d.ingredients, d.steps, d.is_available, d.avg_rating,
              COALESCE(c.name, '') AS category
       FROM dishes d
       LEFT JOIN categories c ON d.category_id = c.id
       WHERE d.${scopeCol} = ?
       ORDER BY d.avg_rating DESC, d.id`,
      [scopeVal]
    );

    const dishes = rows.map(function(r) {
      var ingredients = [];
      var steps = [];
      try { ingredients = JSON.parse(r.ingredients || '[]'); } catch (_) {}
      try { steps = JSON.parse(r.steps || '[]'); } catch (_) {}

      return {
        id: r.id,
        name: r.name,
        category: r.category,
        image: r.image || '',
        ingredients: ingredients,
        steps: steps,
        isAvailable: !!r.is_available,
        avgRating: Number(r.avg_rating) || 0,
      };
    });

    res.json({ dishes: dishes });
  } catch (err) {
    console.error('GET /api/family/dishes error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.post('/api/family/dishes', requireAuth, upload.single('image'), async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;
    const categoryName = (req.body.category || '').trim();

    var categoryId = null;
    if (categoryName) {
      const [catRows] = await pool.execute(
        `SELECT id FROM categories WHERE name = ? AND ${scopeCol} = ?`,
        [categoryName, userId]
      );
      if (catRows.length > 0) categoryId = catRows[0].id;
    }

    var imageVal = req.body.imageUrl || '';
    if (req.file) imageVal = req.file.filename;

    var ingredients = req.body.ingredients;
    if (typeof ingredients === 'string') {
      try { ingredients = JSON.parse(ingredients); } catch (_) {
        ingredients = ingredients.split(/[、,，]/).map(function(s) { return s.trim(); }).filter(Boolean);
      }
    }
    var steps = req.body.steps;
    if (typeof steps === 'string') {
      try { steps = JSON.parse(steps); } catch (_) {
        steps = steps.split('\n').map(function(s) { return s.trim(); }).filter(Boolean);
      }
    }

    const isAvailable = req.body.isAvailable === true || req.body.isAvailable === 'true';
    var fgId = (scopeCol === 'family_group_id') ? scopeVal : null;
    const [result] = await pool.execute(
      'INSERT INTO dishes (name, category_id, image, ingredients, steps, is_available, user_id, family_group_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
      [(req.body.name || '').trim().slice(0, 100), categoryId, imageVal, JSON.stringify(ingredients || []), JSON.stringify(steps || []), isAvailable ? 1 : 0, userId, fgId]
    );

    invalidateCache(scopeVal);
    res.json({
      success: true,
      dish: {
        id: result.insertId,
        name: (req.body.name || '').trim().slice(0, 100),
        category: categoryName,
        image: imageVal,
        ingredients: ingredients || [],
        steps: steps || [],
        isAvailable: isAvailable,
      },
    });
  } catch (err) {
    console.error('POST /api/family/dishes error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.put('/api/family/dishes/:id', requireAuth, upload.single('image'), async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;
    const categoryName = (req.body.category || '').trim();

    var categoryId = null;
    if (categoryName) {
      const [catRows] = await pool.execute(
        `SELECT id FROM categories WHERE name = ? AND ${scopeCol} = ?`,
        [categoryName, userId]
      );
      if (catRows.length > 0) categoryId = catRows[0].id;
    }

    const fields = [];
    const values = [];

    if (req.body.name !== undefined) { fields.push('name = ?'); values.push(req.body.name); }
    if (categoryId !== null || req.body.category !== undefined) {
      fields.push('category_id = ?');
      values.push(categoryId);
    }
    if (req.file) { fields.push('image = ?'); values.push(req.file.filename); }
    else if (req.body.imageUrl !== undefined) { fields.push('image = ?'); values.push(req.body.imageUrl || ''); }
    if (req.body.ingredients !== undefined) {
      var ings = req.body.ingredients;
      if (typeof ings === 'string') {
        try { ings = JSON.parse(ings); } catch (_) { ings = ings.split(/[、,，]/).map(function(s) { return s.trim(); }).filter(Boolean); }
      }
      fields.push('ingredients = ?'); values.push(JSON.stringify(ings));
    }
    if (req.body.steps !== undefined) {
      var stps = req.body.steps;
      if (typeof stps === 'string') {
        try { stps = JSON.parse(stps); } catch (_) { stps = stps.split('\n').map(function(s) { return s.trim(); }).filter(Boolean); }
      }
      fields.push('steps = ?'); values.push(JSON.stringify(stps));
    }
    if (req.body.isAvailable !== undefined) {
      fields.push('is_available = ?');
      values.push(req.body.isAvailable === true || req.body.isAvailable === 'true' ? 1 : 0);
    }

    if (fields.length === 0 && !req.file) {
      return res.status(400).json({ error: '没有要更新的字段' });
    }

    values.push(req.params.id, scopeVal);
    const [result] = await pool.execute(
      `UPDATE dishes SET ${fields.join(', ')} WHERE id = ? AND ${scopeCol} = ?`,
      values
    );

    if (result.affectedRows === 0) return res.status(404).json({ error: '菜品不存在' });
    invalidateCache(scopeVal);
    res.json({ success: true });
  } catch (err) {
    console.error('PUT /api/family/dishes/:id error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.delete('/api/family/dishes/:id', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    const [result] = await pool.execute(
      `DELETE FROM dishes WHERE id = ? AND ${scopeCol} = ?`,
      [req.params.id, scopeVal]
    );
    if (result.affectedRows === 0) return res.status(404).json({ error: '菜品不存在' });
    invalidateCache(scopeVal);
    res.json({ success: true });
  } catch (err) {
    console.error('DELETE /api/family/dishes/:id error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// ========== 家庭组分类 API ==========

app.get('/api/family/categories', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    const [rows] = await pool.execute(
      `SELECT name FROM categories WHERE ${scopeCol} = ? ORDER BY sort_order, id`,
      [scopeVal]
    );
    res.json({ categories: rows.map(function(r) { return r.name; }) });
  } catch (err) {
    console.error('GET /api/family/categories error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.post('/api/family/categories', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;
    var name = (req.body.name || '').trim();
    if (!name) return res.status(400).json({ error: '分类名不能为空' });

    // 检查重复
    const [existing] = await pool.execute(
      `SELECT id FROM categories WHERE name = ? AND ${scopeCol} = ?`,
      [name, userId]
    );
    if (existing.length > 0) return res.status(400).json({ error: '分类已存在' });

    var fgId = (scopeCol === 'family_group_id') ? scopeVal : null;
    await pool.execute(
      'INSERT INTO categories (name, user_id, family_group_id) VALUES (?, ?, ?)',
      [name, userId, fgId]
    );
    res.json({ success: true });
  } catch (err) {
    console.error('POST /api/family/categories error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.put('/api/family/categories/:name', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var newName = (req.body.newName || '').trim();
    if (!newName) return res.status(400).json({ error: '新名称不能为空' });

    // 检查旧分类是否存在
    const [oldRows] = await pool.execute(
      `SELECT id FROM categories WHERE name = ? AND ${scopeCol} = ?`,
      [req.params.name, scopeVal]
    );
    if (oldRows.length === 0) return res.status(404).json({ error: '分类不存在' });

    // 检查新名称是否重复
    const [dupRows] = await pool.execute(
      `SELECT id FROM categories WHERE name = ? AND ${scopeCol} = ? AND id != ?`,
      [newName, scopeVal, oldRows[0].id]
    );
    if (dupRows.length > 0) return res.status(400).json({ error: '分类名已存在' });

    await pool.execute(
      `UPDATE categories SET name = ? WHERE id = ? AND ${scopeCol} = ?`,
      [newName, oldRows[0].id, scopeVal]
    );

    res.json({ success: true });
  } catch (err) {
    console.error('PUT /api/family/categories/:name error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.delete('/api/family/categories/:name', requireAuth, async (req, res) => {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;

    // 查找分类
    const [catRows] = await pool.execute(
      `SELECT id FROM categories WHERE name = ? AND ${scopeCol} = ?`,
      [req.params.name, scopeVal]
    );
    if (catRows.length === 0) return res.status(404).json({ error: '分类不存在' });

    // 检查该分类下是否有菜品
    const [dishRows] = await pool.execute(
      `SELECT COUNT(*) AS cnt FROM dishes WHERE category_id = ? AND ${scopeCol} = ?`,
      [catRows[0].id, scopeVal]
    );
    if (dishRows[0].cnt > 0) return res.status(400).json({ error: '该分类下有菜品，无法删除' });

    await pool.execute(
      `DELETE FROM categories WHERE id = ? AND ${scopeCol} = ?`,
      [catRows[0].id, scopeVal]
    );

    res.json({ success: true });
  } catch (err) {
    console.error('DELETE /api/family/categories/:name error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

const PORT = process.env.PORT || 3000;
var server = http.createServer(app);
var io = new Server(server);

io.use(function(socket, next) {
  sessionMiddleware(socket.request, {}, next);
});
io.use(function(socket, next) {
  var req = socket.request;
  if (req.session && req.session.userId) {
    socket.userId = req.session.userId;
    socket.userRole = req.session.userRole;
    socket.familyGroupId = req.session.familyGroupId;
    return next();
  }
  next(new Error('未登录'));
});
io.on('connection', function(socket) {
  socket.on('join', async function(familyGroupId) {
    if (!familyGroupId || !socket.userId) return;
    try {
      var [[row]] = await pool.execute('SELECT family_group_id FROM users WHERE id = ?', [socket.userId]);
      if (row && String(row.family_group_id) === String(familyGroupId)) {
        socket.join('fg_' + familyGroupId);
      }
    } catch (_) {}
  });
});

// 缓存失效辅助：写入菜品/分类/订单时清除相关缓存
function invalidateCache(scopeVal) {
  cacheService.del('dash_stats');
  if (scopeVal) {
    cacheService.del('menu_' + scopeVal);
    cacheService.del('family_stats_' + scopeVal);
  } else {
    cacheService.flush();
  }
}

loadAdminPassword().then(function() {
  server.listen(PORT, () => {
    console.log('http://localhost:' + PORT);
  });
});

// 导出供路由使用
app.set('io', io);

// ========== 超级管理员后台 ==========

// 管理员密码 hash（内存缓存 + DB 持久化）
var adminPassHash = null;

async function loadAdminPassword() {
  try {
    var [[row]] = await pool.execute("SELECT setting_value FROM admin_settings WHERE setting_key = 'admin_pass_hash'");
    if (row) { adminPassHash = row.setting_value; return; }
  } catch (_) {}
  // 首次启动：从环境变量读取明文密码，bcrypt 后存入 DB
  var envPass = process.env.DASHBOARD_PASS;
  if (envPass) {
    adminPassHash = await bcrypt.hash(envPass, 12);
    await pool.execute("INSERT INTO admin_settings (setting_key, setting_value) VALUES ('admin_pass_hash', ?) ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value)", [adminPassHash]);
    console.log(ts() + ' [ADMIN] password hash stored to DB');
  }
}

async function verifyAdminPassword(inputPassword) {
  if (!adminPassHash) return false;
  return bcrypt.compare(inputPassword, adminPassHash);
}

function requireDashboard(req, res, next) {
  if (req.session && req.session.isDashboardAdmin) return next();
  if (req.path === '/dashboard/login') return next();
  res.redirect('/dashboard/login');
}

app.get('/dashboard/login', function(req, res) {
  if (req.session && req.session.isDashboardAdmin) return res.redirect('/dashboard');
  res.render('dashboard', { loggedIn: false, error: '' });
});

app.post('/dashboard/login', dashboardLimiter, csrfCheck, function(req, res) {
  var username = req.body.username;
  var password = req.body.password;
  if (!username || !password) {
    return res.render('dashboard', { loggedIn: false, error: '账号或密码错误' });
  }
  var envUser = process.env.DASHBOARD_USER || 'admin';
  if (username !== envUser) {
    return res.render('dashboard', { loggedIn: false, error: '账号或密码错误' });
  }
  if (!adminPassHash) {
    return res.render('dashboard', { loggedIn: false, error: '管理后台未配置凭据，请设置 DASHBOARD_PASS 环境变量后重启服务' });
  }
  verifyAdminPassword(password).then(function(ok) {
    if (!ok) {
      return res.render('dashboard', { loggedIn: false, error: '账号或密码错误' });
    }
    req.session.regenerate(function(err) {
      if (err) {
        console.error('Dashboard session regenerate error:', err);
        return res.render('dashboard', { loggedIn: false, error: '服务器错误' });
      }
      req.session.isDashboardAdmin = true;
      return res.redirect('/dashboard');
    });
  }).catch(function(err) {
    console.error('Dashboard bcrypt error:', err);
    res.render('dashboard', { loggedIn: false, error: '服务器错误' });
  });
});

app.get('/dashboard/logout', function(req, res) {
  req.session.destroy(function(err) {
    if (err) console.error('Dashboard session destroy error:', err);
    res.clearCookie('connect.sid');
    res.redirect('/dashboard/login');
  });
});

app.get('/dashboard', requireDashboard, function(req, res) {
  res.render('dashboard', { loggedIn: true, error: '' });
});

// ===== 大厨快速归档 =====
app.put('/api/orders/archive/:id', requireAuth, requireChef, async function(req, res) {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var [r] = await pool.execute('UPDATE orders SET archived_at = NOW() WHERE id = ? AND ' + scopeCol + ' = ? AND deleted_at IS NULL', [req.params.id, scopeVal]);
    if (r.affectedRows === 0) return res.json({ success: false, message: '订单不存在或已被删除' });
    invalidateCache(scopeVal);
    res.json({ success: true, message: '已移出看板' });
  } catch (e) { console.error(ts() + ' [API] 服务器错误:', e.message); res.status(500).json({ error: '服务器错误' }); }
});

app.put('/api/orders/archive-batch', requireAuth, requireChef, async function(req, res) {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var status = (req.body.status || '').trim();
    if (!status) return res.json({ success: false, message: '请指定要清理的状态' });
    var [r] = await pool.execute('UPDATE orders SET archived_at = NOW() WHERE ' + scopeCol + ' = ? AND status = ? AND deleted_at IS NULL', [scopeVal, status]);
    invalidateCache(scopeVal);
    res.json({ success: true, message: '已清理 ' + r.affectedRows + ' 条订单' });
  } catch (e) { console.error(ts() + ' [API] 服务器错误:', e.message); res.status(500).json({ error: '服务器错误' }); }
});

// ===== 数据管理中心 =====
app.get('/api/dashboard/data/archived', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var page = parseInt(req.query.page, 10) || 1;
    var limit = parseInt(req.query.limit, 10) || 20;
    var offset = (page - 1) * limit;
    var [[{ cnt: total }]] = await pool.execute('SELECT COUNT(*) AS cnt FROM orders WHERE deleted_at IS NULL AND archived_at IS NOT NULL');
    var [rows] = await pool.execute('SELECT o.id, o.status, o.archived_at, o.created_at, u.phone FROM orders o LEFT JOIN users u ON o.user_id = u.id WHERE o.deleted_at IS NULL AND o.archived_at IS NOT NULL ORDER BY o.archived_at DESC LIMIT ' + limit + ' OFFSET ' + offset);
    var ids = rows.map(function(r) { return r.id; });
    var dishMap = {};
    if (ids.length > 0) {
      var [items] = await pool.execute('SELECT order_id, dish_name FROM order_items WHERE order_id IN (' + ids.map(function() { return '?'; }).join(',') + ')', ids);
      items.forEach(function(it) { if (!dishMap[it.order_id]) dishMap[it.order_id] = []; dishMap[it.order_id].push(it.dish_name); });
    }
    res.json({ orders: rows.map(function(r) { return { id: r.id, status: r.status, phone: (r.phone||'').substring(0,3)+'****'+(r.phone||'').substring(7), dishNames: dishMap[r.id] || [], archivedAt: r.archived_at, createdAt: r.created_at }; }), total: total, page: page, totalPages: Math.ceil(total / limit) });
  } catch (e) { console.error(ts() + ' [API] 服务器错误:', e.message); res.status(500).json({ error: '服务器错误' }); }
});

app.get('/api/dashboard/data/deleted', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var page = parseInt(req.query.page, 10) || 1;
    var limit = parseInt(req.query.limit, 10) || 20;
    var offset = (page - 1) * limit;
    var [[{ cnt: total }]] = await pool.execute('SELECT COUNT(*) AS cnt FROM orders WHERE deleted_at IS NOT NULL');
    var [rows] = await pool.execute('SELECT o.id, o.status, o.deleted_at, o.created_at, u.phone FROM orders o LEFT JOIN users u ON o.user_id = u.id WHERE o.deleted_at IS NOT NULL ORDER BY o.deleted_at DESC LIMIT ' + limit + ' OFFSET ' + offset);
    var ids = rows.map(function(r) { return r.id; });
    var dishMap = {};
    if (ids.length > 0) {
      var [items] = await pool.execute('SELECT order_id, dish_name FROM order_items WHERE order_id IN (' + ids.map(function() { return '?'; }).join(',') + ')', ids);
      items.forEach(function(it) { if (!dishMap[it.order_id]) dishMap[it.order_id] = []; dishMap[it.order_id].push(it.dish_name); });
    }
    res.json({ orders: rows.map(function(r) { return { id: r.id, status: r.status, phone: (r.phone||'').substring(0,3)+'****'+(r.phone||'').substring(7), dishNames: dishMap[r.id] || [], deletedAt: r.deleted_at, createdAt: r.created_at }; }), total: total, page: page, totalPages: Math.ceil(total / limit) });
  } catch (e) { console.error(ts() + ' [API] 服务器错误:', e.message); res.status(500).json({ error: '服务器错误' }); }
});

app.put('/api/dashboard/data/restore/:id', requireDashboard, dashboardApiLimiter, csrfCheck, async function(req, res) {
  try {
    var [r] = await pool.execute('UPDATE orders SET archived_at = NULL, deleted_at = NULL WHERE id = ?', [req.params.id]);
    if (r.affectedRows === 0) return res.json({ success: false, message: '订单不存在' });
    cacheService.del('dash_stats');
    res.json({ success: true, message: '已恢复为活跃状态' });
  } catch (e) { console.error(ts() + ' [API] 服务器错误:', e.message); res.status(500).json({ error: '服务器错误' }); }
});

// 批量操作
app.put('/api/dashboard/data/batch-archive', requireDashboard, dashboardApiLimiter, csrfCheck, async function(req, res) {
  try {
    var status = (req.body.status || '').trim();
    if (!status) return res.json({ success: false, message: '请指定状态' });
    var [r] = await pool.execute('UPDATE orders SET archived_at = NOW() WHERE status = ? AND deleted_at IS NULL', [status]);
    cacheService.del('dash_stats');
    res.json({ success: true, message: '已归档 ' + r.affectedRows + ' 条' });
  } catch (e) { console.error(ts() + ' [API] 服务器错误:', e.message); res.status(500).json({ error: '服务器错误' }); }
});

app.put('/api/dashboard/data/batch-softdelete', requireDashboard, dashboardApiLimiter, csrfCheck, async function(req, res) {
  try {
    var [r] = await pool.execute('UPDATE orders SET deleted_at = NOW() WHERE archived_at IS NOT NULL AND deleted_at IS NULL');
    cacheService.del('dash_stats');
    res.json({ success: true, message: '已软删除 ' + r.affectedRows + ' 条' });
  } catch (e) { console.error(ts() + ' [API] 服务器错误:', e.message); res.status(500).json({ error: '服务器错误' }); }
});

app.put('/api/dashboard/data/batch-restore', requireDashboard, dashboardApiLimiter, csrfCheck, async function(req, res) {
  try {
    var scope = (req.body.scope || '').trim();
    var where = 'WHERE deleted_at IS NULL';
    if (scope === 'archived') where = 'WHERE archived_at IS NOT NULL AND deleted_at IS NULL';
    else if (scope === 'deleted') where = 'WHERE deleted_at IS NOT NULL';
    else if (scope === 'all') where = '';
    else return res.json({ success: false, message: '请指定范围: archived/deleted/all' });
    var [r] = await pool.execute('UPDATE orders SET archived_at = NULL, deleted_at = NULL ' + where);
    cacheService.del('dash_stats');
    res.json({ success: true, message: '已恢复 ' + r.affectedRows + ' 条' });
  } catch (e) { console.error(ts() + ' [API] 服务器错误:', e.message); res.status(500).json({ error: '服务器错误' }); }
});

app.delete('/api/dashboard/data/permanent/:id', requireDashboard, dashboardApiLimiter, csrfCheck, async function(req, res) {
  try {
    var [[row]] = await pool.execute('SELECT id FROM orders WHERE id = ? AND deleted_at IS NOT NULL', [req.params.id]);
    if (!row) return res.json({ success: false, message: '只能永久删除已软删除的订单' });
    var conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      await conn.execute('DELETE FROM order_items WHERE order_id = ?', [req.params.id]);
      await conn.execute('DELETE FROM orders WHERE id = ?', [req.params.id]);
      await conn.commit();
      cacheService.del('dash_stats');
      res.json({ success: true, message: '已永久删除' });
    } catch (e2) {
      await conn.rollback();
      throw e2;
    } finally {
      conn.release();
    }
  } catch (e) { console.error(ts() + ' [API] 服务器错误:', e.message); res.status(500).json({ error: '服务器错误' }); }
});

app.get('/api/dashboard/stats', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var cached = cacheService.get('dash_stats');
    if (cached) return res.json(cached);

    // 优先读聚合表 O(1)，回退到实时查询
    var [[pre]] = await pool.execute('SELECT total_users, total_families, today_orders, today_completed, active_families FROM dashboard_stats WHERE id=1');
    if (pre && pre.total_users !== null && pre.total_users > 0) {
      // 补充聚合表缺的字段：newUsersToday, cookingOrders, archivedCount, deletedCount
      try {
        var extra = await Promise.all([
          pool.execute('SELECT COUNT(*) AS cnt FROM users WHERE DATE(created_at)=CURDATE()'),
          pool.execute('SELECT COUNT(*) AS cnt FROM orders WHERE deleted_at IS NULL AND status=\"cooking\"'),
          pool.execute('SELECT COUNT(*) AS cnt FROM orders WHERE deleted_at IS NULL').catch(() => [[{cnt:0}]]),
          pool.execute('SELECT COUNT(*) AS cnt FROM orders').catch(() => [[{cnt:0}]]),
        ]);
        var [[{ cnt: newUsersToday }]] = extra[0];
        var [[{ cnt: cookingOrders }]] = extra[1];
        var [[{ cnt: activeOrders }]] = extra[2];
      } catch (_) { var newUsersToday = 0, cookingOrders = 0, activeOrders = 0; }
      var d = { userCount: pre.total_users, familyCount: pre.total_families, todayOrders: pre.today_orders, activeFamilyGroups: pre.active_families, newUsersToday: newUsersToday || 0, cookingOrders: cookingOrders || 0, completedOrdersToday: pre.today_completed || 0, completionRate: pre.today_orders > 0 ? Math.round((pre.today_completed || 0) / pre.today_orders * 1000) / 10 : 0, archivedCount: 0, deletedCount: 0 };
      cacheService.set('dash_stats', d, 15);
      return res.json(d);
    }

    // 回退：7 个独立 COUNT 查询
    var results = await Promise.all([
      pool.execute('SELECT COUNT(*) AS cnt FROM users'),
      pool.execute('SELECT COUNT(*) AS cnt FROM family_groups'),
      pool.execute('SELECT COUNT(*) AS cnt FROM orders WHERE deleted_at IS NULL AND DATE(created_at) = CURDATE()'),
      pool.execute('SELECT COUNT(DISTINCT family_group_id) AS cnt FROM orders WHERE deleted_at IS NULL AND family_group_id IS NOT NULL AND created_at >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)'),
      pool.execute('SELECT COUNT(*) AS cnt FROM users WHERE DATE(created_at) = CURDATE()'),
      pool.execute('SELECT COUNT(*) AS cnt FROM orders WHERE deleted_at IS NULL AND status = ?', ['cooking']),
      pool.execute('SELECT COUNT(*) AS cnt FROM orders WHERE deleted_at IS NULL AND status = ? AND DATE(created_at) = CURDATE()', ['completed']),
      pool.execute('SELECT COUNT(*) AS cnt FROM orders WHERE archived_at IS NOT NULL AND deleted_at IS NULL'),
      pool.execute('SELECT COUNT(*) AS cnt FROM orders WHERE deleted_at IS NOT NULL'),
    ]);
    var [[{ cnt: userCount }]] = results[0];
    var [[{ cnt: familyCount }]] = results[1];
    var [[{ cnt: todayOrders }]] = results[2];
    var [[{ cnt: activeFamilyGroups }]] = results[3];
    var [[{ cnt: newUsersToday }]] = results[4];
    var [[{ cnt: cookingOrders }]] = results[5];
    var [[{ cnt: completedOrdersToday }]] = results[6];
    var [[{ cnt: archivedCount }]] = results[7];
    var [[{ cnt: deletedCount }]] = results[8];
    var completionRate = todayOrders > 0 ? Math.round(completedOrdersToday / todayOrders * 1000) / 10 : 0;
    var data = { userCount, familyCount, todayOrders, activeFamilyGroups, newUsersToday, cookingOrders, completedOrdersToday, completionRate, archivedCount: archivedCount || 0, deletedCount: deletedCount || 0 };
    cacheService.set('dash_stats', data, 15);
    res.json(data);
  } catch (err) {
    res.status(500).json({ error: '服务器错误' });
  }
});

// 近7天订单趋势
app.get('/api/dashboard/trend', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var [rows] = await pool.execute(
      'SELECT DATE_FORMAT(created_at, "%Y-%m-%d") AS date, COUNT(*) AS cnt FROM orders WHERE deleted_at IS NULL AND created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) GROUP BY DATE_FORMAT(created_at, "%Y-%m-%d") ORDER BY date'
    );
    var dateMap = {};
    rows.forEach(function(r) { dateMap[r.date] = r.cnt; });
    var trend = [];
    for (var i = 6; i >= 0; i--) {
      var d = new Date();
      d.setDate(d.getDate() - i);
      var ds = d.getFullYear() + '-' + String(d.getMonth() + 1).padStart(2, '0') + '-' + String(d.getDate()).padStart(2, '0');
      trend.push({ date: ds, count: dateMap[ds] || 0 });
    }
    res.json({ trend: trend });
  } catch (err) {
    res.status(500).json({ error: '服务器错误' });
  }
});

app.get('/api/dashboard/users', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var page = parseInt(req.query.page, 10) || 1;
    var limit = parseInt(req.query.limit, 10) || 20;
    var offset = (page - 1) * limit;
    var search = sanitizeSearch(req.query.search, 100);
    var role = (req.query.role || '').trim().slice(0, 20);

    var where = [];
    var params = [];
    if (search) { where.push('u.phone LIKE ?'); params.push('%' + search + '%'); }
    if (role) { where.push('u.role = ?'); params.push(role); }
    var whereClause = where.length > 0 ? 'WHERE ' + where.join(' AND ') : '';

    var [[{ cnt: total }]] = await pool.execute(
      'SELECT COUNT(*) AS cnt FROM users u ' + whereClause,
      params
    );

    params.push(limit, offset);
    var dataSql = 'SELECT u.id, u.phone, u.role, u.status, u.created_at, f.name AS family_name FROM users u LEFT JOIN family_groups f ON u.family_group_id = f.id ' + whereClause + ' ORDER BY u.created_at DESC LIMIT ? OFFSET ?';
    try {
      var [rows] = await pool.execute(dataSql, params);
    } catch (sqlErr) {
      console.error('/api/dashboard/users SQL error:', sqlErr.message, '| SQL:', dataSql, '| params:', JSON.stringify(params));
      throw sqlErr;
    }

    res.json({
      users: rows.map(function(r) {
        return {
          id: r.id,
          phone: r.phone || '',
          role: r.role,
          createdAt: r.created_at,
          familyName: r.family_name || '未加入',
          status: r.status || 'active',
        };
      }),
      total: total,
      page: page,
      limit: limit,
      totalPages: Math.ceil(total / limit),
    });
  } catch (err) {
    console.error('/api/dashboard/users error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.get('/api/dashboard/users/:id', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var [[user]] = await pool.execute(
      'SELECT u.id, u.phone, u.role, u.status, u.created_at, u.last_login_at, f.name AS family_name FROM users u LEFT JOIN family_groups f ON u.family_group_id = f.id WHERE u.id = ?',
      [req.params.id]
    );
    if (!user) return res.status(404).json({ error: '用户不存在' });
    res.json({
      id: user.id,
      phone: user.phone || '',
      role: user.role,
      createdAt: user.created_at,
      lastLoginAt: user.last_login_at,
      familyName: user.family_name || '未加入',
      status: user.status || 'active',
    });
  } catch (err) {
    console.error('/api/dashboard/users/:id error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.delete('/api/dashboard/users/:id', requireDashboard, dashboardApiLimiter, csrfCheck, async function(req, res) {
  try {
    var userId = parseInt(req.params.id, 10);
    // 不能删除自己（管理员账号在 users 表中对应的用户）
    // 此处 userId 为要删除的普通用户ID，管理员通过 dashboard session 登录，不一定是 users 表中的用户
    // 安全校验：不允许删除 users 表中手机号为管理员账号的记录
    var [[targetUser]] = await pool.execute('SELECT phone FROM users WHERE id = ?', [userId]);
    if (!targetUser) return res.status(404).json({ error: '用户不存在' });
    var adminPhone = process.env.DASHBOARD_USER || 'admin';
    if (targetUser.phone === adminPhone) {
      return res.status(400).json({ error: '不能删除管理员账号' });
    }
    // 级联清理关联数据
    var conn = await pool.getConnection();
    try {
      await conn.beginTransaction();
      await conn.execute('DELETE FROM sms_codes WHERE phone = ?', [targetUser.phone]);
      await conn.execute('DELETE FROM ratings WHERE user_id = ?', [userId]);
      await conn.execute('DELETE FROM order_items WHERE order_id IN (SELECT id FROM orders WHERE user_id = ?)', [userId]);
      await conn.execute('DELETE FROM orders WHERE user_id = ?', [userId]);
      await conn.execute('DELETE FROM bind_requests WHERE from_user_id = ? OR to_user_id = ?', [userId, userId]);
      await conn.execute('DELETE FROM users WHERE id = ?', [userId]);
      await conn.commit();
      res.json({ success: true, message: '已删除用户及相关数据' });
    } catch (err2) {
      await conn.rollback();
      console.error('DELETE user cascade error:', err2);
      res.status(500).json({ error: '服务器错误' });
    } finally {
      conn.release();
    }
  } catch (err) {
    console.error('DELETE /api/dashboard/users/:id error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// 禁用用户
app.put('/api/dashboard/users/:id/disable', requireDashboard, dashboardApiLimiter, csrfCheck, async function(req, res) {
  try {
    var userId = parseInt(req.params.id, 10);
    var [[user]] = await pool.execute('SELECT id, phone FROM users WHERE id = ?', [userId]);
    if (!user) return res.status(404).json({ error: '用户不存在' });
    await pool.execute('UPDATE users SET status = ? WHERE id = ?', ['disabled', userId]);
    res.json({ success: true, message: '已禁用' });
  } catch (err) {
    console.error('PUT /api/dashboard/users/:id/disable error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// 启用用户
app.put('/api/dashboard/users/:id/enable', requireDashboard, dashboardApiLimiter, csrfCheck, async function(req, res) {
  try {
    var userId = parseInt(req.params.id, 10);
    var [[user]] = await pool.execute('SELECT id FROM users WHERE id = ?', [userId]);
    if (!user) return res.status(404).json({ error: '用户不存在' });
    await pool.execute('UPDATE users SET status = ? WHERE id = ?', ['active', userId]);
    res.json({ success: true, message: '已启用' });
  } catch (err) {
    console.error('PUT /api/dashboard/users/:id/enable error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.get('/api/dashboard/families', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var page = parseInt(req.query.page, 10) || 1;
    var limit = parseInt(req.query.limit, 10) || 20;
    var offset = (page - 1) * limit;
    var search = sanitizeSearch(req.query.search, 100);

    var where = '';
    var params = [];
    if (search) {
      where = 'WHERE f.name LIKE ? OR f.invite_code LIKE ?';
      params.push('%' + search + '%', '%' + search + '%');
    }

    var [[{ cnt: total }]] = await pool.execute(
      'SELECT COUNT(*) AS cnt FROM family_groups f ' + where,
      params
    );

    params.push(limit, offset);
    var [rows] = await pool.execute(
      'SELECT f.id, f.name, f.invite_code, f.created_at, (SELECT COUNT(*) FROM users WHERE family_group_id = f.id) AS member_count FROM family_groups f ' + where + ' ORDER BY f.created_at DESC LIMIT ? OFFSET ?',
      params
    );

    res.json({
      families: rows.map(function(r) {
        return {
          id: r.id, name: r.name, inviteCode: r.invite_code,
          memberCount: r.member_count, createdAt: r.created_at,
          status: 'active',
        };
      }),
      total: total,
      page: page,
      limit: limit,
      totalPages: Math.ceil(total / limit),
    });
  } catch (err) {
    console.error('/api/dashboard/families error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.get('/api/dashboard/families/:id', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var [[fg]] = await pool.execute(
      'SELECT f.id, f.name, f.invite_code, f.created_at, (SELECT COUNT(*) FROM users WHERE family_group_id = f.id) AS member_count FROM family_groups f WHERE f.id = ?',
      [req.params.id]
    );
    if (!fg) return res.status(404).json({ error: '家庭组不存在' });

    var [members] = await pool.execute(
      'SELECT id, phone, role, created_at FROM users WHERE family_group_id = ? ORDER BY created_at',
      [fg.id]
    );

    res.json({
      id: fg.id,
      name: fg.name,
      inviteCode: fg.invite_code,
      createdAt: fg.created_at,
      memberCount: fg.member_count,
      members: members.map(function(m) {
        return {
          id: m.id,
          phone: m.phone || '',
          role: m.role,
          createdAt: m.created_at,
        };
      }),
    });
  } catch (err) {
    console.error('/api/dashboard/families/:id error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.get('/api/dashboard/family/:id/members', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var [[fg]] = await pool.execute('SELECT id, name FROM family_groups WHERE id = ?', [req.params.id]);
    if (!fg) return res.status(404).json({ error: '家庭组不存在' });

    var [members] = await pool.execute(
      'SELECT id, phone, role, created_at FROM users WHERE family_group_id = ? ORDER BY created_at',
      [fg.id]
    );

    res.json({
      familyId: fg.id,
      familyName: fg.name,
      members: members.map(function(m) {
        var ph = m.phone || '';
        return {
          id: m.id,
          phone: ph.length >= 11 ? ph.substring(0, 3) + '****' + ph.substring(7) : ph,
          role: m.role,
          createdAt: m.created_at,
        };
      }),
    });
  } catch (err) {
    console.error('/api/dashboard/family/:id/members error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

app.delete('/api/dashboard/families/:id', requireDashboard, dashboardApiLimiter, csrfCheck, async function(req, res) {
  try {
    var fgId = parseInt(req.params.id, 10);
    var [[fg]] = await pool.execute('SELECT id FROM family_groups WHERE id = ?', [fgId]);
    if (!fg) return res.status(404).json({ error: '家庭组不存在' });

    await pool.execute('UPDATE users SET family_group_id = NULL WHERE family_group_id = ?', [fgId]);
    await pool.execute('DELETE FROM family_groups WHERE id = ?', [fgId]);
    res.json({ success: true, message: '已删除' });
  } catch (err) {
    console.error('DELETE /api/dashboard/families/:id error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// 订单记录
app.get('/api/dashboard/orders', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var page = parseInt(req.query.page, 10) || 1;
    var limit = parseInt(req.query.limit, 10) || 20;
    var offset = (page - 1) * limit;
    var status = (req.query.status || '').trim();

    var where = 'WHERE o.deleted_at IS NULL';
    var countParams = [];
    var dataParams = [];
    if (status) { where += ' AND o.status = ?'; countParams.push(status); dataParams.push(status); }

    var [[{ cnt: total }]] = await pool.execute(
      'SELECT COUNT(*) AS cnt FROM orders o ' + where,
      countParams
    );

    var [rows] = await pool.execute(
      'SELECT o.id, o.status, o.created_at, u.phone FROM orders o LEFT JOIN users u ON o.user_id = u.id ' + where + ' ORDER BY o.created_at DESC LIMIT ' + limit + ' OFFSET ' + offset,
      dataParams
    );

    var orderIds = rows.map(function(r) { return r.id; });
    var itemsMap = {};
    if (orderIds.length > 0) {
      var [itemRows] = await pool.execute(
        'SELECT order_id, dish_name FROM order_items WHERE order_id IN (' + orderIds.map(function() { return '?'; }).join(',') + ')',
        orderIds
      );
      itemRows.forEach(function(it) {
        if (!itemsMap[it.order_id]) itemsMap[it.order_id] = [];
        itemsMap[it.order_id].push(it.dish_name);
      });
    }

    res.json({
      orders: rows.map(function(r) {
        var ph = r.phone || '';
        return {
          id: r.id,
          phone: ph.length >= 11 ? ph.substring(0, 3) + '****' + ph.substring(7) : ph,
          dishNames: itemsMap[r.id] || [],
          status: r.status,
          createdAt: r.created_at,
        };
      }),
      total: total,
      page: page,
      limit: limit,
      totalPages: Math.ceil(total / limit),
    });
  } catch (err) {
    console.error('GET /api/dashboard/orders error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// 菜品列表
app.get('/api/dashboard/dishes', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var page = parseInt(req.query.page, 10) || 1;
    var limit = parseInt(req.query.limit, 10) || 20;
    var offset = (page - 1) * limit;
    var search = sanitizeSearch(req.query.search, 100);

    var where = '';
    var params = [];
    if (search) {
      where = 'WHERE d.name LIKE ?';
      params.push('%' + search + '%');
    }

    var [[{ cnt: total }]] = await pool.execute(
      'SELECT COUNT(*) AS cnt FROM dishes d ' + where,
      params
    );

    var [rows] = await pool.execute(
      'SELECT d.id, d.name, d.is_available, d.avg_rating, COALESCE(c.name, \'\') AS category, COALESCE(f.name, \'\') AS family_name FROM dishes d LEFT JOIN categories c ON d.category_id = c.id LEFT JOIN family_groups f ON d.family_group_id = f.id ' + where + ' ORDER BY d.id DESC LIMIT ' + limit + ' OFFSET ' + offset,
      params
    );

    res.json({
      dishes: rows.map(function(r) {
        return {
          id: r.id,
          name: r.name,
          category: r.category || '未分类',
          familyName: r.family_name || '未归属',
          avgRating: Number(r.avg_rating) || 0,
          isAvailable: !!r.is_available,
        };
      }),
      total: total,
      page: page,
      limit: limit,
      totalPages: Math.ceil(total / limit),
    });
  } catch (err) {
    console.error('GET /api/dashboard/dishes error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// 系统设置 - 平台信息
app.get('/api/dashboard/settings', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var uptime = Math.floor((Date.now() - serverStartTime.getTime()) / 1000);
    var days = Math.floor(uptime / 86400);
    var hours = Math.floor((uptime % 86400) / 3600);
    var minutes = Math.floor((uptime % 3600) / 60);
    var uptimeStr = days > 0 ? days + '天 ' + hours + '小时 ' + minutes + '分钟' : hours + '小时 ' + minutes + '分钟';
    res.json({
      platformName: '家庭私厨 · 管理后台',
      version: '1.0.0',
      startTime: serverStartTime.toISOString(),
      uptime: uptimeStr,
    });
  } catch (err) {
    res.status(500).json({ error: '服务器错误' });
  }
});

// 系统设置 - 修改管理员密码
app.put('/api/dashboard/settings/password', requireDashboard, dashboardApiLimiter, csrfCheck, async function(req, res) {
  try {
    var oldPassword = (req.body.oldPassword || '').trim();
    var newPassword = (req.body.newPassword || '').trim();
    if (!oldPassword || !newPassword) {
      return res.json({ success: false, message: '旧密码和新密码不能为空' });
    }
    if (newPassword.length < 8) {
      return res.json({ success: false, message: '新密码至少8位' });
    }
    if (newPassword.length > 128) {
      return res.json({ success: false, message: '新密码不能超过128位' });
    }
    var hasUpper = /[A-Z]/.test(newPassword);
    var hasLower = /[a-z]/.test(newPassword);
    var hasDigit = /\d/.test(newPassword);
    if (!hasUpper || !hasLower || !hasDigit) {
      return res.json({ success: false, message: '新密码必须包含大写字母、小写字母和数字' });
    }
    if (!adminPassHash) {
      return res.json({ success: false, message: '未配置管理员密码' });
    }
    var ok = await verifyAdminPassword(oldPassword);
    if (!ok) {
      return res.json({ success: false, message: '旧密码错误' });
    }
    adminPassHash = await bcrypt.hash(newPassword, 12);
    await pool.execute("INSERT INTO admin_settings (setting_key, setting_value) VALUES ('admin_pass_hash', ?) ON DUPLICATE KEY UPDATE setting_value = VALUES(setting_value)", [adminPassHash]);
    res.json({ success: true, message: '密码修改成功' });
  } catch (err) {
    console.error('PUT /api/dashboard/settings/password error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// 实时动态墙
app.get('/api/dashboard/live-feed', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var filter = sanitizeSearch(req.query.filter, 100);
    var rows = [];

    // 注册事件
    var [regRows] = await pool.execute(
      'SELECT phone, role, created_at FROM users ORDER BY created_at DESC LIMIT 15'
    );
    regRows.forEach(function(r) {
      var ph = (r.phone || '');
      rows.push({
        type: '注册', category: '系统',
        desc: '新用户 ' + (ph.length >= 11 ? ph.substring(0,3)+'****'+ph.substring(7) : ph) + ' 注册为' + (r.role === 'chef' ? '大厨' : '点菜员'),
        time: r.created_at,
      });
    });

    // 订单事件
    var [orderRows] = await pool.execute(
      'SELECT o.id, o.status, o.created_at, u.phone FROM orders o LEFT JOIN users u ON o.user_id = u.id WHERE o.deleted_at IS NULL ORDER BY o.created_at DESC LIMIT 20'
    );
    orderRows.forEach(function(r) {
      var ph = (r.phone || '');
      var statusText = { pending: '提交了点菜', accepted: '的订单已被接单', cooking: '的订单开始烹饪', completed: '的订单已完成', rejected: '的订单被拒绝' };
      rows.push({
        type: '点菜', category: '点菜',
        desc: (ph.length >= 11 ? ph.substring(0,3)+'****'+ph.substring(7) : ph) + (statusText[r.status] || ' 更新了订单'),
        time: r.created_at,
      });
    });

    // 评分事件
    var [rateRows] = await pool.execute(
      'SELECT r.rating, r.created_at, u.phone, d.name AS dish_name FROM ratings r LEFT JOIN users u ON r.user_id = u.id LEFT JOIN dishes d ON r.dish_id = d.id ORDER BY r.created_at DESC LIMIT 10'
    );
    rateRows.forEach(function(r) {
      var ph = r.phone || '';
      rows.push({
        type: '评分', category: '烹饪',
        desc: (ph.length >= 11 ? ph.substring(0,3)+'****'+ph.substring(7) : ph) + ' 给「' + (r.dish_name || '菜品') + '」评了 ' + r.rating + ' 星',
        time: r.created_at,
      });
    });

    // 绑定事件
    var [bindRows] = await pool.execute(
      'SELECT br.status, br.created_at, u.phone FROM bind_requests br JOIN users u ON br.applicant_id = u.id WHERE br.status IN (\'approved\', \'pending\') ORDER BY br.created_at DESC LIMIT 10'
    );
    bindRows.forEach(function(r) {
      var ph = r.phone || '';
      rows.push({
        type: '绑定', category: '系统',
        desc: (ph.length >= 11 ? ph.substring(0,3)+'****'+ph.substring(7) : ph) + (r.status === 'approved' ? ' 成功绑定家庭组' : ' 申请绑定家庭组'),
        time: r.created_at,
      });
    });

    // 排序取最近30条
    rows.sort(function(a, b) { return new Date(b.time) - new Date(a.time); });
    var liveFeed = rows.slice(0, 30);

    if (filter) {
      liveFeed = liveFeed.filter(function(r) { return r.category === filter; });
    }

    res.json({ liveFeed: liveFeed });
  } catch (err) {
    console.error('GET /api/dashboard/live-feed error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// 智能建议
app.get('/api/dashboard/suggestions', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    var suggestions = [];

    // 本周 vs 上周对比
    var [[{ cnt: thisWeek }]] = await pool.execute(
      'SELECT COUNT(*) AS cnt FROM orders WHERE deleted_at IS NULL AND created_at >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)'
    );
    var [[{ cnt: lastWeek }]] = await pool.execute(
      'SELECT COUNT(*) AS cnt FROM orders WHERE deleted_at IS NULL AND created_at >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AND created_at < DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY)'
    );
    if (lastWeek > 0) {
      var change = Math.round((thisWeek - lastWeek) / lastWeek * 100);
      if (change > 10) {
        suggestions.push('本周订单量较上周增长 ' + change + '%，平台活跃度上升，建议关注食材储备');
      } else if (change < -10) {
        suggestions.push('本周订单量较上周下降 ' + Math.abs(change) + '%，建议推送消息提醒用户点菜');
      }
    }

    // 热门菜品涨幅
    var [hotDishes] = await pool.execute(
      'SELECT d.name, COUNT(*) AS cnt FROM order_items oi JOIN orders o ON oi.order_id = o.id JOIN dishes d ON oi.dish_id = d.id WHERE o.deleted_at IS NULL AND o.created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) GROUP BY d.id, d.name ORDER BY cnt DESC LIMIT 1'
    );
    if (hotDishes.length > 0 && hotDishes[0].cnt >= 3) {
      suggestions.push('近7天「' + hotDishes[0].name + '」点单 ' + hotDishes[0].cnt + ' 次，是最受欢迎的菜品，建议多备相关食材');
    }

    // 不活跃家庭组
    var [inactiveFgs] = await pool.execute(
      'SELECT f.name FROM family_groups f WHERE f.id NOT IN (SELECT DISTINCT family_group_id FROM orders WHERE deleted_at IS NULL AND created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND family_group_id IS NOT NULL) LIMIT 3'
    );
    if (inactiveFgs.length > 0) {
      var names = inactiveFgs.map(function(f) { return '「' + f.name + '」'; }).join('、');
      suggestions.push(names + ' 近7天未活跃，建议推送提醒或优惠活动激活用户');
    }

    // 确保至少3条建议
    if (suggestions.length < 3) {
      suggestions.push('平台运行正常，各项指标健康，继续保持运营节奏');
    }
    if (suggestions.length < 3) {
      suggestions.push('定期更新菜品信息有助于提升用户体验，建议每周审核菜品');
    }
    if (suggestions.length < 3) {
      suggestions.push('关注用户评价反馈，及时调整菜品配方和服务流程');
    }

    res.json({ suggestions: suggestions.slice(0, 3) });
  } catch (err) {
    console.error('GET /api/dashboard/suggestions error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// 数据洞察
app.get('/api/dashboard/insights', requireDashboard, dashboardApiLimiter, async function(req, res) {
  try {
    // 热门菜品 Top 5（本周）
    var [hotDishes] = await pool.execute(
      'SELECT d.name, COUNT(*) AS cnt FROM order_items oi JOIN orders o ON oi.order_id = o.id JOIN dishes d ON oi.dish_id = d.id WHERE o.deleted_at IS NULL AND o.created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) GROUP BY d.id, d.name ORDER BY cnt DESC LIMIT 5'
    );

    // 活跃时段热力图（7天×24小时）
    var [hourRows] = await pool.execute(
      'SELECT HOUR(created_at) AS h, DAYOFWEEK(created_at) AS dow, COUNT(*) AS cnt FROM orders WHERE deleted_at IS NULL AND created_at >= DATE_SUB(CURDATE(), INTERVAL 6 DAY) GROUP BY HOUR(created_at), DAYOFWEEK(created_at)'
    );
    var heatmap = [];
    for (var d = 0; d < 7; d++) {
      var dayData = [];
      for (var h = 0; h < 24; h++) {
        dayData.push(0);
      }
      heatmap.push(dayData);
    }
    hourRows.forEach(function(r) {
      var dayIdx = r.dow - 1; // MySQL DAYOFWEEK: 1=Sunday
      heatmap[dayIdx][r.h] = r.cnt;
    });

    // 家庭组活跃排行（本周点菜次数）
    var [fgRankings] = await pool.execute(
      'SELECT f.name, COUNT(*) AS cnt FROM orders o JOIN family_groups f ON o.family_group_id = f.id WHERE o.deleted_at IS NULL AND o.created_at >= DATE_SUB(CURDATE(), INTERVAL 7 DAY) AND o.family_group_id IS NOT NULL GROUP BY f.id, f.name ORDER BY cnt DESC LIMIT 5'
    );

    res.json({
      hotDishes: hotDishes,
      heatmap: heatmap,
      fgRankings: fgRankings,
    });
  } catch (err) {
    console.error('GET /api/dashboard/insights error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// 今日智能推荐
app.get('/api/recommend/today', requireAuth, async function(req, res) {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var userId = req.session.userId;

    var result = [];

    // 获取所有菜品
    var [dishes] = await pool.execute(
      'SELECT d.id, d.name, d.ingredients, d.avg_rating, d.image FROM dishes d WHERE d.' + scopeCol + ' = ? AND d.is_available = 1',
      [scopeVal]
    );
    if (dishes.length === 0) return res.json({ recommendations: [] });

    // ① 本周点菜次数最多的菜（热门）
    var [hotRows] = await pool.execute(
      'SELECT oi.dish_id, COUNT(*) AS cnt FROM order_items oi JOIN orders o ON oi.order_id = o.id WHERE o.' + scopeCol + ' = ? AND o.deleted_at IS NULL AND o.created_at >= DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) DAY) GROUP BY oi.dish_id ORDER BY cnt DESC LIMIT 3',
      [scopeVal]
    );

    // ② 上次吃的时间最久远的菜（避免重复）
    var [lastRows] = await pool.execute(
      'SELECT oi.dish_id, MAX(o.created_at) AS last_time FROM order_items oi JOIN orders o ON oi.order_id = o.id WHERE o.' + scopeCol + ' = ? AND o.deleted_at IS NULL GROUP BY oi.dish_id ORDER BY last_time ASC',
      [scopeVal]
    );

    // ③ 评分最高的菜
    var [ratedRows] = await pool.execute(
      'SELECT d.id, d.name, d.avg_rating FROM dishes d WHERE d.' + scopeCol + ' = ? AND d.avg_rating > 0 ORDER BY d.avg_rating DESC LIMIT 3',
      [scopeVal]
    );

    // ④ 食材库存充足的菜
    var [invRows] = await pool.execute(
      'SELECT ingredient_name, quantity FROM inventory WHERE ' + scopeCol + ' = ?',
      [scopeVal]
    );
    var stockMap = {};
    invRows.forEach(function(inv) { stockMap[inv.ingredient_name] = inv.quantity; });

    // 遍历菜品打分
    var scores = {};
    dishes.forEach(function(d) {
      var score = 0;
      var reasons = [];

      // 热度分
      var hot = hotRows.find(function(h) { return h.dish_id === d.id; });
      if (hot) { score += Math.min(hot.cnt, 10) * 3; reasons.push('本周点了 ' + hot.cnt + ' 次'); }

      // 上次时间分（越久分越高）
      var last = lastRows.find(function(l) { return l.dish_id === d.id; });
      if (last && last.last_time) {
        var daysAgo = Math.floor((Date.now() - new Date(last.last_time).getTime()) / 86400000);
        if (daysAgo > 7) { score += 5; reasons.push('上次吃是 ' + daysAgo + ' 天前'); }
        else if (daysAgo > 3) { score += 2; reasons.push('上次吃是 ' + daysAgo + ' 天前'); }
      } else if (!last) {
        score += 3; reasons.push('还没点过这道菜');
      }

      // 评分分
      var rated = ratedRows.find(function(r) { return r.id === d.id; });
      if (rated && rated.avg_rating >= 4) { score += 5; reasons.push('评分 ' + Number(rated.avg_rating).toFixed(1) + ' ⭐'); }
      else if (rated && rated.avg_rating >= 3) { score += 2; }

      // 库存分
      var stockOk = true;
      var ings = [];
      try { ings = JSON.parse(d.ingredients || '[]'); } catch(_) {}
      if (Object.keys(stockMap).length > 0) {
        ings.forEach(function(ing) {
          if (stockMap[ing] !== undefined && stockMap[ing] <= 0) stockOk = false;
        });
      }
      if (stockOk && Object.keys(stockMap).length > 0) { score += 2; reasons.push('食材充足'); }

      scores[d.id] = { score: score, reasons: reasons, name: d.name, image: d.image, id: d.id };
    });

    // 排序取前3
    var sorted = Object.values(scores).sort(function(a, b) { return b.score - a.score; });
    result = sorted.slice(0, 3).map(function(s) {
      var reason = s.reasons.length > 0 ? s.reasons.slice(0, 2).join('，') : '今日推荐';
      return { id: s.id, name: s.name, image: s.image, reason: reason };
    });

    res.json({ recommendations: result });
  } catch (err) {
    console.error('/api/recommend/today error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// 一键生成今日菜单
app.post('/api/menu/generate', requireAuth, async function(req, res) {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;

    var [dishes] = await pool.execute(
      'SELECT d.id, d.name, d.ingredients, d.avg_rating, d.image FROM dishes d WHERE d.' + scopeCol + ' = ? AND d.is_available = 1',
      [scopeVal]
    );
    if (dishes.length === 0) return res.json({ success: false, message: '暂无可用菜品' });

    // 随机选 1 道主菜 + 最多 1 道配菜
    var shuffled = dishes.sort(function() { return Math.random() - 0.5; });
    var mainDish = shuffled[0];
    var sideDish = shuffled.length > 1 ? shuffled[1] : null;
    if (sideDish && sideDish.id === mainDish.id) sideDish = shuffled.length > 2 ? shuffled[2] : null;

    var resultDishes = [mainDish];
    if (sideDish) resultDishes.push(sideDish);

    // 采购清单
    var ingredientSet = {};
    resultDishes.forEach(function(d) {
      var ings = [];
      try { ings = JSON.parse(d.ingredients || '[]'); } catch(_) {}
      ings.forEach(function(ing) { if (ing.trim()) ingredientSet[ing.trim()] = true; });
    });
    var shoppingList = Object.keys(ingredientSet);

    res.json({
      success: true,
      menu: resultDishes.map(function(d) {
        return { id: d.id, name: d.name, avgRating: Number(d.avg_rating) || 0, image: d.image || '' };
      }),
      shoppingList: shoppingList,
    });
  } catch (err) {
    console.error('/api/menu/generate error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// 厨房实时状态
app.get('/api/family/cooking-status', requireAuth, async function(req, res) {
  try {
    var scopeCol = req.scopeColumn || 'user_id';
    var scopeVal = req.scopeValue || req.session.userId;
    var [rows] = await pool.execute(
      'SELECT o.id, o.status FROM orders o WHERE o.' + scopeCol + ' = ? AND o.deleted_at IS NULL AND o.status = ? ORDER BY o.created_at DESC LIMIT 1',
      [scopeVal, 'cooking']
    );
    if (rows.length === 0) return res.json({ cooking: false });

    var orderId = rows[0].id;
    var [items] = await pool.execute(
      'SELECT dish_name FROM order_items WHERE order_id = ?', [orderId]
    );
    var dishName = items.map(function(i) { return i.dish_name; }).join('、') || '未知菜品';

    res.json({ cooking: true, dishName: dishName, orderId: String(orderId) });
  } catch (err) {
    console.error('/api/family/cooking-status error:', err);
    res.status(500).json({ error: '服务器错误' });
  }
});

// 网站运营总时长
app.get('/api/dashboard/uptime', requireDashboard, dashboardApiLimiter, async function(req, res) {
  res.json({ launchDate: SITE_LAUNCH_DATE });
});

module.exports = { requireAuth, pool };
