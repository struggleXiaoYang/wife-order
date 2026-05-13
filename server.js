const express = require('express');
const fs = require('fs');
const path = require('path');

const session = require('express-session');

const app = express();
const DATA_FILE = path.join(__dirname, 'data.json');

app.use(express.static(path.join(__dirname, 'public')));
app.use(express.json());
app.use(express.urlencoded({ extended: true }));
app.use(session({
  secret: process.env.SESSION_SECRET || 'wife-order-secret',
  resave: false,
  saveUninitialized: false,
}));

// 管理端鉴权中间件
function requireAdmin(req, res, next) {
  if (req.session && req.session.isAdmin) return next();
  res.redirect('/admin/login');
}
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

function readData() {
  return JSON.parse(fs.readFileSync(DATA_FILE, 'utf-8'));
}

function writeData(data) {
  fs.writeFileSync(DATA_FILE, JSON.stringify(data, null, 2), 'utf-8');
}

// 菜单数据（分类列表）
app.get('/api/menu', (req, res) => {
  const data = readData();
  res.json({ categories: data.categories });
});

// 首页
app.get('/', (req, res) => {
  const data = readData();
  res.render('index', { dishes: data.dishes });
});

// 提交订单（下单）
app.post('/api/order', (req, res) => {
  const { dishIds } = req.body;

  if (!Array.isArray(dishIds) || dishIds.length === 0) {
    return res.status(400).json({ error: '请至少选一道菜' });
  }

  const data = readData();

  const order = {
    id: Date.now().toString(),
    dishIds,
    createdAt: new Date().toISOString(),
    status: 'pending',
  };
  data.orders.push(order);
  writeData(data);

  res.json({ success: true, order });
});

// 客户端历史订单 API
app.get('/api/orders', (req, res) => {
  const data = readData();
  const dishMap = {};
  data.dishes.forEach(function(d) { dishMap[d.id] = d.name; });

  const orders = data.orders
    .map(function(o) {
      return {
        id: o.id,
        dishIds: o.dishIds,
        dishNames: o.dishIds.map(function(id) { return dishMap[id] || '未知菜品'; }),
        createdAt: o.createdAt,
        status: o.status,
      };
    })
    .reverse();

  res.json({ orders });
});

// 历史记录页
app.get('/history', (req, res) => {
  res.render('history');
});

// ========== 管理端路由 ==========

// 登录页
app.get('/admin/login', (req, res) => {
  res.render('admin/login', { error: '' });
});

// 登录验证
app.post('/admin/login', (req, res) => {
  const data = readData();
  if (req.body.password === data.password) {
    req.session.isAdmin = true;
    res.redirect('/admin');
  } else {
    res.render('admin/login', { error: '密码错误' });
  }
});

// 退出
app.get('/admin/logout', (req, res) => {
  req.session.destroy();
  res.redirect('/admin/login');
});

// 订单数据 API（需登录）
app.get('/api/admin/orders', requireAdmin, (req, res) => {
  const data = readData();
  const dishMap = {};
  data.dishes.forEach(function(d) { dishMap[d.id] = d.name; });

  const orders = data.orders
    .map(function(o) {
      return {
        id: o.id,
        dishIds: o.dishIds,
        dishNames: o.dishIds.map(function(id) { return dishMap[id] || '未知菜品'; }),
        createdAt: o.createdAt,
        status: o.status,
      };
    })
    .reverse();

  res.json({ orders });
});

// 管理主页（需登录）
app.get('/admin', requireAdmin, (req, res) => {
  res.render('admin/home');
});

// 菜品数据 API（需登录）
app.get('/api/admin/dishes', requireAdmin, (req, res) => {
  const data = readData();
  res.json({ dishes: data.dishes });
});

// 删除菜品
app.delete('/api/admin/dishes/:id', requireAdmin, (req, res) => {
  const data = readData();
  const idx = data.dishes.findIndex(function(d) { return d.id === req.params.id; });
  if (idx === -1) return res.status(404).json({ error: '菜品不存在' });

  data.dishes.splice(idx, 1);
  writeData(data);
  res.json({ success: true });
});

// 分类列表 API（需登录）
app.get('/api/admin/categories', requireAdmin, (req, res) => {
  const data = readData();
  res.json({ categories: data.categories });
});

// 新增分类
app.post('/api/admin/categories', requireAdmin, (req, res) => {
  var name = (req.body.name || '').trim();
  if (!name) return res.status(400).json({ error: '分类名不能为空' });

  var data = readData();
  if (data.categories.indexOf(name) !== -1) return res.status(400).json({ error: '分类已存在' });

  data.categories.push(name);
  writeData(data);
  res.json({ success: true });
});

// 重命名分类
app.put('/api/admin/categories/:name', requireAdmin, (req, res) => {
  var newName = (req.body.newName || '').trim();
  if (!newName) return res.status(400).json({ error: '新名称不能为空' });

  var data = readData();
  var idx = data.categories.indexOf(req.params.name);
  if (idx === -1) return res.status(404).json({ error: '分类不存在' });

  // 更新所有使用该分类的菜品
  data.dishes.forEach(function(d) {
    if (d.category === req.params.name) d.category = newName;
  });

  data.categories[idx] = newName;
  writeData(data);
  res.json({ success: true });
});

// 删除分类
app.delete('/api/admin/categories/:name', requireAdmin, (req, res) => {
  var data = readData();
  var idx = data.categories.indexOf(req.params.name);
  if (idx === -1) return res.status(404).json({ error: '分类不存在' });

  var hasDish = data.dishes.some(function(d) { return d.category === req.params.name; });
  if (hasDish) return res.status(400).json({ error: '该分类下有菜品，无法删除' });

  data.categories.splice(idx, 1);
  writeData(data);
  res.json({ success: true });
});

// 新增菜品
app.post('/api/admin/dishes', requireAdmin, (req, res) => {
  const data = readData();
  var dish = {
    id: Date.now().toString(),
    name: req.body.name || '',
    category: req.body.category || '',
    image: req.body.image || '',
    ingredients: req.body.ingredients || [],
    steps: req.body.steps || [],
    isAvailable: req.body.isAvailable === true || req.body.isAvailable === 'true',
  };
  data.dishes.push(dish);
  writeData(data);
  res.json({ success: true, dish: dish });
});

// 更新菜品
app.put('/api/admin/dishes/:id', requireAdmin, (req, res) => {
  const data = readData();
  var dish = data.dishes.find(function(d) { return d.id === req.params.id; });
  if (!dish) return res.status(404).json({ error: '菜品不存在' });

  if (req.body.name !== undefined) dish.name = req.body.name;
  if (req.body.category !== undefined) dish.category = req.body.category;
  if (req.body.image !== undefined) dish.image = req.body.image;
  if (req.body.ingredients !== undefined) dish.ingredients = req.body.ingredients;
  if (req.body.steps !== undefined) dish.steps = req.body.steps;
  if (req.body.isAvailable !== undefined) dish.isAvailable = req.body.isAvailable === true || req.body.isAvailable === 'true';

  writeData(data);
  res.json({ success: true });
});

// 订单标为完成
app.put('/api/admin/orders/:id', requireAdmin, (req, res) => {
  const data = readData();
  const order = data.orders.find(function(o) { return o.id === req.params.id; });
  if (!order) return res.status(404).json({ error: '订单不存在' });

  order.status = 'done';
  writeData(data);
  res.json({ success: true });
});

const PORT = process.env.PORT || 3000;
app.listen(PORT, () => {
  console.log('http://localhost:' + PORT);
  console.log('admin: http://localhost:' + PORT + '/admin');
});
