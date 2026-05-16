import { config } from 'dotenv';
import mysql from 'mysql2/promise';

config();

const { DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME } = process.env;

if (!DB_HOST || !DB_USER || !DB_PASSWORD || !DB_NAME) {
  console.error('缺少数据库环境变量，请检查 .env 文件中的 DB_HOST/DB_USER/DB_PASSWORD/DB_NAME');
  process.exit(1);
}

async function init() {
  const conn = await mysql.createConnection({
    host: DB_HOST,
    port: Number(DB_PORT) || 3306,
    user: DB_USER,
    password: DB_PASSWORD,
  });

  await conn.query(`CREATE DATABASE IF NOT EXISTS \`${DB_NAME}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci`);
  await conn.query(`USE \`${DB_NAME}\``);

  // ---------- 建表 ----------

  await conn.query(`
    CREATE TABLE IF NOT EXISTS users (
      id          INT             AUTO_INCREMENT PRIMARY KEY,
      phone       VARCHAR(20)     NOT NULL UNIQUE,
      nickname    VARCHAR(50)     DEFAULT '',
      avatar      VARCHAR(255)    DEFAULT '',
      password_hash VARCHAR(255)  DEFAULT '',
      is_admin    TINYINT(1)      DEFAULT 0,
      created_at  DATETIME        DEFAULT CURRENT_TIMESTAMP,
      updated_at  DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  await conn.query(`
    CREATE TABLE IF NOT EXISTS sms_codes (
      id          INT             AUTO_INCREMENT PRIMARY KEY,
      phone       VARCHAR(20)     NOT NULL,
      code        VARCHAR(6)      NOT NULL,
      used        TINYINT(1)      DEFAULT 0,
      expires_at  DATETIME        NOT NULL,
      created_at  DATETIME        DEFAULT CURRENT_TIMESTAMP,
      INDEX idx_phone_code (phone, code)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  await conn.query(`
    CREATE TABLE IF NOT EXISTS categories (
      id          INT             AUTO_INCREMENT PRIMARY KEY,
      name        VARCHAR(50)     NOT NULL,
      user_id     INT             DEFAULT NULL,
      sort_order  INT             DEFAULT 0,
      created_at  DATETIME        DEFAULT CURRENT_TIMESTAMP,
      UNIQUE KEY uk_name_user (name, user_id),
      INDEX idx_user (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  await conn.query(`
    CREATE TABLE IF NOT EXISTS dishes (
      id          INT             AUTO_INCREMENT PRIMARY KEY,
      name        VARCHAR(100)    NOT NULL,
      category_id INT             DEFAULT NULL,
      user_id     INT             DEFAULT NULL,
      image       VARCHAR(255)    DEFAULT '',
      price       DECIMAL(10,2)   DEFAULT 0.00,
      ingredients TEXT,
      steps       TEXT,
      is_available TINYINT(1)     DEFAULT 1,
      created_at  DATETIME        DEFAULT CURRENT_TIMESTAMP,
      updated_at  DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      FOREIGN KEY (category_id) REFERENCES categories(id) ON DELETE SET NULL,
      INDEX idx_user (user_id)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  await conn.query(`
    CREATE TABLE IF NOT EXISTS ratings (
      id          INT             AUTO_INCREMENT PRIMARY KEY,
      user_id     INT             DEFAULT NULL,
      dish_id     INT             NOT NULL,
      rating      TINYINT(1)      NOT NULL COMMENT '1-5',
      comment     TEXT,
      created_at  DATETIME        DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
      FOREIGN KEY (dish_id) REFERENCES dishes(id) ON DELETE CASCADE
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  await conn.query(`
    CREATE TABLE IF NOT EXISTS orders (
      id          INT             AUTO_INCREMENT PRIMARY KEY,
      user_id     INT             DEFAULT NULL,
      session_id  VARCHAR(255)    DEFAULT '',
      status      ENUM('pending','accepted','rejected','cooking','completed') NOT NULL DEFAULT 'pending',
      reject_reason TEXT,
      accepted_at DATETIME        DEFAULT NULL,
      completed_at DATETIME       DEFAULT NULL,
      deleted_at  DATETIME        DEFAULT NULL,
      created_at  DATETIME        DEFAULT CURRENT_TIMESTAMP,
      updated_at  DATETIME        DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
      FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL,
      INDEX idx_status (status),
      INDEX idx_created (created_at),
      INDEX idx_user_deleted (user_id, deleted_at)
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  await conn.query(`
    CREATE TABLE IF NOT EXISTS order_items (
      id          INT             AUTO_INCREMENT PRIMARY KEY,
      order_id    INT             NOT NULL,
      dish_id     INT             DEFAULT NULL,
      dish_name   VARCHAR(100)    NOT NULL,
      quantity    INT             DEFAULT 1,
      created_at  DATETIME        DEFAULT CURRENT_TIMESTAMP,
      FOREIGN KEY (order_id) REFERENCES orders(id) ON DELETE CASCADE,
      FOREIGN KEY (dish_id) REFERENCES dishes(id) ON DELETE SET NULL
    ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
  `);

  // ---------- 迁移：为已有表补加列 ----------
  const migrations = [
    `ALTER TABLE categories ADD COLUMN IF NOT EXISTS user_id INT DEFAULT NULL AFTER name`,
    `ALTER TABLE categories DROP INDEX name`,
    `ALTER TABLE categories ADD UNIQUE KEY IF NOT EXISTS uk_name_user (name, user_id)`,
    `ALTER TABLE categories ADD INDEX IF NOT EXISTS idx_user (user_id)`,
    `ALTER TABLE dishes ADD COLUMN IF NOT EXISTS user_id INT DEFAULT NULL AFTER category_id`,
    `ALTER TABLE dishes ADD INDEX IF NOT EXISTS idx_user (user_id)`,
    `ALTER TABLE orders ADD COLUMN IF NOT EXISTS deleted_at DATETIME DEFAULT NULL AFTER completed_at`,
    `ALTER TABLE orders ADD INDEX IF NOT EXISTS idx_user_deleted (user_id, deleted_at)`,
  ];

  for (const sql of migrations) {
    try { await conn.query(sql); } catch (_) { /* 列/索引已存在则跳过 */ }
  }

  // ---------- 种子管理员 ----------
  await conn.query(`
    INSERT IGNORE INTO users (phone, password_hash, is_admin, created_at)
    VALUES ('admin', '', 1, NOW())
  `);

  console.log(`数据库 ${DB_NAME} 及全部 7 张表初始化完成`);
  await conn.end();
}

init().catch((err) => {
  console.error('初始化失败:', err.message);
  process.exit(1);
});
