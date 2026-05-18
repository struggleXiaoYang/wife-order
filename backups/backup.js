// 自动数据库备份脚本
// 用法: node backups/backup.js
// 配合 node-cron 或 Windows 任务计划程序 / Railway Cron 使用

const mysqldump = require('mysqldump');
const fs = require('fs');
const path = require('path');
require('dotenv').config({ path: path.join(__dirname, '..', '.env') });

const BACKUP_DIR = __dirname;
const KEEP_DAYS = 7; // 保留最近7天

async function run() {
  var now = new Date();
  var stamp = now.getFullYear() + '-' + String(now.getMonth()+1).padStart(2,'0') + '-' + String(now.getDate()).padStart(2,'0') + '_' + String(now.getHours()).padStart(2,'0') + String(now.getMinutes()).padStart(2,'0');
  var file = path.join(BACKUP_DIR, 'wife_order_' + stamp + '.sql.gz');

  console.log(new Date().toISOString() + ' [BACKUP] start → ' + file);
  try {
    await mysqldump({
      connection: {
        host: process.env.DB_HOST || 'localhost',
        port: parseInt(process.env.DB_PORT, 10) || 3306,
        user: process.env.DB_USER || 'root',
        password: process.env.DB_PASSWORD || '',
        database: process.env.DB_NAME || 'wife_order',
      },
      dumpToFile: file,
      compressFile: true,
    });
    console.log(new Date().toISOString() + ' [BACKUP] done');

    // 清理旧备份
    var files = fs.readdirSync(BACKUP_DIR).filter(function(f) { return f.endsWith('.sql.gz'); }).sort();
    while (files.length > KEEP_DAYS) {
      var old = path.join(BACKUP_DIR, files[0]);
      fs.unlinkSync(old);
      console.log(new Date().toISOString() + ' [BACKUP] removed old: ' + files[0]);
      files.shift();
    }
  } catch (e) {
    console.error(new Date().toISOString() + ' [BACKUP] ERROR:', e.message);
  }
}

run();
