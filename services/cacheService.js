const NodeCache = require('node-cache');

// 默认 TTL 60s，每 120s 检查过期
const cache = new NodeCache({ stdTTL: 60, checkperiod: 120 });

module.exports = {
  get(key) {
    return cache.get(key);
  },
  set(key, value, ttl) {
    return cache.set(key, value, ttl || 60);
  },
  del(key) {
    return cache.del(key);
  },
  // 按前缀批量删除
  delByPrefix(prefix) {
    var keys = cache.keys();
    keys.forEach(function(k) { if (k.startsWith(prefix)) cache.del(k); });
  },
  // 清除所有缓存
  flush() {
    return cache.flushAll();
  },
};
