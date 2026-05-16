const https = require('https');
const querystring = require('querystring');

/**
 * 互亿无线短信发送
 * @param {string} phone - 目标手机号
 * @param {string} code  - 6位纯数字验证码
 * @returns {Promise<{success: boolean, error?: string}>}
 */
function sendSms(phone, code) {
  return new Promise(function(resolve) {
    var settled = false;
    function done(result) {
      if (settled) return;
      settled = true;
      resolve(result);
    }

    var smsUrl = process.env.SMS_API_URL || 'http://106.ihiyi.com/webservice/sms.php';
    var urlObj = new URL(smsUrl);

    var postData = querystring.stringify({
      account: process.env.SMS_API_ID,
      password: process.env.SMS_API_KEY,
      mobile: phone,
      templateid: process.env.SMS_TEMPLATE_ID,
      content: code,
      format: 'json',
    });

    var options = {
      hostname: urlObj.hostname,
      path: urlObj.pathname + '?method=Submit',
      method: 'POST',
      headers: {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Content-Length': Buffer.byteLength(postData),
      },
    };

    var req = https.request(options, function(res) {
      var data = '';
      res.on('data', function(chunk) { data += chunk; });
      res.on('end', function() {
        try {
          var result = JSON.parse(data);
          if (result.code === 2) {
            done({ success: true });
          } else {
            done({ success: false, error: result.msg || '短信发送失败' });
          }
        } catch (_) {
          done({ success: false, error: '短信服务响应异常' });
        }
      });
    });

    req.setTimeout(10000, function() {
      req.destroy();
      done({ success: false, error: '短信服务连接超时' });
    });

    req.on('error', function(err) {
      console.error('[SMS] 发送失败:', err.message || err);
      done({ success: false, error: '短信服务连接失败' });
    });

    req.write(postData);
    req.end();
  });
}

module.exports = { sendSms };
