const fs = require('fs');
exports.host = '127.0.0.1';
exports.port = 8101;
exports.schema = fs.readFileSync(__dirname + '/test.thrift');
