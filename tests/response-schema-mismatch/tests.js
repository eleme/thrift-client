const fs = require('fs');
const ThriftClient = require('../../thrift-client');
const { port } = require('./mock-server');
const schema = fs.readFileSync(__dirname + '/test.thrift');

let client = new ThriftClient({ host: '127.0.0.1', port, schema });

let tests = module.exports = [];

setTimeout(() => { throw 'Timeout'; }, 1000);
setTimeout(() => {
  Promise.all(tests.map(f => f(client))).then(() => {
    process.exit();
  }, reason => {
    console.error(reason); // eslint-disable-line
    process.exit(1);
  });
});
