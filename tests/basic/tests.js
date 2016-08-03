const ThriftClient = require('../../thrift-client');

let client = new ThriftClient(require('./config'));

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
