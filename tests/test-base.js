const Thrift = require('node-thrift-protocol');
const assert = require('assert');
const ThriftClient = require('../thrift-client');

const host = '127.0.0.1';
const port = 8101;
const schema = `
  typedef map<string, map<string, i16>> T1
  typedef map<S1, string> T2
  exception E1 {
    1: required string name,
    2: required string message,
  }
  struct S1 {
    1: required i32 a,
  }
  struct Args {
    1: required list<S1> list1,
    2: required T1 map1,
    3: required T2 map2,
  }
  service Test {
    Args test(1: list<S1> list1, 2: T1 map1, 3: T2 map2) throws (1: E1 exception);
    binary bin(1: binary data);
  }
`;

ThriftClient.start({ port, schema }).register('test', ctx => {
  let [ item = {} ] = ctx.list1;
  if (item.a === 999) {
    let message = JSON.stringify(ctx);
    throw { 'exception': { name: 'ERROR_999', message } };
  }
  return ctx;
}).register('bin', ctx => {
  return ctx.data;
});

let client = new ThriftClient({ host, port, schema });

let tests = [
  () => {
    let data = {
      list1: [ { a: 1 }, { a: 2 } ],
      map1: { "a": { "one": 1 }, "b": { "two": 2 } },
      map2: { "{\"a\":1}": "one", "{\"a\":2}": "two" },
    };
    return client.call('test', data).then(result => {
      assert.deepEqual(data, result);
    });
  },
  () => {
    return client.call('test', {}).then(result => {
      throw result;
    }, error => {
      return error;
    });
  },
  () => {
    let data = { list1: [ { a: 999 } ], map1: {}, map2: {} };
    return client.call('test', data).then(result => {
      throw result;
    }, error => {
      assert.equal(JSON.stringify(data), error.data.message);
    });
  },
  () => {
    let data = new Buffer([ 1, 2, 3, 4, 5 ]);
    return client.call('bin', { data }).then(result => {
      assert.deepEqual(result, data);
    });
  }
];

setTimeout(() => { throw 'Timeout'; }, 1000);
Promise.all(tests.map(f => f())).then(() => {
  process.exit();
}, reason => {
  console.error(reason);
  process.exit(1);
});
