const Thrift = require('node-thrift-protocol');
const assert = require('assert');
const ThriftClient = require('../thrift-client');


Thrift.createServer(thrift => {
  let sx = new ThriftClient({ thrift, schema });
  sx.register('test', (ctx) => {
    return ctx;
  });
}).listen(8101);


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
    Args test(1: list<S1> list1, 2: T1 map1, 3: T2 map2)
      throws (1: E1 exception),
  }
`;

let client = new ThriftClient({ host, port, schema });

let data = {
  list1: [ { a: 1 }, { a: 2 } ],
  map1: { "a": { "one": 1 }, "b": { "two": 2 } },
  map2: { "{\"a\":1}": "one", "{\"a\":2}": "two" },
};

let test1 = client.call('test', data).then(result => {
  assert.deepEqual(data, result);
});

let test2 = client.call('test', {}).then(result => {
  throw result;
}, reason => {
  return reason;
});

Promise.all([ test1, test2 ]).then(() => {
  process.exit();
}, reason => {
  console.error(reason);
  process.exit(1);
});
