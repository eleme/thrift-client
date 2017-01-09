require('./mock-server');
const tests = require('./tests');
const assert = require('assert');
const path = require('path');
const BigNumber = require('bignumber.js');

const rel = './' + path.relative('', process.argv[1]);
const done = name => console.log(`[32;1m[Done][0m [0;1m${rel} ${name}[0m`); // eslint-disable-line

tests.push(client => {
  let data = {
    list1: [ { a: 1 }, { a: 2 } ],
    map1: { 'a': { 'one': 1 }, 'b': { 'two': 2 } },
    map2: { '{"a":1}': 'one', '{"a":2}': 'two' }
  };
  return client.call('test', data).then(result => {
    assert.deepEqual(data, result);
    done('object as a key');
  });
});

tests.push(client => {
  let hehe = Math.random();
  return client.oneway('oneway_set_hehe', { hehe }).then(() => {
    return new Promise(resolve => setTimeout(resolve, 100));
  }).then(() => client.call('get_hehe')).then(result => {
    done('oneway call and success checked');
    assert.equal(result, hehe);
  });
});

tests.push(client => {
  return client.call('test', {}).then(result => {
    throw result;
  }, error => {
    done('throw an error');
    return error;
  });
});

tests.push(client => {
  let data = { list1: [ { a: 999 } ] };
  return client.call('test', data).then(result => {
    throw result;
  }, error => {
    assert.equal(JSON.stringify(data), error.data.message);
    done('object in list');
  });
});

tests.push(client => {
  let data = new Buffer([ 1, 2, 3, 4, 5 ]);
  return client.call('bin', { data }).then(result => {
    assert.deepEqual(result, data);
    done('binary data');
  });
});

tests.push(client => {
  let data = new Buffer([ 1, 2, 3, 4, 5 ]);
  return client.call('unknown', { data }).then(result => {
    throw result;
  }, error => {
    assert.equal(error.name, 'UNKNOWN_METHOD');
    done('exception on unknown method');
  });
});

tests.push(client => {
  let str = '-1234567890123456789';
  return client.call('bignumber', { data: new BigNumber(str) }).then(result => {
    assert.equal(result + '', str);
    done('i64 within BigNumber');
  });
});

tests.push(client => {
  let { host, port } = client;
  return new Promise((resolve, reject) => {
    require('http').get(`http://${host}:${port}`, () => {
      reject('must throw');
    }).on('error', error => {
      assert.equal(error.code, 'ECONNRESET');
      done('close on protocol error');
      resolve();
    });
  });
});

tests.push(client => {
  return client.call('required_a').then(() => {
    throw new Error('must throw');
  }, error => {
    assert.equal(error.name, 'THRIFT_SCHEMA_MISMATCH_REQUEST');
    done('pass undefined to required field');
  });
});

tests.push(client => {
  return client.call('arr', { arr: 1 }).then(() => {
    throw new Error('must throw');
  }, error => {
    assert.equal(error.name, 'THRIFT_SCHEMA_MISMATCH_REQUEST');
    done('pass non-list to a list');
  });
});

tests.push(client => {
  return client.call('response_a').then(() => {
    throw new Error('must throw');
  }, error => {
    assert.equal(error.name, 'INTERNAL_ERROR');
    done('internal error');
  });
});

tests.push(client => {
  return client.call('def_req_arg', { i: 234 }).then(result => {
    assert.equal(result[234], 'hehe');
    done('default request arguments');
  });
});

tests.push(client => {
  return client.call('def_res_arg').then(result => {
    assert.equal(result.i, 234);
    assert.equal(result.s, 'hehe');
    done('default response arguments');
  });
});

tests.push(client => {
  return client.call('zero', { zero: 0 }).then(result => {
    assert.equal(result.zero, 0);
    done('zero argument');
  });
});

tests.push(client => {
  return client.call('i64', { data: new BigNumber('1234567890123456789') }).then(result => {
    assert.deepEqual(result.data, new BigNumber('1234567890123456789'));
    done('i64 response self');
  });
});
