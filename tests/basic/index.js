require('./mock-server');
const tests = require('./tests');
const assert = require('assert');

tests.push(client => {
  let data = {
    list1: [ { a: 1 }, { a: 2 } ],
    map1: { 'a': { 'one': 1 }, 'b': { 'two': 2 } },
    map2: { '{"a":1}': 'one', '{"a":2}': 'two' }
  };
  return client.call('test', data).then(result => {
    assert.deepEqual(data, result);
  });
});

tests.push(client => {
  return client.call('test', {}).then(result => {
    throw result;
  }, error => {
    return error;
  });
});

tests.push(client => {
  let data = { list1: [ { a: 999 } ], map1: {}, map2: {} };
  return client.call('test', data).then(result => {
    throw result;
  }, error => {
    assert.equal(JSON.stringify(data), error.data.message);
  });
});

tests.push(client => {
  let data = new Buffer([ 1, 2, 3, 4, 5 ]);
  return client.call('bin', { data }).then(result => {
    assert.deepEqual(result, data);
  });
});

tests.push(client => {
  let data = new Buffer([ 1, 2, 3, 4, 5 ]);
  return client.call('unknown', { data }).then(result => {
    throw result;
  }, error => {
    assert.equal(error.type, 1);
  });
});
