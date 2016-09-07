const tests = require('./tests');
const assert = require('assert');
const path = require('path');

const rel = './' + path.relative('', process.argv[1]);
const done = name => console.log(`[32;1m[Done][0m [0;1m${rel} ${name}[0m`);

tests.push(client => {
  return client.call('a').then(result => {
    throw new Error('must throw');
  }, error => {
    assert.equal(error.name, 'THRIFT_SCHEMA_MISMATCHING_RESPONSE');
    done('void vs struct');
  });
});

tests.push(client => {
  return client.call('b').then(result => {
    throw new Error('must throw');
  }, error => {
    assert.equal(error.name, 'THRIFT_SCHEMA_MISMATCHING_RESPONSE');
    done('i32 vs list');
  });
});
