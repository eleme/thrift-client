const ThriftClient = require('../../thrift-client');

let server = ThriftClient.start(require('./config'));

server.register('test', ctx => {
  let [ item = {} ] = ctx.list1;
  if (item.a === 999) {
    let message = JSON.stringify(ctx);
    throw { 'exception': { name: 'ERROR_999', message } };
  }
  return ctx;
});

server.register('bin', ctx => {
  return ctx.data;
});
