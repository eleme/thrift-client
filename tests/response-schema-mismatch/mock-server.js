const Thrift = require('node-thrift-protocol');

let server = Thrift.createServer(client => {
  client.on('data', message => {
    let { type, name, id, fields } = message;
    switch (name) {
      case 'a':     
        client.write({ id, type: 'REPLY', name, fields: [] });
        break;
      case 'b':
        client.write({ id, type: 'REPLY', name, fields: [
          { id: 0, type: 'i32', value: 1 }
        ] });
        break;
    }
  });
});

server.listen();

exports.port = server.address().port;
