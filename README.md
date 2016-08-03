## thrift-client

A nodejs thrift client.

### Demo

###### Client

```js
let ThriftClient = require('thrift-client');

const host = '127.0.0.1'; // Server Host
const port = 3000; // Service Port
const schema = `
  service MyService {
    bool ping()
  }
`; // Service Thrift File Contents

let client = new ThriftClient({ host, port, schema });

client.call('ping').then(result => {
  console.log(result); // true or false
});
```

###### Server

```js
const port = 3000;

const schema = `
  service MyService {
    bool ping()
  }
`;

let ThriftClient = require('thrift-client');

let server = ThriftClient.start({ port, schema });

server.register('ping', () => {
  return true;
});
```
