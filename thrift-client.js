const Thrift = require('node-thrift-protocol');
const Storage = require('./lib/storage');
const ThriftSchema = require('./lib/thrift-schema');

class ThriftClient {
  constructor(options) {
    Object.keys(options).forEach(key => this[key] = options[key]);
    this.retryDefer = this.retryDefer || 1000;
    let { host, port } = this;
    this.thrift = Thrift.connect({ host, port });
  }
  get storage() {
    let value = new Storage();
    Object.defineProperty(this, 'storage', { value });
    return value;
  }
  set schema(data) {
    let schema = new ThriftSchema(data);
    let desc = Object.getOwnPropertyDescriptor(ThriftClient.prototype, 'schema');
    desc.get = () => schema;
    Object.defineProperty(this, 'schema', desc);
  }
  set thrift(thrift) {
    thrift.on('error', reason => this.error(reason));
    thrift.on('data', message => this.receive(message));
    let desc = Object.getOwnPropertyDescriptor(ThriftClient.prototype, 'thrift');
    desc.get = () => thrift;
    Object.defineProperty(this, 'thrift', desc);
  }
  call(name, params = {}) {
    let api = this.schema.service[name];
    return new Promise((resolve, reject) => {
      if (!api) return reject(new Error(`API ${JSON.stringify(name)} not found`));
      let { fields } = this.schema.encodeStruct(api.args, params);
      let id = this.storage.push({ resolve, reject });
      this.thrift.write({ id, name, type: 'CALL', fields });
    });
  }
  receive(message) {
    let { id, type, name, fields } = message;
    let api = this.schema.service[name];
    switch (type) {
      case 'CALL':
        break;
      case 'EXCEPTION':
      case 'REPLY':
        let { resolve, reject } = this.storage.take(id);
        let field = fields[0];
        if (field.id) {
          let errorType = (api.throws || []).find(item => item.id == field.id);
          if (errorType) {
            let type = errorType.name;
            let data = this.schema.decodeValueWithType(field, errorType.type);
            reject({ name: 'THRIFT_EXCEPTION', type, data });
          } else {
            reject({ name: 'THRIFT_ERROR', field });
          }
        } else {
          try {
            resolve(this.schema.decodeValueWithType(field, api.type));
          } catch (reason) {
            reject(reason);
          }
        }
        break;
      default:
        throw Error('No Implement');
    }
  }
  error(reason) {
    this.storage.takeForEach(({ reject }) => reject(reason));
    if (this.retryDefer) setTimeout(() => {
      let { host, port } = this;
      this.thrift = Thrift.connect({ host, port });
    }, this.retryDefer);
  }
}

module.exports = ThriftClient;
