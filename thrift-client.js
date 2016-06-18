const Thrift = require('node-thrift-protocol');
const Storage = require('./lib/storage');
const ThriftSchema = require('./lib/thrift-schema');

class ThriftClient {
  constructor(options) {
    Object.keys(options).forEach(key => this[key] = options[key]);
    this.retryDefer = this.retryDefer || 1000;
    this.methods = [];
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
  get thrift() {
    let { host, port } = this;
    this.thrift = Thrift.connect({ host, port });
    return this.thrift;
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
        let params = this.schema.decodeStruct(api.args, { fields });
        this.trigger(name, params).then(result => {
          result = this.schema.encodeValueWithType(result, api.type);
          result.id = 0;
          let fields = [ result ];
          this.thrift.write({ id, type: 'REPLY', name, fields });
        }, error => {
          let fields;
          try {
            fields = this.schema.encodeStruct(api.throws, error).fields;
            if (!fields.length) throw error;
          } catch (error) {
            let { name, message } = error;
            fields = [ { id: 999, type: 'STRING', value: JSON.stringify({ name, message }) } ];
          }
          this.thrift.write({ id, type: 'REPLY', name, fields });
        });
        break;
      case 'EXCEPTION':
      case 'REPLY':
        let { resolve, reject } = this.storage.take(id);
        let field = fields[0];
        if (field.id) {
          let errorType = api.throws.find(item => +item.id === +field.id);
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
  register(name, ...handlers) {
    const chains = (ctx, index = 0) => {
      if (index >= handlers.length) return null;
      let handler = handlers[index];
      if (typeof handler !== 'function') return chains(ctx, index + 1);
      return handler(ctx, () => chains(ctx, index + 1));
    }
    this.methods[name] = chains;
  }
  trigger(name, ctx) {
    return Promise.resolve(ctx).then(this.methods[name]);
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
