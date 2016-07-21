const Thrift = require('node-thrift-protocol');
const Storage = require('./lib/storage');
const ThriftSchema = require('./lib/thrift-schema');
const { EventEmitter } = require('events');

const METHODS = Symbol();
const STORAGE = Symbol();

class ThriftListener {
  constructor({ port, schema }) {
    Object.defineProperty(this, METHODS, { value: [] });
    Thrift.createServer(thrift => {
      let client = new ThriftClient({ thrift, schema });
      this[METHODS].forEach(args => client.register(...args));
    }).listen(port);
  }
  register(...args) {
    this[METHODS].push(args);
    return this;
  }
}


/**
 * Process a connection error
**/
function tcError(that, reason) {
  that[STORAGE].takeForEach(({ reject }) => reject(reason));
  if (that.retryDefer > 0) setTimeout(() => that.resetThrift(), that.retryDefer);
  that.emit('error', reason);
}


/**
 * Process a thrift frame
**/
function tcReceive(that, { id, type, name, fields }) {
  let api = that.schema.service[name];
  switch (type) {
    case 'CALL':
      let params = that.schema.decodeStruct(api.args, { fields });
      that.trigger(name, params).then(result => {
        result = that.schema.encodeValueWithType(result, api.type);
        result.id = 0;
        let fields = [ result ];
        that.thrift.write({ id, type: 'REPLY', name, fields });
      }, error => {
        let fields;
        try {
          fields = that.schema.encodeStruct(api.throws, error).fields;
          if (!fields.length) throw error;
        } catch (error) {
          let { name, message } = error;
          fields = [ { id: 999, type: 'STRING', value: JSON.stringify({ name, message }) } ];
        }
        that.thrift.write({ id, type: 'REPLY', name, fields });
      });
      break;
    case 'EXCEPTION':
    case 'REPLY':
      let { resolve, reject } = that[STORAGE].take(id);
      let field = fields[0];
      if (field.id) {
        let errorType = api.throws.find(item => +item.id === +field.id);
        if (errorType) {
          let type = errorType.name;
          let data = that.schema.decodeValueWithType(field, errorType.type);
          reject({ name: 'THRIFT_EXCEPTION', type, data });
        } else {
          reject({ name: 'THRIFT_ERROR', field });
        }
      } else {
        try {
          resolve(that.schema.decodeValueWithType(field, api.type));
        } catch (reason) {
          reject(reason);
        }
      }
      break;
    default:
      throw Error('No Implement');
  }
}

class ThriftClient extends EventEmitter {
  static start({ port, schema }) {
    return new ThriftListener({ port, schema });
  }
  constructor(options) {
    super();
    Object.defineProperty(this, METHODS, { value: {} });
    Object.defineProperty(this, STORAGE, { value: new Storage() });
    Object.keys(options).forEach(key => this[key] = options[key]);
    if (!('retryDefer' in this)) this.retryDefer = 1000;
  }
  set schema(data) {
    let schema = new ThriftSchema(data);
    let desc = Object.getOwnPropertyDescriptor(ThriftClient.prototype, 'schema');
    desc.get = () => schema;
    Object.defineProperty(this, 'schema', desc);
  }
  set thrift(thrift) {
    thrift.on('error', reason => tcError(this, reason));
    thrift.on('end', () => this.emit('end'));
    thrift.on('data', message => tcReceive(this, message));
    let desc = Object.getOwnPropertyDescriptor(ThriftClient.prototype, 'thrift');
    desc.get = () => thrift;
    Object.defineProperty(this, 'thrift', desc);
  }
  get thrift() { return this.resetThrift(); }
  resetThrift() {
    let { host = '127.0.0.1', port = 3000 } = this;
    return this.thrift = Thrift.connect({ host, port });
  }
  call(name, params = {}) {
    let api = this.schema.service[name];
    return new Promise((resolve, reject) => {
      if (!api) return reject(new Error(`API ${JSON.stringify(name)} not found`));
      let { fields } = this.schema.encodeStruct(api.args, params);
      let id = this[STORAGE].push({ resolve, reject });
      this.thrift.write({ id, name, type: 'CALL', fields });
    });
  }
  register(name, ...handlers) {
    const chains = (ctx, index = 0) => {
      if (index >= handlers.length) return null;
      let handler = handlers[index];
      if (typeof handler !== 'function') return chains(ctx, index + 1);
      return handler(ctx, () => chains(ctx, index + 1));
    }
    this[METHODS][name] = chains;
    return this;
  }
  trigger(name, ctx) {
    return Promise.resolve(ctx).then(this[METHODS][name]);
  }
}

module.exports = ThriftClient;
