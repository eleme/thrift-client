const Thrift = require('node-thrift-protocol');
const Storage = require('./lib/storage');
const ThriftSchema = require('./lib/thrift-schema');
const { EventEmitter } = require('events');
const Header = require('./lib/header');
const TApplicationException = require('./lib/tapplicationexception');

const METHODS = Symbol();
const STORAGE = Symbol();

class SocketClosedByBackEnd extends Error {
  constructor() {
    super('socket closed by backend');
    this.name = 'SOCKET_CLOSED_BY_BACKEND';
    this.status = 503;
  }
}

class ThriftListener extends EventEmitter {
  constructor({ server, port, schema }) {
    super();
    Object.defineProperty(this, METHODS, { value: [] });
    if (server) {
      server.on('connection', socket => {
        let thrift = new Thrift(socket);
        let client = new ThriftClient({ thrift, schema });
        client.on('error', () => thrift.end());
        this[METHODS].forEach(args => client.register(...args));
      });
    } else {
      Thrift.createServer(thrift => {
        let client = new ThriftClient({ thrift, schema });
        client.on('error', () => thrift.end());
        this[METHODS].forEach(args => client.register(...args));
      }).listen(port);
    }
  }
  register(...args) {
    this[METHODS].push(args);
    return this;
  }
}


/**
 * Process a connection error
**/
const tcError = (that, reason) => {
  that[STORAGE].takeForEach(({ reject }) => reject(reason));
  if (that.retryDefer > 0) setTimeout(() => that.reset(), that.retryDefer);
  that.thrift.removeAllListeners();
  that.emit('error', reason);
};

/**
 * Process a thrift frame
**/
let tcReceive = (that, { id, type, name, fields }) => {
  let api = that.schema.service[name];
  switch (type) {
    case 'CALL':
      if (that.hasRegistered(name)) {
        new Promise((resolve, reject) => {
          try {
            let params = that.schema.decodeStruct(api.args, { fields });
            resolve(that.trigger(name, params));
          } catch (error) {
            reject(error);
          }
        }).then(result => {
          result = that.schema.encodeValueWithType(result, api.type);
          result.id = 0;
          let fields = [ result ];
          that.thrift.write({ id, type: 'REPLY', name, fields });
        }).catch(error => {
          let fields;
          try {
            fields = that.schema.encodeStruct(api.throws, error).fields;
            if (!fields.length) throw error;
            that.thrift.write({ id, type: 'REPLY', name, fields });
          } catch (error) {
            let fields = that.schema.encodeStruct(TApplicationException.SCHEMA, {
              message: error.stack || error.message || error.name,
              type: TApplicationException.TYPE_ENUM.INTERNAL_ERROR 
            }).fields;
            that.thrift.write({ id, type: 'EXCEPTION', name, fields });
          }
        });
      } else {
        let fields = that.schema.encodeStruct(TApplicationException.SCHEMA, {
          message: `method '${name}' is not found`,
          type: TApplicationException.TYPE_ENUM.UNKNOWN_METHOD
        }).fields;
        that.thrift.write({ id, type: 'EXCEPTION', name, fields });
      }
      break;
    case 'ONEWAY':
      if (that.hasRegistered(name)) {
        try {
          let params = that.schema.decodeStruct(api.args, { fields });
          that.trigger(name, params);
        } catch (error) {
          /* ignore oneway error */
        }
      }
      break;
    case 'EXCEPTION': {
      let item = that[STORAGE].take(id);
      let params = that.schema.decodeStruct(TApplicationException.SCHEMA, { fields });
      item.reject(new TApplicationException(params.type, params.message));
      break;
    }
    case 'REPLY': {
      let item = that[STORAGE].take(id);
      let resolve = item.resolve;
      let reject = item.reject;
      if (fields.length === 0) fields = [ { id: 0, type: 'VOID' } ];
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
    }
    default:
      throw Error('No Implement');
  }
};

class ThriftClient extends EventEmitter {
  static start(args) {
    return new ThriftListener(args);
  }
  constructor(options) {
    super();
    Object.defineProperty(this, METHODS, { value: {} });
    Object.defineProperty(this, STORAGE, { value: new Storage() });
    this.ignoreResponseCheck = !!options.ignoreResponseCheck;
    Object.assign(this, options);
    if (!('retryDefer' in this) && !this.thrift) this.retryDefer = 1000;
    this.reset(this.thrift);
  }
  set schema(data) {
    let { ignoreResponseCheck } = this;
    let schema = new ThriftSchema(data, { ignoreResponseCheck });
    let desc = Object.getOwnPropertyDescriptor(ThriftClient.prototype, 'schema');
    desc.get = () => schema;
    Object.defineProperty(this, 'schema', desc);
  }
  reset(thrift) {
    let host = this.host || '127.0.0.1';
    let port = this.port || 3000;
    if (!thrift) thrift = Thrift.connect({ host, port });
    thrift.on('error', reason => tcError(this, reason));
    thrift.on('end', () => tcError(this, new SocketClosedByBackEnd()));
    thrift.on('data', message => tcReceive(this, message));
    this.thrift = thrift;
  }
  call(name, params = {}, header) {
    let api = this.schema.service[name];
    return new Promise((resolve, reject) => {
      if (!api) return reject(new Error(`API ${JSON.stringify(name)} not found`));
      let fields = this.schema.encodeStruct(api.args, params).fields;
      let id = this[STORAGE].push({ resolve, reject });
      if (header) header = Header.encode(header);
      this.thrift.write({ id, name, type: 'CALL', fields, header });
    });
  }
  oneway(name, params = {}, header) {
    let api = this.schema.service[name];
    return new Promise((resolve, reject) => {
      if (!api) return reject(new Error(`API ${JSON.stringify(name)} not found`));
      let fields = this.schema.encodeStruct(api.args, params).fields;
      let id = this[STORAGE].noop();
      if (header) header = Header.encode(header);
      this.thrift.write({ id, name, type: 'ONEWAY', fields, header });
      resolve();
    });
  }
  register(name, ...handlers) {
    const chains = (ctx, index = 0) => {
      if (index >= handlers.length) return null;
      let handler = handlers[index];
      if (typeof handler !== 'function') return chains(ctx, index + 1);
      return handler.call(this, ctx, () => chains(ctx, index + 1));
    };
    this[METHODS][name] = chains;
    return this;
  }
  end() { return this.thrift.end(); }
  hasRegistered(name) { return name in this[METHODS]; }
  trigger(name, ctx) {
    return Promise.resolve(ctx).then(this[METHODS][name]);
  }
}

module.exports = ThriftClient;
