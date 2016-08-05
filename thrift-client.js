const Thrift = require('node-thrift-protocol');
const Storage = require('./lib/storage');
const ThriftSchema = require('./lib/thrift-schema');
const { EventEmitter } = require('events');
const Header = require('./lib/header');

const METHODS = Symbol();
const STORAGE = Symbol();

const TApplicationException = [
  { id: '1', name: 'message', type: 'string' },
  { id: '2', name: 'type', type: 'i32' }
];
const UNKNOWN = 0;
const UNKNOWN_METHOD = 1;
const INVALID_MESSAGE_TYPE = 2;
const WRONG_METHOD_NAME = 3;
const BAD_SEQUENCE_ID = 4;
const MISSING_RESULT = 5;
const INTERNAL_ERROR = 6;
const PROTOCOL_ERROR = 7;

class ThriftListener {
  constructor({ server, port, schema }) {
    Object.defineProperty(this, METHODS, { value: [] });
    if (server) {
      server.on('connection', socket => {
        let thrift = new Thrift(socket);
        let client = new ThriftClient({ thrift, schema });
        this[METHODS].forEach(args => client.register(...args));
      });
    } else {
      Thrift.createServer(thrift => {
        let client = new ThriftClient({ thrift, schema });
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
let tcError = (that, reason) => {
  that[STORAGE].takeForEach(({ reject }) => reject(reason));
  if (that.retryDefer > 0) setTimeout(() => that.reset(), that.retryDefer);
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
            that.thrift.write({ id, type: 'REPLY', name, fields });
          } catch (error) {
            let { fields } = that.schema.encodeStruct(TApplicationException, {
              message: error.stack || error.message || error.name,
              type: INTERNAL_ERROR 
            });
            that.thrift.write({ id, type: 'EXCEPTION', name, fields });
          }
        });
      } else {
        let { fields } = that.schema.encodeStruct(TApplicationException, {
          message: `method '${name}' is not found`,
          type: UNKNOWN_METHOD
        });
        that.thrift.write({ id, type: 'EXCEPTION', name, fields });
      }
      break;
    case 'EXCEPTION': {
      let { resolve, reject } = that[STORAGE].take(id);
      let params = that.schema.decodeStruct(TApplicationException, { fields });
      reject(params);
      break;
    }
    case 'REPLY': {
      let { resolve, reject } = that[STORAGE].take(id);
      if (fields.length === 0) return resolve(null); // return a void
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
    // Don't use Object.assign, because setter properties may be overwrite
    Object.keys(options).forEach(key => this[key] = options[key]);
    if (!('retryDefer' in this)) this.retryDefer = 1000;
    this.reset(this.thrift);
  }
  set schema(data) {
    let schema = new ThriftSchema(data);
    let desc = Object.getOwnPropertyDescriptor(ThriftClient.prototype, 'schema');
    desc.get = () => schema;
    Object.defineProperty(this, 'schema', desc);
  }
  reset(thrift) {
    let { host = '127.0.0.1', port = 3000 } = this;
    if (!thrift) thrift = Thrift.connect({ host, port });
    thrift.on('error', reason => tcError(this, reason));
    thrift.on('end', () => this.emit('end'));
    thrift.on('data', message => tcReceive(this, message));
    this.thrift = thrift;
  }
  call(name, params = {}, header) {
    let api = this.schema.service[name];
    return new Promise((resolve, reject) => {
      if (!api) return reject(new Error(`API ${JSON.stringify(name)} not found`));
      let { fields } = this.schema.encodeStruct(api.args, params);
      let id = this[STORAGE].push({ resolve, reject });
      if (header) header = Header.encode(header);
      this.thrift.write({ id, name, type: 'CALL', fields, header });
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
  hasRegistered(name) { return name in this[METHODS]; }
  trigger(name, ctx) {
    return Promise.resolve(ctx).then(this[METHODS][name]);
  }
}

module.exports = ThriftClient;
