const Thrift = require('node-thrift-protocol');
const thriftParser = require('thrift-parser');

class ThriftClient {
  constructor(options) {
    this.options = Object.assign({}, options);
  }
  get queue() {
    let value = [];
    Object.defineProperty(this, 'queue', { value });
    return value;
  }
  get service() {
    let value = Object.keys(this.schema.service).map(name => this.schema.service[name]);
    value = Object.assign({}, ...value);
    Object.defineProperty(this, 'service', { value });
    return value;
  }
  get schema() {
    let value = thriftParser(this.options.schema);
    let { service } = value;
    if (!service) throw new Error('Service not found in schema');
    Object.defineProperty(this, 'schema', { value });
    return value;
  }
  get thrift() {
    let value = Thrift.connect(this.options);
    value.on('data', message => this.receive(message));
    Object.defineProperty(this, 'thrift', { value });
    return value;
  }
  call(name, params = {}) {
    let api = this.service[name];
    return new Promise((resolve, reject) => {
      if (!api) return reject(new Error(`API ${JSON.stringify(name)} not found`));
      let { fields } = this.encodeStruct(api.args, params);
      let type = 'CALL';
      let id = this.queue.push({ resolve, reject }) - 1;
      this.thrift.write({ id, name, type, fields });
    });
  }
  receive(message) {
    let { id, type, name, fields } = message;
    let api = this.service[name];
    switch (type) {
      case 'EXCEPTION':
      case 'REPLY':
        let { resolve, reject } = this.queue[id];
        delete this.queue[id];
        let field = fields[0];
        if (field.id) {
          let errorType = (api.throws || []).find(item => item.id == field.id);
          if (errorType) {
            reject({ type: errorType.name, data: this.decodeValueWithType(field, errorType.type) });
          } else {
            reject({ type: 'UNKNOWN_ERROR', field });
          }
        } else {
          try {
            resolve(this.decodeValueWithType(field, api.type));
          } catch (reason) {
            reject(reason);
          }
        }
        break;
      default:
        throw Error('No Implement');
    }
  }
  getThriftType(type) {
    let { typedef, struct, exception } = this.schema;
    let enumx = this.schema.enum;
    while (typedef && type in typedef) type = typedef[type].type;
    if (enumx && type in enumx) type = 'I32';
    if ((struct && type in struct) || (exception && type in exception)) type = 'STRUCT';
    if (typeof type === 'string') type = type.toUpperCase();
    return type;
  }
  getPlainType(type) {
    type = this.getThriftType(type);
    return String(type.name || type).toUpperCase();
  }
  encodeStruct(schema, params) {
    let fields = schema.map(({ id, name, type }) => {
      if (!(name in params)) return void 0;
      let field = this.encodeValueWithType(params[name], type);
      field.id = +id;
      return field;
    }).filter(Boolean);
    return { fields };
  }
  encodeValueWithType(value, type) {
    let plainType = this.getPlainType(type);
    switch (plainType) {
      case 'BOOL':
      case 'BYTE':
      case 'I16':
      case 'I32':
      case 'I64':
      case 'DOUBLE':
      case 'STRING':
        return { type: plainType, value };
      case 'STRUCT': {
        let { struct = {}, exception = {} } = this.schema;
        value = this.encodeStruct(struct[type] || exception[type], value);
        return { type: plainType, value };
      }
      case 'LIST': {
        let { valueType } = this.getThriftType(type);
        let data = value.map(item => this.encodeValueWithType(item, valueType).value);
        valueType = this.getPlainType(valueType);
        return { type: 'LIST', value: { valueType, data } };
      }
      case 'MAP': {
        let { keyType, valueType } = this.getThriftType(type);
        let data = Object.keys(value).map(k => {
          let v = value[k];
          let plainKeyType = this.getPlainType(keyType);
          if (plainKeyType === 'STRUCT' || keyType.name) k = JSON.parse(k);
          return {
            key: this.encodeValueWithType(k, keyType).value,
            value: this.encodeValueWithType(v, valueType).value
          };
        });
        keyType = this.getPlainType(keyType);
        valueType = this.getPlainType(valueType);
        return { type: 'MAP', value: { keyType, valueType, data } };
      } 
      default:
        throw new Error(`Error Type "${type}"`);
    }
  }
  decodeValueWithType(field, type) {
    let plainType = this.getPlainType(type);
    switch (plainType) {
      case 'BOOL':
      case 'BYTE':
      case 'I16':
      case 'I32':
      case 'I64':
      case 'DOUBLE':
      case 'STRING':
        return field.value;
      case 'STRUCT': {
        let sMap = {};
        field.value.fields.forEach(item => sMap[item.id] = item);
        let receiver = {};
        let { struct = {}, exception = {} } = this.schema;
        (struct[type] || exception[type]).forEach(field => {
          let { id, type, name, option } = field;
          let value = sMap[id];
          if (option === 'required' && value === void 0) throw new Error(`Required field "${name}" not found`);
          if (value !== void 0) receiver[name] = this.decodeValueWithType(value, type);
        });
        return receiver;
      }
      case 'LIST':
        let { valueType } = this.getThriftType(type);
        return field.value.data.map(item => this.decodeValueWithType({ value: item }, valueType));
      case 'MAP': {
        let { keyType, valueType } = this.getThriftType(type);
        let receiver = {};
        field.value.data.forEach(({ key, value }) => {
          key = this.decodeValueWithType({ value: key }, keyType);
          value = this.decodeValueWithType({ value: value }, valueType);
          let plainKeyType = this.getPlainType(keyType);
          if (plainKeyType === 'STRUCT' || keyType.name) key = JSON.stringify(key);
          receiver[key] = value;
        });
        return receiver;
      }
      default:
        throw new Error(`Error Type ${JSON.stringify(type)}`);
    }
  }
}

module.exports = ThriftClient;
