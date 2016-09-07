const thriftParser = require('thrift-parser');

class ThriftSchemaMismatchRequest extends Error {
  constructor(message) {
    super(message);
    this.status = 400;
    this.name = 'THRIFT_SCHEMA_MISMATCH_REQUEST';
  }
}

class ThriftSchemaMismatchResponse extends Error {
  constructor(message) {
    super(message);
    this.status = 500;
    this.name = 'THRIFT_SCHEMA_MISMATCHING_RESPONSE';
  }
}

class ThriftSchema {
  toJSON() { return this.plain; }
  constructor(schema) {
    if (schema instanceof Buffer) schema += '';
    let result = typeof schema === 'string' ? thriftParser(schema) : schema;
    this.plain = result;
    let { service = {} } = result;
    service = Object.keys(service).reduce((base, name) => Object.assign(base, service[name]), {});
    Object.assign(this, result, { service });
  }
  parseType(type) {
    let { typedef, struct, exception, enum: enumx } = this;
    while (typedef && type in typedef) type = typedef[type].type;
    if (enumx && type in enumx) type = 'I32';
    if ((struct && type in struct) || (exception && type in exception)) type = 'STRUCT';
    if (typeof type === 'string') type = type.toUpperCase();
    let thriftType = type;
    let plainType = String(type.name || type).toUpperCase();
    return { thriftType, plainType };
  }
  encodeStruct(schema, params) {
    let fields = schema.map(({ id, name, type, option }) => {
      if (name in params) {
        let field = this.encodeValueWithType(params[name], type);
        field.id = +id;
        return field;
      } else {
        if (option === 'required') {
          throw new ThriftSchemaMismatchRequest(`Required field "${name}" not found`);
        }
      }
    }).filter(Boolean);
    return { fields };
  }
  decodeStruct(schema, value) {
    let sMap = value.fields.reduce((base, item) => (base[item.id] = item, base), {});
    return schema.reduce((base, field) => {
      let { id, type, name, option } = field;
      let value = sMap[id];
      if (option === 'required' && value === void 0) {
        throw new ThriftSchemaMismatchResponse(`Required field "${name}" not found`);
      }
      if (value !== void 0) base[name] = this.decodeValueWithType(value, type);
      return base;
    }, {});
  }
  encodeValueWithType(value, type) {
    let { plainType, thriftType } = this.parseType(type);
    switch (plainType) {
      case 'VOID':
        return { type: plainType, value: null };
      case 'BOOL':
      case 'BYTE':
      case 'I16':
      case 'I32':
      case 'I64':
      case 'DOUBLE':
      case 'BINARY':
      case 'STRING':
        return { type: plainType, value };
      case 'STRUCT': {
        let { struct = {}, exception = {} } = this;
        value = this.encodeStruct(struct[type] || exception[type], value);
        return { type: plainType, value };
      }
      case 'LIST': {
        let { valueType } = thriftType;
        if (!(value instanceof Array)) throw new ThriftSchemaMismatchRequest(`"${value}" is not an Array`);
        let data = value.map(item => this.encodeValueWithType(item, valueType).value);
        valueType = this.parseType(valueType).plainType;
        return { type: 'LIST', value: { valueType, data } };
      }
      case 'MAP': {
        let { keyType, valueType } = thriftType;
        let data = Object.keys(value).map(k => {
          let v = value[k];
          let plainKeyType = this.parseType(keyType).plainType;
          if (plainKeyType === 'STRUCT' || keyType.name) k = JSON.parse(k);
          return {
            key: this.encodeValueWithType(k, keyType).value,
            value: this.encodeValueWithType(v, valueType).value
          };
        });
        keyType = this.parseType(keyType).plainType;
        valueType = this.parseType(valueType).plainType;
        return { type: 'MAP', value: { keyType, valueType, data } };
      } 
      default:
        throw new ThriftSchemaMismatchRequest(`Unknown field type "${type}"`);
    }
  }
  decodeValueWithType(field, type) {
    let { plainType, thriftType } = this.parseType(type);
    if ('type' in field) {
      let xPlainType = plainType === 'BINARY' ? 'STRING' : plainType;
      if (field.type !== xPlainType) {
        throw new ThriftSchemaMismatchResponse(`Responsed type ${field.type} is not match schema type ${plainType}`);
      }
    }
    switch (plainType) {
      case 'VOID':
        return null;
      case 'BOOL':
      case 'BYTE':
      case 'I16':
      case 'I32':
      case 'I64':
      case 'DOUBLE':
      case 'BINARY':
        return field.value;
      case 'STRING':
        return field.value + '';
      case 'STRUCT': {
        let { value } = field;
        let { struct = {}, exception = {} } = this;
        while (this.typedef && type in this.typedef) type = this.typedef[type].type;
        let schema = struct[type] || exception[type];
        if (!schema) throw new ThriftSchemaMismatchResponse(`Type "${type}" not found.`);
        return this.decodeStruct(schema, value);
      }
      case 'LIST':
        let { valueType } = thriftType;
        return field.value.data.map(item => this.decodeValueWithType({ value: item }, valueType));
      case 'MAP': {
        let { keyType, valueType } = thriftType;
        let receiver = {};
        field.value.data.forEach(({ key, value }) => {
          key = this.decodeValueWithType({ value: key }, keyType);
          value = this.decodeValueWithType({ value: value }, valueType);
          let plainKeyType = this.parseType(keyType).plainType;
          if (plainKeyType === 'STRUCT' || keyType.name) key = JSON.stringify(key);
          receiver[key] = value;
        });
        return receiver;
      }
      default:
        throw new ThriftSchemaMismatchResponse(`Unknown field type ${JSON.stringify(type)}`);
    }
  }
}

module.exports = ThriftSchema;
