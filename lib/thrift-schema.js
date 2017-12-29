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

const parseType = (that, rawType) => {
  let typeCache = that.typeCache;
  if (typeof rawType === 'string' && rawType in typeCache) return typeCache[rawType];
  let thriftType = rawType;
  if (typeof thriftType === 'string') {
    let typedef = that.typedef;
    if (typedef) while (thriftType in typedef) thriftType = typedef[thriftType].type;
    let enumx = that.enum;
    if (enumx && thriftType in enumx) thriftType = 'I32';
    let exception = that.exception;
    let struct = that.struct;
    if ((struct && thriftType in struct) || (exception && thriftType in exception)) thriftType = 'STRUCT';
    if (typeof thriftType === 'string') thriftType = thriftType.toUpperCase();
  }
  let plainType = String(thriftType.name || thriftType).toUpperCase();
  let result = { thriftType, plainType };
  if (typeof rawType === 'string') typeCache[rawType] = result;
  return result;
};

class ThriftSchema {
  toJSON() { return this.plain; }
  constructor(schema, options = {}) {
    if (schema instanceof Buffer) schema += '';
    let result = typeof schema === 'string' ? thriftParser(schema) : schema;
    Object.assign(this, options);
    this.plain = result;
    let service = result.service || {};
    service = Object.keys(service).reduce((base, name) => Object.assign(base, service[name]), {});
    Object.assign(this, result, { service });
    this.typeCache = {};
  }
  encodeStruct(schema, params = {}) {
    let fields = [];
    for (let i = 0; i < schema.length; i++) {
      let name = schema[i].name;
      let value = params[name];
      if (value === void 0) value = schema[i].defaultValue;
      if (value !== void 0) {
        let field = this.encodeValueWithType(value, schema[i].type);
        field.id = +schema[i].id;
        fields.push(field);
      } else {
        if (schema[i].option === 'required') {
          throw new ThriftSchemaMismatchRequest('Required field "' + name + '" not found');
        }
      }
    }
    return { fields };
  }
  decodeStruct(schema, value) {
    // Build value.fields to a map
    let sMap = {};
    let fields = value.fields;
    for (let i = 0; i < fields.length; i++) sMap[fields[i].id] = fields[i];
    let result = {};
    for (let i = 0; i < schema.length; i++) {
      let name = schema[i].name;
      let value = sMap[schema[i].id];
      if (value === void 0) { // If this value is no defined in schema
        if ('defaultValue' in schema[i]) { // Check defaultValue
          result[name] = schema[i].defaultValue;
        } else if (schema[i].option === 'required') { // Check required
          if (!this.ignoreResponseCheck)
            throw new ThriftSchemaMismatchResponse('Required field "' + name + '" not found');
        }
      } else { // Decode normally
        result[name] = this.decodeValueWithType(value, schema[i].type);
      }
    }
    return result;
  }
  encodeValueWithType(value, type) {
    let parsedType = parseType(this, type);
    let plainType = parsedType.plainType;
    let thriftType = parsedType.thriftType;
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
        let struct = this.struct || {};
        let typedef = this.typedef || {};
        let exception = this.exception || {};
        while (typedef && type in typedef) type = typedef[type].type;
        value = this.encodeStruct(struct[type] || exception[type], value);
        return { type: plainType, value };
      }
      case 'LIST': {
        let valueType = thriftType.valueType;
        if (!(value instanceof Array)) throw new ThriftSchemaMismatchRequest('"' + value + '" is not an Array');
        let data = [];
        for (let i = 0; i < value.length; i++) {
          data[i] = this.encodeValueWithType(value[i], valueType).value;
        }
        valueType = parseType(this, valueType).plainType;
        return { type: 'LIST', value: { valueType, data } };
      }
      case 'MAP': {
        let keyType = thriftType.keyType;
        let valueType = thriftType.valueType;
        let keys = Object.keys(value);
        let data = [];
        for (let i = 0; i < keys.length; i++) {
          let k = keys[i];
          let v = value[k];
          let plainKeyType = parseType(this, keyType).plainType;
          if (plainKeyType === 'STRUCT' || keyType.name) k = JSON.parse(k);
          data.push({
            key: this.encodeValueWithType(k, keyType).value,
            value: this.encodeValueWithType(v, valueType).value
          });
        }
        keyType = parseType(this, keyType).plainType;
        valueType = parseType(this, valueType).plainType;
        return { type: 'MAP', value: { keyType, valueType, data } };
      } 
      default:
        throw new ThriftSchemaMismatchRequest('Unknown field type "' + type + '"');
    }
  }
  decodeValueWithType(field, type) {
    let parsedType = parseType(this, type);
    let plainType = parsedType.plainType;
    let thriftType = parsedType.thriftType;
    if ('type' in field) {
      let xPlainType = plainType;
      if (xPlainType === 'BINARY') xPlainType = 'STRING';
      else if (xPlainType === 'BYTE') xPlainType = 'I08';
      if (!this.ignoreResponseCheck) {
        if (field.type !== xPlainType) {
          throw new ThriftSchemaMismatchResponse('Responsed type ' + field.type + ' is not match schema type ' + plainType);
        }
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
        let value = field.value;
        let struct = this.struct || {};
        let exception = this.exception || {};
        while (this.typedef && type in this.typedef) type = this.typedef[type].type;
        let schema = struct[type] || exception[type];
        if (!schema) throw new ThriftSchemaMismatchResponse('Type "' + type + '" not found.');
        return this.decodeStruct(schema, value);
      }
      case 'LIST': {
        let valueType = thriftType.valueType;
        let data = field.value.data;
        let result = [];
        for (let i = 0; i < data.length; i++) {
          result.push(this.decodeValueWithType({ value: data[i] }, valueType));
        }
        return result;
      }
      case 'MAP': {
        let keyType = thriftType.keyType;
        let valueType = thriftType.valueType;
        let receiver = {};
        let data = field.value.data;
        for (let i = 0; i < data.length; i++) {
          let key = data[i].key;
          let value = data[i].value;
          key = this.decodeValueWithType({ value: key }, keyType);
          value = this.decodeValueWithType({ value: value }, valueType);
          let plainKeyType = parseType(this, keyType).plainType;
          if (plainKeyType === 'STRUCT' || keyType.name) key = JSON.stringify(key);
          receiver[key] = value;
        }
        return receiver;
      }
      default:
        throw new ThriftSchemaMismatchResponse('Unknown field type ' + JSON.stringify(type));
    }
  }
}

module.exports = ThriftSchema;
