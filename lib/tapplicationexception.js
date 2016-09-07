class TApplicationException extends Error {
  static get SCHEMA() {
    let value = [
      { id: '1', name: 'message', type: 'string' },
      { id: '2', name: 'type', type: 'i32' }
    ];
    Object.defineProperty(this, 'SCHEMA', { configurable: true, value });
    return value;
  }
  static get TYPE_ENUM() {
    let value = {
      UNKNOWN: 0,
      UNKNOWN_METHOD: 1,
      INVALID_MESSAGE_TYPE: 2,
      WRONG_METHOD_NAME: 3,
      BAD_SEQUENCE_ID: 4,
      MISSING_RESULT: 5,
      INTERNAL_ERROR: 6,
      PROTOCOL_ERROR: 7
    };
    Object.defineProperty(this, 'TYPE_ENUM', { configurable: true, value });
    return value;
  }
  static get TYPE_ENUM_INV() {
    let value = {};
    let { TYPE_ENUM } = this;
    for (let key in TYPE_ENUM) value[TYPE_ENUM[key]] = key;
    Object.defineProperty(this, 'TYPE_ENUM_INV', { configurable: true, value });
    return value;
  }
  constructor(type, message) {
    super(message);
    this.name = TApplicationException.TYPE_ENUM_INV[type];
    this.status = 500;
  }
}

module.exports = TApplicationException;
