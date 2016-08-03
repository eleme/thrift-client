const ThriftSchema = require('./thrift-schema');

const HeaderSchema = new ThriftSchema(`
  struct RequestHeader {
    1: string request_id;
    2: string seq;
    3: map<string, string> meta;
  }
`);

const { RequestHeader } = HeaderSchema.struct;

module.exports.encode = args => HeaderSchema.encodeStruct(RequestHeader, args);

module.exports.decode = args => HeaderSchema.decodeStruct(RequestHeader, args);
