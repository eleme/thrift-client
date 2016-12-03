class Storage extends Map {
  constructor(...args) {
    super(...args);
    this.inc = 0;
  }
  noop() { return ++this.inc; }
  push(data) {
    let id = this.inc++;
    this.set(id, data);
    return id;
  }
  take(id) {
    let data = this.get(id);
    this.delete(id);
    return data;
  }
  takeForEach(callback) {
    this.forEach((value, key) => {
      this.delete(key);
      callback(value);
    });
  }
}

module.exports = Storage;
