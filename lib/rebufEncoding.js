
var rebuf = require('logbase').rebuf

module.exports = {
  encode: function (entry) {
    return JSON.stringify(entry)
  },
  decode: function (entry) {
    switch (entry[0]) {
      case '{':
      case '[':
        return rebuf(JSON.parse(entry))
      default:
        return entry
    }
  },
  buffer: false,
  type: 'jsonWithBufs'
}
