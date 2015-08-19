
var rebuf = require('logbase').rebuf

module.exports = {
  encode: function (entry) {
    return JSON.stringify(entry)
  },
  decode: function (entry) {
    return rebuf(JSON.parse(entry))
  },
  buffer: false,
  type: 'jsonWithBufs'
}
