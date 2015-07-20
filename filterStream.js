
var map = require('map-stream')

module.exports = function filterStream (filter) {
  return map(function (data, cb) {
    if (filter(data)) {
      return cb(null, data)
    } else {
      return cb()
    }
  })
}
