
var utils = require('tradle-utils')
var Builder = require('chained-obj').Builder

module.exports = function getDHTKey (obj) {
  new Builder()
    .data(this.identityJSON)
    .build(function (err, buf) {
      if (err) return cb(err)

      utils.getStorageKeyFor(buf, function (err, key) {
        if (err) return cb(err)

        cb(null, key)
      })
    })
}
