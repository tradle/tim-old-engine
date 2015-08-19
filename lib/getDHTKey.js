var utils = require('tradle-utils')
var Builder = require('chained-obj').Builder

module.exports = function getDHTKey (obj, cb) {
  new Builder()
    .data(obj)
    .build(function (err, result) {
      if (err) return cb(err)

      utils.getStorageKeyFor(result.form, function (err, key) {
        if (err) return cb(err)

        cb(null, key.toString('hex'))
      })
    })
}
