var utils = require('tradle-utils')
var Builder = require('chained-obj').Builder
var NONCE = require('tradle-constants').NONCE

module.exports = function getDHTKey (obj, cb) {
  if (!obj[NONCE]) throw new Error('missing nonce')

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
