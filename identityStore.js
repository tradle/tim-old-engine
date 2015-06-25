
var util = require('util')
var TypeStore = require('./typestore')

module.exports = function identityStore (options) {
  var typeStore = new TypeStore(options)
  var byFingerprint = typeStore.createSublevel('byFingerprint')

  typeStore.use({
    update: function (obj, cb, next) {
      this.byRootHash(obj._r, function (err, stored) {
        var fingerprintBatch = obj.pubkeys.map(function (k) {
          return { type: 'put', key: k.fingerprint, value: obj._r }
        })

        byFingerprint.batch(fingerprintBatch, cb)
        next(query, cb)
      })
    },
    query: function (query, cb, next) {
      if (!query.fingerprint) return next(query, cb)

      byFingerprint.get(query.fingerprint, function (err, match) {
        cb(err, match)
      })
    }
  })

  return typeStore
}
