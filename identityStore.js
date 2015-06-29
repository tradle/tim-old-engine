
var util = require('util')
var extend = require('extend')
var pick = require('object.pick')
var Identity = require('midentity').Identity
var TypeStore = require('./typestore')
var constants = require('./constants')

module.exports = function identityStore (options) {
  var typeStore = new TypeStore(extend({
    type: Identity.TYPE,
  }, options))

  var byFingerprint = typeStore.createSublevel('byFingerprint')

  typeStore.use({
    _update: function (obj, cb, next) {
      var rootHash = obj[constants.rootHash]
      var fingerprintBatch = obj.pubkeys.map(function (k) {
        return { type: 'put', key: k.fingerprint, value: rootHash }
      })

      console.log('fingerprints', obj.pubkeys.map(function (p) {return p.fingerprint}))
      byFingerprint.batch(fingerprintBatch, function (err) {
        if (err) return cb(err)

        next(obj, cb)
      })
    },
    _query: function (query, cb, next) {
      if (!query.fingerprint) return next(query, cb)

      byFingerprint.get(query.fingerprint, function (err, match) {
        query.fingerprint = query.fingerprint
        if (err) return cb(err)

        typeStore.get(match, cb)
      })
    }
  })

  return typeStore
}
