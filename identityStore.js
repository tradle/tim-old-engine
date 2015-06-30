
var util = require('util')
var extend = require('extend')
var pick = require('object.pick')
var Identity = require('midentity').Identity
var constants = require('tradle-constants')
var TypeStore = require('./typestore')

module.exports = function identityStore (options) {
  var typeStore = new TypeStore(extend({
    type: Identity.TYPE,
  }, options))

  var byFingerprint = typeStore.createSublevel('byFingerprint')
  var storedFingerprints = []

  typeStore.use({
    _update: function (obj, cb, next) {
      var rootHash = obj[constants.ROOT_HASH]
      console.log('stored', rootHash)
      var fingerprintBatch = obj.pubkeys.map(function (k) {
        storedFingerprints.push(k.fingerprint)
        return { type: 'put', key: k.fingerprint, value: rootHash }
      })

      // console.log('fingerprints', obj.pubkeys.map(function (k) {
      //   return k.fingerprint
      // }))

      byFingerprint.batch(fingerprintBatch, function (err) {
        if (err) return cb(err)

        next(obj, cb)
      })
    },
    _query: function (query, cb, next) {
      if (!query.fingerprint) return next(query, cb)

      // console.log('has fingerprint', storedFingerprints.indexOf(query.fingerprint) !== -1)
      byFingerprint.get(query.fingerprint, function (err, match) {
        query.fingerprint = query.fingerprint
        if (err) return cb(err)

        typeStore.get(match, function (err, identity) {
          if (err) return cb(err)

          cb(null, [identity])
        })
      })
    }
  })

  return typeStore
}
