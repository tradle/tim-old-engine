
var Q = require('q')
var bitcoin = require('@tradle/bitcoinjs-lib')
var ECPubKey = bitcoin.ECPubKey
var ECKey = require('@tradle/kiki').Keys.EC
var Cache = require('lru-cache')
var extend = require('xtend')
var Driver = require('../')
var enabled = false

var enable = function () {
  if (enabled) return

  enabled = true
  var lookupObjectCache = new Cache({
    max: 30,
    maxAge: 60000 // 1 min
  })

  var cachedBitcoinPubKeys = new Cache({
    max: 100
  })

  var cachedECPubKeys = new Cache({
    max: 100
  })

  var fromBuffer = ECPubKey.fromBuffer
  var fromHex = ECPubKey.fromHex

  ECPubKey.fromBuffer = function (buffer, skipCache) {
    var hex
    if (!skipCache) {
      hex = buffer.toString('hex')
      var cached = cachedBitcoinPubKeys.get(hex)
      if (cached) return cached
    }

    var key = fromBuffer.call(ECPubKey, buffer)
    if (!skipCache) {
      cachedBitcoinPubKeys.set(hex, key)
    }

    return key
  }

  ECPubKey.fromHex = function (hex) {
    var cached = cachedBitcoinPubKeys.get(hex)
    if (!cached) {
      cached = ECPubKey.fromBuffer(new Buffer(hex, 'hex'), true) // skip cache
      cachedBitcoinPubKeys.set(hex, cached)
    }

    return cached
  }

  var ecParsePub = ECKey.prototype.parsePub
  ECKey.prototype.parsePub = function (pub) {
    var cached = cachedECPubKeys.get(pub)
    if (!cached) {
      cached = ecParsePub.apply(this, arguments)
      cachedECPubKeys.set(pub, cached)
    }

    return cached
  }

  var lookupObject = Driver.prototype.lookupObject
  Driver.prototype.lookupObject = function (info) {
    var key
    if (info.txData) {
      key = info.txData.toString('hex')
      var cached = lookupObjectCache.get(key)
      if (cached) {
        return Q(cached)
      }
    }

    return lookupObject.apply(this, arguments)
      .then(function (result) {
        key = key || result.txData.toString('hex')
        lookupObjectCache.set(key, Object.freeze(result))
        return result
      })
  }
}

module.exports = enable
module.exports.enable = enable