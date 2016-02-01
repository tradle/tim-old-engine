
var Q = require('q')
var bitcoin = require('@tradle/bitcoinjs-lib')
var ECPubKey = bitcoin.ECPubKey
var Keys = require('@tradle/kiki').Keys
var ECKey = bitcoin.ECKey
var Cache = require('lru-cache')
var EllipticKeyPair = require('elliptic/lib/elliptic/ec/key')
var utils = require('@tradle/utils')
var extend = require('xtend')
var Driver = require('../')
var enabled = false
var ID = 0
var UINT16    = 0xffff;
var getNextId = function () {
  return (ID++) & UINT16
}

function enable () {
  if (enabled) return

  enabled = true
  enableECPubKeyCaching()
  enableLookupObjectCaching()
}

module.exports = enable
module.exports.enable = enable
module.exports.enableLookupObjectCaching = enableLookupObjectCaching
module.exports.enableECPubKeyCaching = enableECPubKeyCaching

function enableLookupObjectCaching () {
  // var lookupObjectCache = new Cache({
  //   max: 30,
  //   maxAge: 60000 // 1 min
  // })

  // var lookupObject = Driver.prototype.lookupObject
  // Driver.prototype.lookupObject = function (info, verify) {
  //   var key
  //   if (info.txData) {
  //     key = info.txData.toString('hex')
  //     var cached = !verify && lookupObjectCache.get(key)
  //     if (cached) return Q(cached)
  //   }

  //   return lookupObject.apply(this, arguments)
  //     .then(function (result) {
  //       key = key || result.txData.toString('hex')
  //       lookupObjectCache.set(key, Object.freeze(result))
  //       return result
  //     })
  // }
}

function enableECPubKeyCaching () {
  // var cachedECPubKeys = new Cache({
  //   max: 500
  // })

  var cachedBitcoinPubKeys = new Cache({
    max: 500
  })

  var cachedBitcoinPrivKeys = new Cache({
    max: 500
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

  var fromWIF = ECKey.fromWIF
  ECKey.fromWIF = function (wif) {
    var cached = cachedBitcoinPrivKeys.get(wif)
    if (!cached) {
      cached = fromWIF(wif)
      cachedBitcoinPrivKeys.set(wif, cached)
    }

    return cached
  }

  Object.keys(Keys).filter(function (type) {
    return type !== 'Base'
  }).forEach(function (type) {
    var cache = new Cache({
      max: 500
    })

    var KeyCl = Keys[type]
    ;['parsePub', 'parsePriv'].forEach(function (method) {
      var orig = KeyCl.prototype[method]
      KeyCl.prototype[method] = function (pub) {
        var cached = cache.get(pub)
        if (!cached) {
          cached = orig.apply(this, arguments)
          cache.set(pub, cached)
        }

        return cached
      }
    })
  })

  var ecdh = utils.ecdh
  var cache = new Cache({
    max: 500,
    maxAge: 60 * 1000
  })

  utils.ecdh = function (aPriv, bPub) {
    var key = aPriv + bPub
    var cached = cache.get(key)
    if (!cached) {
      cached = ecdh.apply(this, arguments)
      cache.set(key, cached)
    }

    return cached
  }

  var ecdhCache = new Cache({
    max: 1000,
    maxAge: 60 * 1000
  })

  var sharedEncryptionKey = utils.sharedEncryptionKey
  utils.sharedEncryptionKey = function (aPriv, bPub, cb) {
    // TODO: unhardcode network
    if (typeof aPriv !== 'string') aPriv = aPriv.toWIF(bitcoin.networks.testnet)
    if (typeof bPub !== 'string') bPub = bPub.toHex()

    var key = aPriv + bPub
    var cached = ecdhCache.get(key)
    if (!cb) {
      if (!cached) {
        cached = sharedEncryptionKey.apply(this, arguments)
        ecdhCache.set(key, cached)
      }

      return cached
    }

    if (cached) return cb(null, cached)

    sharedEncryptionKey.call(utils, aPriv, bPub, function (err, result) {
      if (err) return cb(err)

      ecdhCache.set(key, result)
      return cb(null, result)
    })
  }

  // var edCache = new Cache({
  //   max: 1000,
  //   maxAge: 60 * 1000
  // })

  // ;['encryptAsync', 'decryptAsync'].forEach(function (method) {
  //   var orig = utils[method]
  //   utils[method] = function (opts, cb) {
  //     var key = method + opts.data.toString('hex')
  //     var cached = edCache.get(key)
  //     if (cached) {
  //       console.log(method + ' from cache!')
  //       return cb(null, cached)
  //     }

  //     orig.call(utils, opts, function (err, result) {
  //       if (err) return cb(err)

  //       edCache.set(key, result)
  //       cb(null, result)
  //     })
  //   }
  // })
}
