
var bitcoin = require('@tradle/bitcoinjs-lib')
var ECPubKey = bitcoin.ECPubKey
var Cache = require('lru-cache')
var cachedByHex = new Cache({
  max: 100
})

var fromBuffer = ECPubKey.fromBuffer
var fromHex = ECPubKey.fromHex

ECPubKey.fromBuffer = function (buffer, skipCache) {
  var hex
  if (!skipCache) {
    hex = buffer.toString('hex')
    var cached = cachedByHex.get(hex)
    if (cached) return cached
  }

  var key = fromBuffer.call(ECPubKey, buffer)
  if (!skipCache) {
    cachedByHex.set(hex, key)
  }

  return key
}

ECPubKey.fromHex = function (hex) {
  var cached = cachedByHex.get(hex)
  if (cached) return cached

  var key = ECPubKey.fromBuffer(new Buffer(hex, 'hex'), true) // skip cache
  cachedByHex.set(hex, key)
  return key
}
