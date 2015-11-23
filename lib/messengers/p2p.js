
var Q = require('q')
var util = require('util')
var debug = require('debug')('p2p-messenger')
var typeforce = require('typeforce')
var Messenger = require('./base')
var utils = require('../utils')

util.inherits(P2PMessenger, Messenger)
module.exports = P2PMessenger

function P2PMessenger (options) {
  var self = this

  typeforce({
    zlorp: 'Object'
  }, options)

  Messenger.call(this, options)

  this._zlorp = options.zlorp
  this._zlorp.on('data', function (msg, fingerprint) {
    self.emit('message', msg, { fingerprint: fingerprint })
  })
}

P2PMessenger.prototype.send = function (rootHash, msg, identityInfo) {
  var fingerprint = utils.getOTRKeyFingerprint(identityInfo.identity)
  return Q.ninvoke(this._zlorp, 'send', msg, fingerprint)
}

P2PMessenger.prototype.destroy = function () {
  return Q.ninvoke(this._zlorp, 'destroy')
}

// P2PMessenger.prototype._receive = function (buf, fingerprint) {
//   var self = this

//   try {
//     msg = utils.bufferToMsg(buf)
//   } catch (err) {
//     return this.emit('warn', 'received message not in JSON format', buf)
//   }

//   this._debug('received msg', msg)

//   // this thing repeats work all over the place
//   var txInfo
//   var valid = utils.validateMsg(msg)
//   return Q[valid ? 'resolve' : 'reject']()
//     .then(function () {
//       return Q.ninvoke(self.identities, 'byFingerprint', fingerprint)
//     })
//     .then(function (result) {
//       result.msg = msg
//       return result
//     })
// }
