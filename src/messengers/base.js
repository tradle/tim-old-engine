
var util = require('util')
var EventEmitter = require('events').EventEmitter
var Q = require('q')
var typeforce = require('typeforce')
var constants = require('@tradle/constants')
var debug = require('debug')('messenger')
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH
var SEND_TIMEOUT = 20000
// var RECV_TEMPLATE = buildRecvTemplate()

module.exports = Messenger
util.inherits(Messenger, EventEmitter)

/**
 * Messenger interface (abstract away http, p2p, etc)
 */
function Messenger () {
  EventEmitter.call(this)
  // this._outboundQ = {}
}

Messenger.prototype.send = function (rid /*, chainedObj, recipientInfo */) {
  throw new Error('not implemented')
}

/**
 * promise content Messenger
 * @param  {String} rid - unique recipient identifier
 * @param  {...} arguments depend on implementor
 * @return {Q.Promise}
 */
// Messenger.prototype.send = function (rid /*, other args */) {
//   typeforce('String', rid)

//   var queue = this._outboundQ[rid] = this._outboundQ[rid] || []
//   var defer = Q.defer()

//   queue.push({
//     defer: defer,
//     args: arguments
//   })

//   this._processQueue(rid)
//   return defer.promise
// }

/**
 * Implement this
 * @return {Q.Promise}
 */
// Messenger.prototype._send = function (toIdentity, chainedObj) {
//   throw new Error('not implemented')
// }

// Messenger.prototype._processQueue = function (rid) {
//   var self = this
//   var queue = this._outboundQ[rid]
//   if (!queue || !queue.length || queue.processing) return

//   queue.processing = true
//   var item = queue.shift()
//   var timeout = setTimeout(function () {
//     item.defer.reject(new Error('send timed out'))
//   }, SEND_TIMEOUT)

//   debug('sending msg to', rid)
//   this._send.apply(this, item.args)
//     .then(function (resp) {
//       debug('sent msg to', rid)
//       clearTimeout(timeout)
//       item.defer.resolve(resp)
//     })
//     .catch(function (err) {
//       debug('failed to send msg to', rid, err.message)
//       // throw err
//       item.defer.reject(err)
//     })
//     .finally(function () {
//       queue.processing = false
//       self._processQueue(rid)
//     })
//     // .done()
// }

// Messenger.prototype.receive = function (msg /*...*/) {
//   var self = this

//   return this._receive.apply(this, arguments)
//     .then(function (result) {
//       typeforce(RECV_TEMPLATE, result)

//       selt.emit('receive', result)
//     })
// }

/**
 * receive msg, return promise that resolves with
 * {
 *   [ROOT_HASH]: sender's root hash
 *   [CUR_HASH]: sender's current hash
 *   identity: sender's identity
 *   msg: message received
 * }
 * @param  {[type]} msg [description]
 * @param  {[type]} rid [description]
 * @return {[type]}     [description]
 */
// Messenger.prototype._receive = function (msg, rid) {
//   throw new Error('not implemented')
// }

Messenger.prototype.destroy = function () {
  throw new Error('not implemented')
}

// function buildRecvTemplate () {
//   var t = {
//     identity: 'Object',
//     msg: 'Object'
//   }

//   t[ROOT_HASH] = 'String'
//   t[CUR_HASH] = 'String'
//   return typeforce.compile(t)
// }
