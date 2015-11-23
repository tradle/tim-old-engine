
var util = require('util')
var EventEmitter = require('events').EventEmitter
var debug = require('debug')('http-messenger-client')
var Q = require('q')
var typeforce = require('typeforce')
var nets = require('nets')
var debug = require('debug')('http-messenger')
var constants = require('@tradle/constants')
var Messenger = require('./base')
try {
  var putString = !!require('react-native')
} catch (err) {}

var ROOT_HASH = constants.ROOT_HASH

module.exports = HttpMessengerClient
util.inherits(HttpMessengerClient, Messenger)

function HttpMessengerClient (opts) {
  Messenger.call(this)
  this._recipients = {}
  if (opts && opts.rootHash) {
    this.setRootHash(opts.rootHash)
  }
}

HttpMessengerClient.prototype.addEndpoint =
HttpMessengerClient.prototype.addRecipient = function (rootHash, url) {
  typeforce('String', rootHash)
  typeforce('String', url)
  this._recipients[rootHash] = url
}

HttpMessengerClient.prototype.setRootHash = function (hash) {
  typeforce('String', hash)
  this._rootHash = hash
}

HttpMessengerClient.prototype.send = function (rootHash, msg, identityInfo) {
  var self = this
  if (!this._rootHash) {
    return Q.reject(new Error('setRootHash before using'))
  }

  var url = this._recipients[rootHash]
  if (!url) {
    return Q.reject(new Error('recipient url unknown'))
  }

  debug('sending msg to', url)
  return this._doSend(msg, url)
    .then(function (resp) {
      resp = [].concat(resp) // normalize to array
      debug('got response from', url, resp.length, 'items')
      resp.forEach(function (item) {
        // make it consumable by tim
        item = new Buffer(JSON.stringify(item), 'binary')
        var sender = {}
        sender[ROOT_HASH] = identityInfo[ROOT_HASH]
        self.emit('message', item, sender)
      })
    })
}

HttpMessengerClient.prototype._doSend = function (buf, url) {
  var defer = Q.defer()
  if (!/\/$/.test(url)) {
    url += '/'
  }

  nets({
    url: url + this._rootHash,
    body: putString ? buf.toString('binary') : buf,
    method: 'PUT',
    headers: {
      'Content-Type': 'application/octet-stream'
    }
  }, function (err, resp) {
    if (err) return defer.reject(err)
    if (resp.statusCode !== 200) return defer.reject(new Error(resp.body.toString()))

    var body = resp.body
    if (Buffer.isBuffer(body) || typeof body === 'string') {
      try {
        body = JSON.parse(body.toString('binary'))
      } catch (err) {
        return defer.reject(new Error('invalid response'))
      }
    }

    defer.resolve(body)
  })

  return defer.promise
}

HttpMessengerClient.prototype.destroy = function () {
  return Q.resolve()
}
