
var util = require('util')
var Writable = require('readable-stream').Writable
var pad = require('pad')
var debug = require('debug')('oneblock')
var typeforce = require('typeforce')
var concat = require('concat-stream')
var levelup = require('levelup')
var Pend = require('pend')
var extend = require('extend')

module.exports = OneBlockDB
util.inherits(OneBlockDB, Writable)

function OneBlockDB (options) {
  var self = this

  typeforce({
    path: 'String',
    leveldown: 'Function'
  }, options)

  Writable.call(this, {
    objectMode: true,
    highWaterMark: 16
  })

  this._db = levelup(options.path, {
    db: options.leveldown,
    valueEncoding: 'json'
  })

  this._db.createReadStream()
    .pipe(concat(function (txs) {
      options = options
      self._currentHeight = txs.reduce(function (h, txInfo) {
        return Math.max(h, txInfo.height)
      }, 0)

      self._blockTxs = txs.filter(function (txInfo) {
        return txInfo.height === self._currentHeight
      })

      self._ready = true
      self.emit('ready')
    }))
}

OneBlockDB.prototype.height = function () {
  return this._currentHeight
}

OneBlockDB.prototype.destroy = function (cb) {
  this._db.close(cb)
}

OneBlockDB.prototype._write = function (txInfo, enc, next) {
  var self = this

  typeforce({
    height: 'Number',
    tx: 'Object'
  }, txInfo)

  if (this._ready) go()
  else this.once('ready', go)

  function go () {
    self._processTx(txInfo, next)
  }
}

OneBlockDB.prototype._processTx = function (txInfo, cb) {
  var self = this
  var key = getKeyForTx(txInfo)
  var prevHeight = this._currentHeight
  txInfo = extend({}, txInfo)
  if (txInfo.tx.getId) {
    txInfo.tx = txInfo.tx.toHex()
  }

  if (txInfo.height < prevHeight) {
    this.emit('error', new Error('received out of order transaction'))
    return cb()
  }

  var pend = new Pend()
  pend.go(this._db.put.bind(this._db, key, txInfo))
  if (txInfo.height === prevHeight) return pend.wait(cb)

  this._currentHeight = txInfo.height
  var start = getKeyForTx({ height: this._currentHeight })
  var end = start + '\xff'
  this._blockTxs
  this._db.createReadStream({
      start: start,
      end: end
    })
    .pipe(concat(function (batch) {
      debug('deleting ' + batch.length + ' txs: block ' + prevHeight)
      batch = batch.map(function (data) {
        return { type: 'del', key: data.key }
      })

      pend.go(self._db.batch.bind(self._db, batch))
      pend.wait(cb)
    }))
}

function getKeyForTx (txInfo) {
  return pad(20, '' + txInfo.height) + '!' + (txInfo.tx ? txInfo.tx.getId() : '')
}
