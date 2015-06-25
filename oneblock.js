
var debug = require('debug')('oneblock')
var typeforce = require('typeforce')
var levelup = require('levelup')
var Pend = require('pend')
var safe = require('safecb')

module.exports = OneBlockDB

function OneBlockDB (options, cb) {
  var self = this

  typeforce({
    path: 'String',
    leveldown: 'Function'
  }, options)

  this._db = levelup(options.path, {
    db: options.leveldown,
    valueEncoding: 'json'
  })

  this._db.createReadStream()
    .on('error', cb)
    .pipe(concat(function (txs) {
      self._currentHeight = txs.reduce(function (h, txInfo) {
        return Math.max(h, txInfo.height)
      }, 0)

      self._blockTxs = txs.filter(function (txInfo) {
        return txInfo.height === self._currentHeight
      })

      cb()
    }))
}

OneBlockDB.prototype.height = function () {
  return this._currentHeight
}

OneBlockDB.prototype.push = function (txInfo, cb) {
  var self = this

  typeforce({
    height: 'Number',
    tx: 'Object'
  }, txInfo)

  cb = safe(cb)
  var key = getKeyForTx(txInfo)
  var prevHeight = this._currentHeight

  if (txInfo.height < prevHeight) {
    return cb(new Error('received out of order transaction'))
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
      var batch = batch.map(function (data) {
        return { type: 'del', key: data.key }
      })

      pend.go(self._db.batch.bind(self._db, batch))
      pend.wait(cb)
    }))
}

function getKeyForTx (txInfo) {
  return pad(20, txInfo.height) + '!' + (txInfo.tx ? txInfo.tx.getId() : '')
}

