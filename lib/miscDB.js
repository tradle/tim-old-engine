
var debug = require('debug')('txDB')
var typeforce = require('typeforce')
var levelup = require('levelup')
var LiveStream = require('level-live-stream')
// var CUR_HASH = constants.CUR_HASH
var lb = require('logbase')
var LogBase = lb.Simple
var EventType = require('./eventType')
var rebuf = require('./rebufEncoding')
var DEBUG = require('./debug')
var utils = require('./utils')
var ENTRY_TIMEOUT = DEBUG ? false : 5000
var WATCH_ADDRS_KEY = 'watched-addrs'
var WATCH_TXS_KEY = 'watched-txs'

module.exports = function createTxDB (path, options) {
  typeforce('String', path)
  typeforce({
    leveldown: 'Function',
    log: 'Log'
  }, options)

  var db = levelup(path, {
    db: options.leveldown,
    valueEncoding: rebuf
  })

  db = LogBase({
    db: db,
    log: options.log,
    process: processEntry,
    timeout: typeof options.timeout === 'undefined' ? ENTRY_TIMEOUT : options.timeout,
    autostart: options.autostart,
    topics: [
      EventType.tx.ignore,
      EventType.tx.watch,
      EventType.misc.watchAddresses,
      EventType.misc.unwatchAddresses
    ]
  })

  db.getWatchedAddresses = function (cb) {
    db.get(WATCH_ADDRS_KEY, getWatchCallback(cb))
  }

  db.getWatchedTxs = function (cb) {
    db.get(WATCH_TXS_KEY, getWatchCallback(cb))
  }

  return db

  function getWatchCallback (cb) {
    return function (err, results) {
      if (err && !err.notFound) return cb(err)

      return cb(null, results || [])
    }
  }

  function myDebug () {
    var args = [].slice.call(arguments)
    args.unshift(db.name)
    return debug.apply(null, args)
  }

  function processEntry (entry, cb) {
    var eType = entry.get('type')
    // entry = entry.clone()

    var now = Date.now()

    debugger
    switch (eType) {
      case EventType.tx.watch:
        return editWatch({
          watch: [entry.get('txId')],
          key: WATCH_TXS_KEY,
          event: 'watchedTxs'
        }, cb)
      case EventType.tx.ignore:
        return editWatch({
          unwatch: [entry.get('txId')],
          key: WATCH_TXS_KEY,
          event: 'watchedTxs'
        }, cb)
      case EventType.misc.watchAddresses:
        return editWatch({
          watch: entry.get('addresses'),
          key: WATCH_ADDRS_KEY,
          event: 'watchedAddresses'
        }, cb)
      case EventType.misc.unwatchAddresses:
        return editWatch({
          unwatch: entry.get('addresses'),
          key: WATCH_ADDRS_KEY,
          event: 'watchedAddresses'
        }, cb)
      default:
        myDebug('ignoring entry of type', eType)
        return cb()
    }
  }

  function editWatch (opts, cb) {
    var watch = opts.watch
    var unwatch = opts.unwatch
    var key = opts.key

    db.get(opts.key, function (err, items) {
      if (err && !err.notFound) {
        if (DEBUG) throw err
        else return cb()
      }

      items = items || []
      if (unwatch) {
        items = items.filter(function (item) {
          return unwatch.indexOf(item) !== -1
        })
      }

      if (watch) {
        items = watch
          .filter(function (item) {
            return items.indexOf(item) === -1
          })
          .concat(items)
      }

      if (opts.event) {
        process.nextTick(function () {
          db.emit(opts.event, items)
        })
      }

      cb([{
        type: 'put',
        key: opts.key,
        value: items
      }])
    })
  }
}
