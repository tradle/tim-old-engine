var debug = require('debug')('msgDB')
var typeforce = require('typeforce')
// var lexint = require('lexicographic-integer')
var collect = require('stream-collector')
var levelup = require('levelup')
var Q = require('q')
var pump = require('pump')
var map = require('map-stream')
// var levelQuery = require('level-queryengine')
// var jsonQueryEngine = require('jsonquery-engine')
var safe = require('safecb')
var constants = require('@tradle/constants')
// var TxData = require('tradle-tx-data').TxData
var levelErrs = require('level-errors')
var LiveStream = require('level-live-stream')
var CUR_HASH = constants.CUR_HASH
var ROOT_HASH = constants.ROOT_HASH
var TYPE = constants.TYPE
var TYPES = constants.TYPES
var lb = require('logbase')
var Entry = lb.Entry
var LogBase = lb.Simple
var EventType = require('./eventType')
var rebuf = require('./rebufEncoding')
var DEBUG = require('./debug')
var utils = require('./utils')
var Errors = require('./errors')
var SEPARATOR = '!'
var now = utils.now
var ENTRY_TIMEOUT = DEBUG ? false : 5000

module.exports = function createMsgDB (path, options) {
  typeforce('String', path)
  typeforce({
    leveldown: 'Function',
    log: 'Log',
    keeper: 'Object'
  }, options)

  var keeper = options.keeper
  var db = levelup(path, {
    db: options.leveldown,
    valueEncoding: rebuf
  })

  db = LogBase({
    db: db,
    log: options.log,
    process: processEntry,
    timeout: typeof options.timeout === 'undefined' ? ENTRY_TIMEOUT : options.timeout,
    autostart: options.autostart
  })

  var topics = [
    EventType.misc.forget,
    EventType.msg.new,
    EventType.msg.edit,
    EventType.msg.receivedValid,
    EventType.msg.sendSuccess,
  ]

  LiveStream.install(db)

  var createReadStream = db.createReadStream
  db.createReadStream = function (type) {
    return createReadStream.call(db, {
      lte: type + SEPARATOR + '\xff',
      gte: type + SEPARATOR,
      keys: false
    })
    .pipe(map(function (val, cb) {
      cb(null, val.meta)
    }))
  }

  db.setMaxListeners(0)
  return db

  function myDebug () {
    var args = [].slice.call(arguments)
    args.unshift(db.name)
    return debug.apply(null, args)
  }

  function processEntry (entry, cb) {
    var eType = entry.get('type')
    if (topics.indexOf(eType) === -1) return cb()

    entry = entry.clone()

    if (eType === EventType.misc.forget) {
      return forget(entry, cb)
    }

    const type = entry.get(TYPE)
    if (!type) return cb()

    var key = type + SEPARATOR + entry.get(ROOT_HASH)
    return db.get(key, function (err, state) {
      if (!db.isOpen()) return cb()
      if (err && !err.notFound) throw err // should never happen

      var newEntry = state
        ? utils.updateEntry(state, entry)
        : entry

      var from = newEntry.get('from')
      from = from && from[ROOT_HASH]

      var to = newEntry.get('to')
      to = to && to[ROOT_HASH]
      var batch = [{
        type: 'put',
        key: key,
        value: {
          // data needed for lookupObject
          meta: utils.pick(entry.toJSON(), ['txType', 'txData', 'addressesFrom', 'addressesTo']),
          participants: [ from, to ]
        }
      }]

      cb(batch)
    })
  }

  function forget (entry, cb) {
    var otherGuyRootHash = entry.get('who')
    var togo = 3
    var stream = db.rawReadStream({
      gt: '\x00'
    })
    .pipe(map(function (data, cb) {
      if (data.value.participants.indexOf(otherGuyRootHash) !== -1) {
        cb(null, data)
      } else {
        cb()
      }
    }))

    collect(stream, function (err, results) {
      if (err) return cb(err)

      var batch = results.map(function (data) {
        return {
          key: data.key,
          type: 'del'
        }
      })

      cb(batch)
    })
  }
}

function errWithNotFound (cb) {
  return cb(new levelErrs.NotFoundError())
}
