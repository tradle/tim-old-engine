var debug = require('debug')('msgDB')
var typeforce = require('typeforce')
// var lexint = require('lexicographic-integer')
var collect = require('stream-collector')
var levelup = require('levelup')
// var levelQuery = require('level-queryengine')
// var jsonQueryEngine = require('jsonquery-engine')
var safe = require('safecb')
var constants = require('@tradle/constants')
// var TxData = require('tradle-tx-data').TxData
var levelErrs = require('level-errors')
var LiveStream = require('level-live-stream')
var CUR_HASH = constants.CUR_HASH
// var ROOT_HASH = constants.ROOT_HASH
var lb = require('logbase')
var Entry = lb.Entry
var LogBase = lb.Simple
var EventType = require('./eventType')
var rebuf = require('./rebufEncoding')
var DEBUG = require('./debug')
var utils = require('./utils')
var Errors = require('./errors')
var now = utils.now
var ENTRY_TIMEOUT = DEBUG ? false : 5000
var MAIN_PREFIX = 'm!'
var TIMESTAMP_PREFIX = 't'
var CHAIN_TIMESTAMP_PREFIX = TIMESTAMP_PREFIX + 'c!'
var SEND_TIMESTAMP_PREFIX = TIMESTAMP_PREFIX + 's!'

module.exports = function createMsgDB (path, options) {
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
    autostart: options.autostart
  })

  // db = levelQuery(db)
  // db.query.use(jsonQueryEngine())
  // db.ensureIndex(CUR_HASH)
  // db.ensureIndex(ROOT_HASH)
  LiveStream.install(db)

  // db.byCurHash = function (curHash, cb) {
  //   collect(db.createReadStream({
  //     start: curHash,
  //     end: curHash + '\xff'
  //   }), cb)
  // }

  db.byRootHash = db.liveOnly(function (rootHash, cb) {
    collect(db.createValueStream({
      start: rootHash,
      end: rootHash + '\xff'
    }), function (err, entries) {
      if (err) return cb(err)
      if (!entries || !entries.length) {
        return cb(new levelErrs.NotFoundError())
      }

      return cb(null, entries)
    })
  })

  db.byCurHash = db.liveOnly(function (curHash, cb) {
    // TODO: add indices for searchable properties
    // to make this more efficient
    cb = safe(cb)
    var stream = db.createValueStream()
      .on('data', function (value) {
        if (value[CUR_HASH] === curHash) {
          cb(null, value)
          stream.destroy()
        }
      })
      .on('end', function () {
        cb(new levelErrs.NotFoundError())
      })
  })

  db.byRootHashAndCurHash = db.liveOnly(function (rootHash, curHash, cb) {
    return db.byRootHash(rootHash, function (err, entries) {
      if (err) return cb(err)

      var found = entries.some(function (e) {
        if (e[CUR_HASH] === curHash) {
          cb(null, e)
        }
      })

      if (!found) {
        cb(null, new levelErrs.NotFoundError())
      }
    })
  })

  var createReadStream = db.createReadStream
  db.createReadStream = function (opts) {
    opts = opts || {}
    if (!(opts.gt || opts.lt)) {
      opts.gt = MAIN_PREFIX
      opts.lt = MAIN_PREFIX + '\xff'
    }

    return createReadStream.call(db, opts)
  }

  // var get = db.get
  // db.get = function (key, cb) {
  //   return get.call(db, MAIN_PREFIX + key, cb)
  // }

  db.getToChainStream = function (opts) {
    opts = opts || {}
    opts.gt = CHAIN_TIMESTAMP_PREFIX
    opts.lt = CHAIN_TIMESTAMP_PREFIX + '\xff'
    return db.liveStream(opts)
  }

  db.getToSendStream = function (opts) {
    opts = opts || {}
    opts.gt = SEND_TIMESTAMP_PREFIX
    opts.lt = SEND_TIMESTAMP_PREFIX + '\xff'
    return db.liveStream(opts)
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
    entry = entry.clone()

    var postEvent

    switch (eType) {
      case EventType.msg.new:
        myDebug('new msg', entry.get(CUR_HASH))
        break
      case EventType.msg.receivedValid:
        postEvent = 'message'
        // fall through
      case EventType.msg.receivedInvalid:
        myDebug('received msg', entry.get(CUR_HASH))
        entry.set('dateReceived', now())
        break
      case EventType.msg.sendSuccess:
        myDebug('sent msg', entry.get(CUR_HASH))
        entry.set('dateSent', now())
        postEvent = 'sent'
        break
      case EventType.msg.sendError:
        myDebug('send error', entry.get(CUR_HASH))
        break
      case EventType.chain.writeSuccess:
        myDebug('msg chained', entry.get(CUR_HASH), entry.get('txId'))
        entry.set('dateChained', now())
        postEvent = 'chained'
        break
      case EventType.chain.writeError:
        myDebug('msg chained (error)', entry.get(CUR_HASH))
        break
      case EventType.chain.readSuccess:
        myDebug('unchained', entry.get(CUR_HASH), entry.get('txId'), entry.get('dateReceived'))
        entry.set('dateUnchained', now())
        postEvent = 'unchained'
        break
      case EventType.chain.readError:
        myDebug('msg unchained (error)', entry.get(CUR_HASH), entry.get('txId'))
        try {
          getKey(entry)
        } catch (err) {
          // not storing
          return cb()
        }

        break
        // TODO:
      default:
        myDebug('ignoring entry of type', eType)
        return cb()
    }

    update(entry, function (batch) {
      cb(batch)
      if (postEvent && batch) {
        db.emit(postEvent, batch[0].value)
      }
    })
  }

  function update (entry, cb) {
    // console.log('updating', entry.get(CUR_HASH), entry.get(ROOT_HASH))
    var key
    try {
      key = getKey(entry)
    } catch (err) {
      myDebug(err.message)
      return cb()
    }

    return db.get(key, function (err, state) {
      if (!db.isOpen()) return cb()
      if (err && !err.notFound) throw err // should never happen

      var newEntry = state
        ? utils.updateEntry(state, entry)
        : entry

      batch = [{
        type: 'put',
        key: getKey(newEntry),
        value: newEntry.toJSON()
      }]

      // if (!utils.getEntryProp(newEntry, constants.TYPE)) {
      //   throw new Error(utils.getEntryProp(newEntry, 'type'))
      // }

      addTimestampOps(newEntry, entry, batch)

      cb(batch)
    })
  }
}

function getKey (entry) {
  return MAIN_PREFIX + utils.getUID(entry)
}

function addTimestampOps (state, update, batch) {
  var entryKey = getKey(state)
  var isNew = utils.getEntryProp(update, 'type') === EventType.msg.new
  if (utils.getEntryProp(state, 'chain')) {
    if (isNew) {
      batch.push({
        type: 'put',
        key: getChainTimestampKey(update),
        value: entryKey
      })
    } else if (utils.getEntryProp(state, 'dateChained') ||
      utils.getErrors(state, 'chain') >= Errors.MAX_CHAIN) {
      // remove from queue
      batch.push({
        type: 'del',
        key: getChainTimestampKey(state),
        value: entryKey
      })
    }
  }

  if (utils.getEntryProp(state, 'deliver')) {
    if (isNew) {
      batch.push({
        type: 'put',
        key: getSendTimestampKey(update),
        value: entryKey
      })
    } else if (utils.getEntryProp(state, 'dateSent') ||
      utils.getErrors(state, 'send') >= Errors.MAX_SEND) {
      batch.push({
        type: 'del',
        key: getSendTimestampKey(state),
        value: entryKey
      })
    }
  }
}

function getChainTimestampKey (entry) {
  return CHAIN_TIMESTAMP_PREFIX + utils.getEntryProp(entry, 'timestamp')
}

function getSendTimestampKey (entry) {
  return SEND_TIMESTAMP_PREFIX + utils.getEntryProp(entry, 'timestamp')
}
