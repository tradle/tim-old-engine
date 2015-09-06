var typeforce = require('typeforce')
var uniq = require('uniq')
var lexint = require('lexicographic-integer')
var collect = require('stream-collector')
var levelup = require('levelup')
var levelQuery = require('level-queryengine')
var jsonQueryEngine = require('jsonquery-engine')
var constants = require('tradle-constants')
var LiveStream = require('level-live-stream')
var CUR_HASH = constants.CUR_HASH
var ROOT_HASH = constants.ROOT_HASH
var lb = require('logbase')
var Entry = lb.Entry
var LogBase = lb.Simple
var EventType = require('./eventType')
var toObj = require('./toObj')
var rebuf = require('./rebufEncoding')
var DEBUG = require('./debug')

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
    timeout: DEBUG ? false : 5000
  })

  db = levelQuery(db)
  db.query.use(jsonQueryEngine())
  db.ensureIndex(CUR_HASH)
  db.ensureIndex(ROOT_HASH)
  LiveStream.install(db)

  db.setMaxListeners(0)
  return db

  function processEntry (entry, cb) {
    var eType = entry.get('type')
    entry = entry.clone()

    switch (eType) {
      case EventType.msg.new:
        return putAndReturnVal(getKey(entry), entry.toJSON(), cb)
      case EventType.msg.receivedValid:
      case EventType.msg.receivedInvalid:
        entry.set('dateReceived', now())
        return updateByCurHash({
          entry: entry,
          createIfMissing: true
        }, function (err, result) {
          if (err) return cb(err)

          cb()
          if (eType === EventType.msg.receivedValid) {
            db.emit('message', result)
          }
        })
      case EventType.msg.sendSuccess:
        entry.set('dateSent', now())
        return update(entry, callbackWithEmit(cb, 'sent'))
      case EventType.msg.sendError:
        return update(entry, cb)
      case EventType.chain.writeSuccess:
        entry.set('dateChained', now())
        return update(entry, callbackWithEmit(cb, 'chained'))
      case EventType.chain.writeError:
        return update(entry, cb)
      case EventType.chain.readSuccess:
        entry.set('dateUnchained', now())
        return onChainReadSuccess(entry, callbackWithEmit(cb, 'unchained'))
      default:
        return cb()
    }
  }

  function callbackWithEmit (cb, event) {
    return function (err, result) {
      if (err) return cb(err)

      cb()
      if (typeof result !== 'undefined') {
        db.emit(event, result)
      }
    }
  }

  function updateByCurHash (options, cb) {
    var entry = options.entry.clone()
    var curHash = entry.get(CUR_HASH)
    collect(db.query(toObj(CUR_HASH, curHash)), function (err, results) {
      if (!db.isOpen()) return cb()
      if (err) return cb(err)
      if (!results.length) {
        if (!options.createIfMissing) return cb(new Error('not found'))

        var saved = entry.toJSON()
        return putAndReturnVal(getKey(entry), saved, cb)
      }

      entry.prev(getHistory(results[0]))
      return update(entry, cb)
    })
  }

  function getHistory (entry) {
    return uniq(entry.prev.concat(entry.id || [])).sort(function (a, b) {
      return a - b
    })
  }

  function setHistory (latest, previous) {
    var h = getHistory(latest)
    if (previous) {
      h = getHistory(previous).concat(h)
    }

    latest.prev = h
    latest.id = h[0]
  }

  function update (entry, cb) {
    var rootId = entry.get('prev')[0]
    return db.get(getKey(rootId), function (err, root) {
      if (!db.isOpen()) return cb()
      if (err) return cb(err)

      var newEntry = Entry.fromJSON(root)
        .set(entry.toJSON())

      var updated = newEntry.toJSON()
      setHistory(updated)
      putAndReturnVal(getKey(newEntry), updated, cb)
    })
  }

  function onChainReadSuccess (entry, cb) {
    var curHash = entry.get(CUR_HASH)
    var query = toObj(CUR_HASH, curHash)

    collect(db.query(query), function (err, vals) {
      if (!db.isOpen()) return cb()

      if (err || !vals.length) {
        return putAndReturnVal(getKey(entry), entry.toJSON(), cb)
      }

      var txId = entry.get('txId')
      if (txId) {
        vals = vals.filter(function (v) {
          return v.txId === txId
        })
      }

      if (vals.length > 1) {
        return cb(new Error('invalid db state, multiple entries with the same txId and hash'))
      }

      var stored = Entry.fromJSON(vals[0])
      stored.set(entry.toJSON())

      var saved = stored.toJSON()
      setHistory(saved, vals[0])
      putAndReturnVal(getKey(saved), saved, cb)
    })
  }

  function putAndReturnVal (key, val, cb) {
    // delete val.type
    // console.log(val)
    db.put(key, val, function (err) {
      if (err) return cb(err)

      cb(null, val)
    })
  }
}

function getProp (entry, prop) {
  return entry instanceof Entry ? entry.get(prop) : entry[prop]
}

function getKey (entry) {
  var id = typeof entry === 'number' ?
    entry :
    getProp(entry, 'prev')[0] || getProp(entry, 'id')

  return lexint.pack(id, 'hex')
}

function now () {
  return Date.now()
}
