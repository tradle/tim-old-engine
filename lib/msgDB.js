var debug = require('debug')('msgDB')
var typeforce = require('typeforce')
var uniq = require('uniq')
// var lexint = require('lexicographic-integer')
// var collect = require('stream-collector')
var levelup = require('levelup')
var levelQuery = require('level-queryengine')
var jsonQueryEngine = require('jsonquery-engine')
var constants = require('tradle-constants')
// var TxData = require('tradle-tx-data').TxData
var LiveStream = require('level-live-stream')
var CUR_HASH = constants.CUR_HASH
var ROOT_HASH = constants.ROOT_HASH
var lb = require('logbase')
var Entry = lb.Entry
var LogBase = lb.Simple
var EventType = require('./eventType')
// var toObj = require('./toObj')
var rebuf = require('./rebufEncoding')
var DEBUG = require('./debug')
var now = require('./now')
var getUID = require('./getUID')
var ENTRY_TIMEOUT = DEBUG ? false : 5000

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
    timeout: ENTRY_TIMEOUT,
    autostart: options.autostart
  })

  db = levelQuery(db)
  db.query.use(jsonQueryEngine())
  db.ensureIndex(CUR_HASH)
  db.ensureIndex(ROOT_HASH)
  LiveStream.install(db)

  db.setMaxListeners(0)
  return db

  function myDebug () {
    var args = [].slice.call(arguments)
    args.unshift(db.name)
    return debug.apply(null, args)
  }

  function processEntry (entry, cb) {
    if (entry.get('public')) {
      return update(entry, cb)
    }

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

    // cb = postEvent ? callbackWithEmit(cb, postEvent) : cb
    update(entry, function (err, result) {
      if (err) return cb(err)

      cb()
      if (postEvent && typeof result !== 'undefined') {
        db.emit(postEvent, result)
      }
    })
  }

  // function conditionalUpdate (entry, filter, cb) {
  //   if (typeof cb === 'undefined') {
  //     cb = filter
  //     filter = null
  //   }

  function update (entry, cb) {
    return db.get(getKey(entry), function (err, root) {
      if (!db.isOpen()) return cb()
      if (err && !err.notFound) return cb(err)

      var newEntry
      if (root) {
        // if (filter && !filter(root)) return cb()

        var entryJSON = entry.toJSON()
        newEntry = Entry.fromJSON(root)
          .set(entryJSON)

        if (root.errors && entryJSON.errors) {
          entry.set('errors', root.errors.concat(entryJSON.errors))
        }
      } else {
        newEntry = entry
      }

      var updated = newEntry.toJSON()
      // setHistory(updated)
      putAndReturnVal(getKey(newEntry), updated, cb)
    })
  }

  // function getHistory (entry) {
  //   return uniq(entry.prev.concat(entry.id || [])).sort(function (a, b) {
  //     return a - b
  //   })
  // }

  // function setHistory (latest, previous) {
  //   var h = getHistory(latest)
  //   if (previous) {
  //     h = getHistory(previous).concat(h)
  //   }

  //   latest.prev = h
  //   latest.id = h[0]
  // }

  function putAndReturnVal (key, val, cb) {
    db.put(key, val, function (err) {
      if (err) return cb(err)

      cb(null, val)
    })
  }
}

// function getProp (entry, prop) {
//   return entry instanceof Entry ? entry.get(prop) : entry[prop]
// }

function getKey (entry) {
  return getUID(entry)

  // var isPublic = getProp(entry, 'txType') === TxData.types.public
  // var curHash = getProp(entry, CUR_HASH)
  // if (isPublic) return 'public-' + curHash

  // var from = getProp(entry, 'from')
  // from = from && from[ROOT_HASH]
  // var to = getProp(entry, 'to')
  // to = to && to[ROOT_HASH]
  // if (!(from && to && curHash)) {
  //   throw new Error('unable to derive key for value: ' + JSON.stringify(entry))
  // }

  // return from + '-' + to + '-' + curHash

  // console.log(from, to, curHash)

  // var id = typeof entry === 'number' ?
  //   entry :
  //   getProp(entry, 'prev')[0] || getProp(entry, 'id')

  // return lexint.pack(id, 'hex')
}
