
var debug = require('debug')('txDB')
var typeforce = require('typeforce')
var levelup = require('levelup')
var levelQuery = require('level-queryengine')
var jsonQueryEngine = require('jsonquery-engine')
var LiveStream = require('level-live-stream')
// var CUR_HASH = constants.CUR_HASH
var lb = require('logbase')
var LogBase = lb.Simple
var EventType = require('./eventType')
var rebuf = require('./rebufEncoding')
var DEBUG = require('./debug')
var ENTRY_TIMEOUT = DEBUG ? false : 5000

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
    timeout: ENTRY_TIMEOUT,
    autostart: options.autostart
  })

  db = levelQuery(db)
  db.query.use(jsonQueryEngine())
  LiveStream.install(db)

  return db

  function myDebug () {
    var args = [].slice.call(arguments)
    args.unshift(db.name)
    return debug.apply(null, args)
  }

  function processEntry (entry, cb) {
    var eType = entry.get('type')
    entry = entry.clone().unset('type')

    var now = Date.now()

    switch (eType) {
      case EventType.tx.new:
        entry.set('dateDetected', now)
        myDebug('new tx')
        return updateOrPut(entry, cb)
      case EventType.tx.confirmation:
        if (!entry.get('dateDetected')) {
          // this may be the first confirmation
          // for a tx we put on chain
          entry.set('dateDetected', now)
        }

        myDebug('confirmation tx')
        return updateOrPut(entry, cb)
      case EventType.chain.writeSuccess:
        entry.set('dateChained', now)
        myDebug('chain write success')
        return updateOrPut(entry, cb)
      case EventType.chain.readSuccess:
        entry.set('dateUnchained', now)
        myDebug('chain read success')
        return updateOrPut(entry, cb)
      case EventType.chain.readError:
        myDebug('chain read error')
        return updateOrPut(entry, cb)
      default:
        myDebug('ignoring entry of type', eType)
        return cb()
    }
  }

  function put (entry, cb) {
    var key = getKey(entry)
    db.put(key, entry.toJSON(), cb)
  }

  function updateOrPut (entry, cb) {
    var key = getKey(entry)
    return db.get(key, function (err, root) {
      if (!db.isOpen()) return cb()
      if (err) {
        if (err.notFound) return put(entry, cb)
        else return cb(err)
      }

      doUpdate(root, entry, cb)
    })
  }

  function update (entry, cb) {
    var key = getKey(entry)
    return db.get(key, function (err, root) {
      if (!db.isOpen()) return cb()
      if (err) return cb(err)

      // wtf is this?
      doUpdate(root, entry, cb)
    })
  }

  function doUpdate (root, entry, cb) {
    var entryJSON = entry.toJSON()
    entry
      .set(root)
      .set(entryJSON)
      .set('timestamp', root.timestamp)
      // don't overwrite timestamp

    // console.log('overwriting', root, 'with', entryJSON, 'resulting in', entry.toJSON())
    put(entry, cb)
  }
}

function getKey (entry) {
  var txId = entry.get('txId')
  if (txId) return txId

  throw new Error('missing "txId"')
}
