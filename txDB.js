
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

module.exports = function createTxDB (path, options) {
  typeforce('String', path)
  typeforce({
    leveldown: 'Function',
    log: 'Log'
  }, options)

  var db = levelup(options.path, {
    db: options.leveldown,
    valueEncoding: rebuf
  })

  db = LogBase(db, options.log, processEntry)
  db = levelQuery(db)
  db.query.use(jsonQueryEngine())
  LiveStream.install(db)

  function processEntry (entry, cb) {
    var eType = entry.get('type')
    entry = entry.clone().unset('type')

    var now = Date.now()

    switch (eType) {
      case EventType.tx:
        entry.set('dateDetected', now)
        return put(entry, cb)
      case EventType.chain.writeSuccess:
        entry.set('dateChained', now)
        return put(entry, cb)
      case EventType.chain.readSuccess:
        entry.set('dateUnchained', now)
        return update(entry, cb)
        /* fall through */
      case EventType.chain.readError:
        return update(entry, cb)
      default:
        return cb()
    }
  }

  return db

  function put (entry, cb) {
    var key = getKey(entry)
    db.put(key, entry.toJSON(), cb)
  }

  function update (entry, cb) {
    var key = getKey(entry)
    return db.get(key, function (err, root) {
      if (!db.isOpen()) return cb()
      if (err) return cb(err)

      // wtf is this?
      var entryJSON = entry.toJSON()
      entry
        .set(root)
        .set(entryJSON)

      db.put(key, entry.toJSON(), cb)
    })
  }
}

function getKey (entry) {
  var txId = entry.get('txId')
  if (txId) return txId

  throw new Error('missing "txId"')
}
