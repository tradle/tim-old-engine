
var typeforce = require('typeforce')
var lexint = require('lexicographic-integer')
var collect = require('stream-collector')
var filter = require('./filterStream')
var levelup = require('levelup')
var levelQuery = require('level-queryengine')
var jsonQueryEngine = require('jsonquery-engine')
var constants = require('tradle-constants')
var LiveStream = require('level-live-stream')
var CUR_HASH = constants.CUR_HASH
var ROOT_HASH = constants.ROOT_HASH
var lb = require('logbase')
var Entry = lb.Entry
var Base = lb.Simple
var EventType = require('./eventType')

module.exports = function createMsgDB (path, options) {
  typeforce('String', path)
  typeforce({
    leveldown: 'Function',
    log: 'Log'
  }, options)

  var db = levelQuery(levelup(options.path, {
    db: options.leveldown,
    valueEncoding: 'json'
  }))

  db.query.use(jsonQueryEngine())
  db.ensureIndex(CUR_HASH)
  db.ensureIndex(ROOT_HASH)
  LiveStream.install(db)
  Base(db, options.log, processEntry)

  // var db = new Base({
  //   log: options.log,
  //   db: db
  // })

  // db.query = db.query.bind(db)
  // db.query = function (q) {
  //   console.log('querying')
  //   return db.createReadStream()
  //     .pipe(filter(function (data) {
  //       var val = data.value
  //       for (var p in q) {
  //         // var pVal = q[p]
  //         if (val[p] !== q[p]) {
  //           console.log('not equal: ', q[p], val[p])
  //           return false
  //         }
  //       }

  //       return true
  //     }))
  // }

  // var mainDB = db.db()
  // db.liveStream = mainDB.liveStream.bind(mainDB)

  // var put = db.db().put
  // db.db().put = function () {
  //   console.log('put', arguments)
  //   return put.apply(this, arguments)
  // }

  function processEntry (entry, cb) {
    var eType = entry.get('type')
    entry = entry.clone().unset('type')

    switch (eType) {
      case EventType.msg.new:
        return db.put(getKey(entry), entry.toJSON(), cb)
      case EventType.msg.sent:
        entry.set('sent', true)
        return update(entry, cb)
      case EventType.chain.writeSuccess:
        entry.set('chained', true)
        db.emit('chained', entry.toJSON())
        return update(entry, cb)
      case EventType.chain.writeError:
        return update(entry, cb)
      case EventType.chain.readSuccess:
        db.emit('unchained', entry.toJSON())
        return onChainReadSuccess(entry, cb)
      default:
        return cb()
    }
  }

  db.setMaxListeners(0)
  return db

  function update (entry, cb) {
    var rootId = entry.get('prev')[0]
    return db.get(getKey(rootId), function (err, root) {
      if (err) return cb(err)

      var newEntry = Entry.fromJSON(root)
        .set(entry.toJSON())
        .prev(entry)

      db.put(getKey(newEntry), newEntry.toJSON(), cb)
    })
  }

  function onChainReadSuccess (entry, cb) {
    var curHash = entry.get(CUR_HASH)
    var query = toObj(CUR_HASH, curHash)

    collect(db.query(query), function (err, vals) {
      if (err || !vals.length) {
        return db.put(getKey(entry), entry.toJSON(), cb)
      }

      if (vals.length > 1) {
        return cb(new Error('invalid db state, multiple entries for same hash'))
      }

      var stored = Entry.fromJSON(vals[0])
      var prev = stored.prev()
      prev.push(stored.id(), entry.id())

      stored.set(entry.toJSON())
        .prev(prev)

      db.put(getKey(stored), stored.toJSON(), cb)
    })
  }
}

function toObj (/* k1, v1, k2, v2... */) {
  var obj = {}
  for (var i = 0; i < arguments.length; i+=2) {
    obj[arguments[i]] = arguments[i + 1]
  }

  return obj
}

function getKey (entry) {
  var id = typeof entry === 'number' ? entry : entry.prev()[0] || entry.id()
  return lexint.pack(id, 'hex')
}
