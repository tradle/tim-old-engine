
var Q = require('q')
var debug = require('debug')('identityDB')
var typeforce = require('typeforce')
var extend = require('extend')
var map = require('map-stream')
var levelup = require('levelup')
var levelQuery = require('level-queryengine')
var jsonQueryEngine = require('jsonquery-engine')
var Identity = require('midentity').Identity
var constants = require('tradle-constants')
var TYPE = constants.TYPE
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH
var Simple = require('logbase').Simple
var rebuf = require('./rebufEncoding')

module.exports = function mkIdentityDB (path, options) {
  typeforce('String', path)
  typeforce({
    leveldown: 'Function',
    log: 'Log',
    keeper: 'Object'
  }, options)

  var log = options.log
  var db = levelup(path, {
    db: options.leveldown,
    valueEncoding: rebuf
  })

  db = Simple({
    db: db,
    log: log,
    process: processEntry
  })

  db = levelQuery(db)
  db.query.use(jsonQueryEngine())
  var main = db.sublevel('main')
  var byFingerprint = db.sublevel('fingerprint')
  var fingers = []
  var hashes = []
  var keeper = options.keeper

  db.createReadStream = function (options) {
    options = options || {}
    return main.createReadStream(options)
      .pipe(map(function (data, cb) {
        if (options.values === false) return cb(null, data)

        var id = options.keys === false ? data : data.value
        getContents(id, function (err, contents) {
          if (err) return cb(err)

          var ret = options.keys === false ?
            contents :
            {
              key: data.key,
              value: contents
            }

          cb(null, ret)
        })
      }))
  }

  db.createKeyStream = function (options) {
    return db.createReadStream(extend({ values: false }, options))
  }

  db.createValueStream = function (options) {
    return db.createReadStream(extend({ keys: false }, options))
  }

  db.byFingerprint = liveOnly(db, function (fingerprint, cb) {
    // cb = logify(cb)
    byFingerprint.get(fingerprint, function (err, rootHash) {
      if (err) return cb(err)

      return db.byRootHash(rootHash, cb)
    })
  })

  db.byRootHash = liveOnly(db, function (rootHash, cb) {
    main.get(rootHash, function (err, id) {
      if (err) return cb(err)

      getContents(id, cb)
    })
  })

  return db

  function getContents (id, cb) {
    log.get(id, function (err, entry) {
      if (err) return cb(err)

      var curHash = entry.get(CUR_HASH)
      keeper.getOne(curHash)
        .then(function (buf) {
          cb(null, toJSON(buf))
        })
        .catch(cb)
        .done()
    })
  }

  function processEntry (entry, cb) {
    if (entry.get(TYPE) !== Identity.TYPE) return cb()

    var rootHash = entry.get(ROOT_HASH)
    if (!rootHash) return cb(new Error('missing root hash ("' + ROOT_HASH + '")'))

    var curHash = entry.get(CUR_HASH)
    if (!curHash) return cb(new Error('missing current hash ("' + CUR_HASH + '")'))

    keeper.getOne(curHash)
      .then(function (buf) {
        var identityJSON = toJSON(buf)
        var batch = identityJSON.pubkeys.map(function (k) {
          fingers.push(k.fingerprint)
          return {
            type: 'put',
            key: k.fingerprint,
            value: rootHash,
            prefix: byFingerprint
          }
        })

        hashes.push(rootHash)
        batch.push({
          type: 'put',
          key: rootHash,
          value: entry.id(),
          prefix: main
        })

        return Q.ninvoke(db, 'batch', batch)
      })
      .catch(function (err) {
        if (err instanceof TypeError ||
          err instanceof ReferenceError) {
          throw err
        }

        debug('unable to get identity from keeper', err)
      })
      .done(function () {
        cb()
      })
  }
}

function toJSON (buf) {
  if (Buffer.isBuffer(buf)) buf = buf.toString('binary')
  if (typeof buf === 'string') buf = JSON.parse(buf)

  return buf
}

// function logify (cb, errsOnly) {
//   return function (err) {
//     if (errsOnly) {
//       if (err) debug(err)
//     } else {
//       debug(arguments)
//     }

//     cb(arguments[0], arguments[1], arguments[2], arguments[3])
//   }
// }

function liveOnly (db, fn, ctx) {
  return function () {
    var args = arguments
    db.onLive(function () {
      return fn.apply(ctx, args)
    })
  }
}
