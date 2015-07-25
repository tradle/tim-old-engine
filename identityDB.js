var debug = require('debug')('identityDB')
var typeforce = require('typeforce')
var levelup = require('levelup')
var levelQuery = require('level-queryengine')
var jsonQueryEngine = require('jsonquery-engine')
var Identity = require('midentity').Identity
var constants = require('tradle-constants')
var TYPE = constants.TYPE
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH
var Simple = require('logbase').Simple

module.exports = function mkIdentityDB (path, options) {
  typeforce('String', path)
  typeforce({
    leveldown: 'Function',
    log: 'Log',
    keeper: 'Object'
  }, options)

  var log = options.log
  var db = levelup(options.path, {
    db: options.leveldown,
    valueEncoding: 'json'
  })

  db = Simple(db, log, processEntry)
  db = levelQuery(db)
  db.query.use(jsonQueryEngine())
  var main = db.sublevel('main')
  var byFingerprint = db.sublevel('fingerprint')
  var fingers = []
  var hashes = []
  var keeper = options.keeper

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
    })
  })

  return db

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

        db.batch(batch, cb)
      })
      .catch(function (err) {
        if (err instanceof TypeError ||
          err instanceof ReferenceError) {
          throw err
        }

        debug('unable to get identity from keeper', err)
        cb()
      })
      .done()
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
