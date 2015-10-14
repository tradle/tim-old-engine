
var Q = require('q')
var debug = require('debug')('identityDB')
var typeforce = require('typeforce')
var extend = require('xtend')
var map = require('map-stream')
var levelup = require('levelup')
// var levelQuery = require('level-queryengine')
// var jsonQueryEngine = require('jsonquery-engine')
var Identity = require('midentity').Identity
var constants = require('tradle-constants')
var TYPE = constants.TYPE
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH
var Simple = require('logbase').Simple
var rebuf = require('./rebufEncoding')
var EventType = require('./eventType')
var DEBUG = require('./debug')
var ENTRY_TIMEOUT = DEBUG ? false : 5000
var prefixes = {
  fingerprint: 'f'
}

prefixes[CUR_HASH] = CUR_HASH
prefixes[ROOT_HASH] = ROOT_HASH

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
    process: processEntry,
    timeout: ENTRY_TIMEOUT,
    autostart: options.autostart
  })

  // db = levelQuery(db)
  // db.query.use(jsonQueryEngine())
  // var main = db.sublevel('main')
  // var byFingerprint = db.sublevel('fingerprint')
  var fingers = []
  var hashes = []
  var keeper = options.keeper

  var createReadStream = db.createReadStream
  db.createReadStream = function (options) {
    options = options || {}
    return createReadStream.call(db, extend(options, {
        start: prefixes[ROOT_HASH],
        end: prefixes[ROOT_HASH] + '\xff'
      }))
      .pipe(map(function (data, cb) {
        if (options.keys !== false) {
          var key = options.values === false ? data : data.key
          key = unprefixKey(ROOT_HASH, key)
          if (options.values === false) data = key
          else data.key = key
        }

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

  // db.createKeyStream = function (options) {
  //   return db.createReadStream(extend({ values: false }, options))
  // }

  // db.createValueStream = function (options) {
  //   return db.createReadStream(extend({ keys: false }, options))
  // }

  db.byFingerprint = function (fingerprint, cb) {
    db.rootHashByFingerprint(fingerprint, function (err, rootHash) {
      if (err) return cb(err)

      return db.byRootHash(rootHash, cb)
    })
  }

  db.rootHashByFingerprint = db.liveOnly(function (fingerprint, cb) {
    db.get(prefixKey('fingerprint', fingerprint), cb)
  })

  db.byRootHash = db.liveOnly(function (rootHash, cb) {
    db.get(prefixKey(ROOT_HASH, rootHash), function (err, id) {
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
          var ret = {
            identity: toJSON(buf)
          }

          ret[ROOT_HASH] = entry.get(ROOT_HASH)
          ret[CUR_HASH] = curHash
          cb(null, ret)
        })
        .catch(cb)
        .done()
    })
  }

  function processEntry (entry, cb) {
    if (entry.get(TYPE) !== Identity.TYPE ||
      entry.get('type') !== EventType.chain.readSuccess) {
      return cb()
    }

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
            key: prefixKey('fingerprint', k.fingerprint),
            value: rootHash
          }
        })

        hashes.push(rootHash)
        batch.push({
          type: 'put',
          key: prefixKey(ROOT_HASH, rootHash),
          value: entry.id()
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
        debug(db.name, 'stored identity', curHash)
        cb()
      })
  }
}

function toJSON (buf) {
  if (Buffer.isBuffer(buf)) buf = buf.toString('binary')
  if (typeof buf === 'string') buf = JSON.parse(buf)

  return buf
}

function prefixKey (type, key) {
  if (!(type in prefixes)) throw new Error('no such type: ' + type)

  return prefixes[type] + '!' + key
}

function unprefixKey (type, key) {
  if (!(type in prefixes)) throw new Error('no such type: ' + type)
  if (key.indexOf(type) !== 0) throw new Error('key has wrong prefix')

  return key.slice(type.length + 1)
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

// function liveOnly (db, fn, ctx) {
//   return function () {
//     var args = arguments
//     db.onLive(function () {
//       return fn.apply(ctx, args)
//     })
//   }
// }
