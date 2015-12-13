
var debug = require('debug')('identityDB')
var typeforce = require('typeforce')
var extend = require('xtend')
var map = require('map-stream')
var levelup = require('levelup')
var Identity = require('@tradle/identity').Identity
var constants = require('@tradle/constants')
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
    timeout: typeof options.timeout === 'undefined' ? ENTRY_TIMEOUT : options.timeout,
    autostart: options.autostart
  })

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

  db.query = function (query, cb) {
    // by one and only one prop
    if (Object.keys(query).length !== 1 ||
      !(query.fingerprint || query[ROOT_HASH] || query[CUR_HASH])) {
      return process.nextTick(function () {
        cb(new Error('specify "fingerprint", ' + ROOT_HASH + ' OR ' + CUR_HASH))
      })
    }

    if (query.fingerprint) {
      db.byFingerprint(query.fingerprint, cb)
    } else if (query[ROOT_HASH]) {
      db.byRootHash(query[ROOT_HASH], cb)
    } else if (query[CUR_HASH]) {
      db.byCurHash(query[CUR_HASH], cb)
    }
  }

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

  db.byCurHash = db.liveOnly(function (curHash, cb) {
    db.get(prefixKey(CUR_HASH, curHash), function (err, id) {
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
      (entry.get('type') !== EventType.chain.readSuccess &&
      entry.get('type') !== EventType.misc.addIdentity)) {
      return cb()
    }

    var rootHash = entry.get(ROOT_HASH)
    if (!rootHash) throw new Error('missing root hash ("' + ROOT_HASH + '")')

    var curHash = entry.get(CUR_HASH)
    if (!curHash) throw new Error('missing current hash ("' + CUR_HASH + '")')

    db.get(prefixKey(CUR_HASH, curHash), function (err, val) {
      if (err){
        if (!err.notFound) throw new Error('database choked')

        save()
      } else {
        cb()
      }
    })

    function save () {
      keeper.getOne(curHash)
        .then(function (buf) {
          var identityJSON = toJSON(buf)
          var batch = identityJSON.pubkeys.map(function (k) {
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
          }, {
            type: 'put',
            key: prefixKey(CUR_HASH, curHash),
            value: entry.id()
          })

          debug(db.name, 'stored identity', curHash)
          cb(batch)
          // return Q.ninvoke(db, 'batch', batch)
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
