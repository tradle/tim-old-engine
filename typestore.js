
var typeforce = require('typeforce')
var levelup = require('levelup')
var levelQuery = require('level-queryengine')
var sublevel = require('level-sublevel')
var jsonqueryEngine = require('jsonquery-engine')
var externr = require('externr')
var safe = require('safecb')

module.exports = TypeStore

function TypeStore (options) {
  typeforce({
    path: 'String',
    leveldown: 'Function',
    type: 'String'
  }, options)

  this._type = options.type
  this._db = levelQuery(levelup(options.path, {
    db: options.leveldown,
    valueEncoding: 'json'
  }))

  this._db.query.use(jsonqueryEngine())

  this._sub = sublevel(this._db)
  this._sublevels = {}
  this._byRootHash = this.sublevel('_r')
  this._byCurrentHash = this.sublevel('_c')
  this._externs = externr({
    wrap: [ 'update', 'query' ]
  })

  // this._fingerprintToDHTKey = iddb.sublevel('byFingerprint')
}

TypeStore.prototype.createSublevel = function (name) {
  if (this._sublevels[name]) {
    throw new Error('this sublevel already exists')
  }

  this._sublevels[name] = true
  return this._sub.sublevel(name)
}

TypeStore.prototype.update = function (obj, cb) {
  assert(obj._type === this._type)
  cb = safe(cb)
  this._externs.update(this, [ obj, cb ], this._update)
}

TypeStore.prototype.query = function (query, cb) {
  var self = this
  cb = safe(cb)

  this._externs.query(this, [ query, cb ], function default (query, cb) {
    self._db.query(query)
      .once('error', cb)
      .pipe(concat(function (results) {
        cb(null, results)
      }))
  })
}

// TypeStore.prototype.

TypeStore.prototype._update = function (obj, cb) {
  var self = this

  self._byRootHash.get(obj._r, function (err, stored) {
    if (err) {
      if (!/not found/i.test(err.message)) {
        return cb(err)
      }
    }

    if (stored) {
      stored.history.push(obj)
    } else {
      stored = {
        history: [obj]
      }
    }

    self._byRootHash.put(obj._r, stored, cb)
  })
}

TypeStore.prototype.byFingerprint = function (fingerprint, cb) {
  var self = this
  return this._fingerprintToDHTKey.get(fingerprint, function (err, dhtKey) {
    if (err) return cb(err)

    return self._byRootHash.get(fingerprint, cb)
  })
}
