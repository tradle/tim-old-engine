
var typeforce = require('typeforce')
var levelup = require('levelup')
var levelQuery = require('level-queryengine')
var sublevel = require('level-sublevel')
var jsonqueryEngine = require('jsonquery-engine')
var externr = require('externr')
var concat = require('concat-stream')
var constants = require('tradle-constants')
var safe = require('safecb')
var wipedb = require('./wipedb')
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH
var TYPE = constants.TYPE

module.exports = TypeStore

function TypeStore (options) {
  typeforce({
    path: 'String',
    leveldown: 'Function',
    type: 'String'
  }, options)

  this._type = options.type
  this._path = options.path
  this._db = levelQuery(levelup(this._path, {
    db: options.leveldown,
    valueEncoding: 'json'
  }))

  this._db.query.use(jsonqueryEngine())
  this._db.ensureIndex(CUR_HASH)

  this._sub = sublevel(this._db)
  this._sublevels = {}
  // this._byRootHash = this.createSublevel(constants.rootHash)
  // this._byCurrentHash = this.createSublevel(constants.currentHash)
  this._externs = externr({
    wrap: [ 'update', 'query' ]
  })

  this._lastUpdateId = 0
  this._defaultUpdate = this._defaultUpdate.bind(this)
  this.use = this._externs.$register.bind(this._externs)
}

TypeStore.prototype.createSublevel = function (name) {
  if (this._sublevels[name]) {
    throw new Error('this sublevel already exists')
  }

  this._sublevels[name] = true
  return this._sub.sublevel(name)
}

TypeStore.prototype.update = function (updateId, obj, cb) {
  var self = this

  typeforce('Number', updateId)
  typeforce('Object', obj)

  cb = safe(cb)

  if (obj[TYPE] !== this._type) return cb(new Error('invalid type'))
  if (this._lastUpdateId >= updateId) return cb()

  this._externs.update(this, [ obj, function (err) {
    self._lastUpdateId = updateId
    return cb(err, !err) // return true if we updated
  } ], this._defaultUpdate)
}

TypeStore.prototype.get = function (rootHash, cb) {
  return this._db.get(rootHash, cb)
}

// TypeStore.prototype.query = function (query) {
//   return Q.ninvoke(this, 'query', query)
// }

TypeStore.prototype.query = function (query, cb) {
  var self = this

  this._externs.query(this, [ query, cb ], function defQuery (query, cb) {
    self._db.query(query)
      .once('error', cb)
      .pipe(concat(function (results) {
        if (!results.length) return cb(new Error('not found'))

        cb(null, results)
      }))
  })
}

TypeStore.prototype._defaultUpdate = function (obj, cb) {
  this._db.put(obj[ROOT_HASH], obj, cb)
}

TypeStore.prototype.clear = function (cb) {
  wipedb(this._db, cb)
}

TypeStore.prototype.close = function (cb) {
  this._db.close(cb)
}
