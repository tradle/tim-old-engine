
var assert = require('assert')
var typeforce = require('typeforce')
var Q = require('q')
var levelup = require('levelup')
var levelQuery = require('level-queryengine')
var sublevel = require('level-sublevel')
var jsonqueryEngine = require('jsonquery-engine')
var externr = require('externr')
var concat = require('concat-stream')
var pick = require('object.pick')
var constants = require('tradle-constants')
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
  this._db = levelQuery(levelup(options.path, {
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
    wrap: [ '_update', '_query' ]
  })

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

TypeStore.prototype.update = function (obj) {
  return Q.ninvoke(this, '_update', obj)
}

TypeStore.prototype._update = function (obj, cb) {
  assert.equal(obj[TYPE], this._type)
  this._externs._update(this, [ obj, cb ], this._defaultUpdate)
}

TypeStore.prototype.get = function (rootHash, cb) {
  return this._db.get(rootHash, cb)
}

TypeStore.prototype.query = function (query) {
  return Q.ninvoke(this, '_query', query)
}

TypeStore.prototype._query = function (query, cb) {
  var self = this

  this._externs._query(this, [ query, cb ], function defQuery (query, cb) {
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
