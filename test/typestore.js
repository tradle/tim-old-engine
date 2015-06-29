
var test = require('tape')
var crypto = require('crypto')
var leveldown = require('memdown')
var extend = require('extend')
var TypeStore = require('../typestore')
var identityStore = require('../identityStore')
var Identity = require('midentity').Identity
var pick = require('object.pick')
var ted = require('./fixtures/ted-pub')
var ROOT_HASH = require('../constants').rootHash
var CUR_HASH = require('../constants').currentHash

test('typestore', function (t) {
  t.plan(1)

  var store = new TypeStore({
    path: 'typestore',
    leveldown: leveldown,
    type: 'blah'
  })

  var obj = {
    _type: 'blah',
    hey: 'ho'
  }

  obj[ROOT_HASH] = 'abc'
  obj[CUR_HASH] = 'bcd'

  store.update(obj)
    .then(function () {
      return store.query(pick(obj, CUR_HASH))
    })
    .then(function (results) {
      t.deepEqual(results[0], obj)
    })
    .done()

})

test('identity store', function (t) {
  t.plan(1)

  var store = identityStore({
    path: 'identitystore',
    leveldown: leveldown
  })

  var obj = extend(true, {}, ted)
  obj[ROOT_HASH] = 'abc'
  obj[CUR_HASH] = 'bcd'

  store.update(obj)
    .then(function () {

      return store.query({
        fingerprint: ted.pubkeys[0].fingerprint
      })
    })
    .then(function (identity) {
      t.deepEqual(identity, obj)
    })
    .done()
})
