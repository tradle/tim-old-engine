
var test = require('tape')
var leveldown = require('memdown')
var extend = require('extend')
var TypeStore = require('../typestore')
var identityStore = require('../identityStore')
var pick = require('object.pick')
var constants = require('tradle-constants')
var ted = require('./fixtures/ted-pub')
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH
var TYPE = constants.TYPE

test('typestore', function (t) {
  var num = 3
  // t.plan(2 + num * 4)

  var type = 'blah'
  var store = new TypeStore({
    path: 'typestore',
    leveldown: leveldown,
    type: type
  })

  var obj = {
    hey: 'ho'
  }

  obj[TYPE] = type
  obj[ROOT_HASH] = 'abc'
  obj[CUR_HASH] = 'bcd'

  function update (n, expectUpdated, cb) {
    store.update(n, obj, function (err, updated) {
      if (err) throw err

      t.equal(!!updated, expectUpdated)
      store.query(pick(obj, CUR_HASH), function (err, results) {
        if (err) throw err

        t.deepEqual(results[0], obj)
        if (cb) cb()
      })
    })
  }

  var first = num
  update(first, true, function () {
    for (var i = 0; i < num * 2; i++) {
      update(i, i > first, i === num * 2 - 1 ? t.end : null)
    }
  })
})

test('identity store', function (t) {
  var store = identityStore({
    path: 'identitystore',
    leveldown: leveldown
  })

  var obj = extend(true, {}, ted)
  obj[ROOT_HASH] = 'abc'
  obj[CUR_HASH] = 'bcd'

  store.update(1, obj, function (err) {
    if (err) throw err

    store.query({
      fingerprint: ted.pubkeys[0].fingerprint
    }, function (err, identities) {
      if (err) throw err

      t.deepEqual(identities[0], obj)
      t.end()
    })
  })
})
