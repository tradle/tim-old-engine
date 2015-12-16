
var test = require('tape')
var extend = require('xtend')
var leveldown = require('memdown')
var Q = require('q')
var collect = require('stream-collector')
var constants = require('@tradle/constants')
var Log = require('logbase').Log
var Entry = require('logbase').Entry
var fakeKeeper = require('@tradle/test-helpers').fakeKeeper
var createIdentityDB = require('../lib/identityDB')
var ted = require('./fixtures/ted-pub')
var EventType = require('../lib/eventType')
var utils = require('../lib/utils')
var TYPE = constants.TYPE
var ROOT_HASH = constants.ROOT_HASH
var PREV_HASH = constants.PREV_HASH
var CUR_HASH = constants.CUR_HASH
var IDENTITY_TYPE = constants.TYPES.IDENTITY
var dbCounter = 0
var nextDBName = function () {
  return 'db' + (dbCounter++)
}

test('ignore identities that collide on keys', function (t) {
  t.plan(ted.pubkeys.length)

  var log = new Log(nextDBName(), {
    db: leveldown
  })

  ted = extend(ted) // defensive copy
  var badPerson = extend(ted, { name: 'evil ted' })
  var keeperMap = {}
  var tedHash = 'abc'
  var badPersonHash = 'efg'
  keeperMap[tedHash] = ted
  keeperMap[badPersonHash] = badPerson

  var tedFromChain = new Entry({
      type: EventType.chain.readSuccess
    })
    .set(TYPE, IDENTITY_TYPE)
    .set(CUR_HASH, tedHash)
    .set(ROOT_HASH, tedHash)

  var badPersonFromChain = new Entry({
      type: EventType.chain.readSuccess
    })
    .set(TYPE, IDENTITY_TYPE)
    .set(CUR_HASH, badPersonHash)
    .set(ROOT_HASH, badPersonHash)

  log.append(tedFromChain)
  log.append(badPersonFromChain)

  var keeper = fakeKeeper.forMap(keeperMap)
  var identities = createIdentityDB(nextDBName(), {
    leveldown: leveldown,
    log: log,
    keeper: keeper
  })

  identities.onLive(function () {
    Q.all(ted.pubkeys.map(function (k) {
        return Q.ninvoke(identities, 'byFingerprint', k.fingerprint)
      }))
      .done(function (identities) {
        identities.forEach(function (i) {
          t.deepEqual(i.identity.name, ted.name)
        })
      })
  })
})

test('update identity', function (t) {
  t.plan(7)

  var log = new Log(nextDBName(), {
    db: leveldown
  })

  ted = extend(ted)

  var keeperMap = {}
  var originalHash = 'abc'
  var entry = new Entry({
      type: EventType.chain.readSuccess
    })
    .set(TYPE, IDENTITY_TYPE)
    .set(CUR_HASH, originalHash)
    .set(ROOT_HASH, originalHash)

  keeperMap[originalHash] = ted
  log.append(entry)

  ted = extend(ted)
  ted.name = 'ted!'

  var updateHash = 'abc1'
  var update = new Entry(entry.toJSON())
    .set(CUR_HASH, updateHash)
    .set(PREV_HASH, originalHash)

  keeperMap[updateHash] = ted
  log.append(update)

  var keeper = fakeKeeper.forMap(keeperMap)
  var identities = createIdentityDB(nextDBName(), {
    leveldown: leveldown,
    log: log,
    keeper: keeper
  })

  identities.on('change', function (id) {
    if (id !== 2) return

    identities.byFingerprint(ted.pubkeys[0].fingerprint, function (err, storedTed) {
      if (err) {
        t.error(err)
        throw err
      }

      t.deepEqual(storedTed.identity, ted)
    })

    testStreams()
  })

  function testStreams () {
    collect(identities.createReadStream(), function (err, stored) {
      if (err) throw err

      t.equal(stored.length, 1)
      t.deepEqual(stored[0].value.identity, ted)
    })

    collect(identities.createKeyStream(), function (err, stored) {
      if (err) throw err

      t.equal(stored.length, 1)
      t.equal(stored[0], originalHash)
    })

    collect(identities.createValueStream(), function (err, stored) {
      if (err) throw err

      t.equal(stored.length, 1)
      t.equal(stored[0].identity, ted)
    })
  }
})
