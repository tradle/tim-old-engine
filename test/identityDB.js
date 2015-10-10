
var test = require('tape')
var extend = require('xtend/mutable')
var leveldown = require('memdown')
var collect = require('stream-collector')
var constants = require('tradle-constants')
var Log = require('logbase').Log
var Entry = require('logbase').Entry
var fakeKeeper = require('tradle-test-helpers').fakeKeeper
var createIdentityDB = require('../lib/identityDB')
var ted = require('./fixtures/ted-pub')
var EventType = require('../lib/eventType')
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH

test('identity store', function (t) {
  t.plan(7)

  var log = new Log('log.db', {
    db: leveldown
  })

  ted = extend({}, ted)
  var keeperMap = {}
  var hash = ted[ROOT_HASH] = ted[CUR_HASH] = 'abc'
  keeperMap[hash] = ted
  var entry = new Entry(extend({ type: EventType.chain.readSuccess }, ted))
  log.append(entry)
  entry.set('name', 'bill')
  log.append(entry)

  var keeper = fakeKeeper.forMap(keeperMap)
  var identities = createIdentityDB('identities.db', {
    leveldown: leveldown,
    log: log,
    keeper: keeper
  })

  identities.on('change', function (id) {
    if (id === 2) {
      identities.byFingerprint(ted.pubkeys[0].fingerprint, function (err, storedTed) {
        if (err) throw err

        t.deepEqual(storedTed.identity, ted)
      })

      testStreams()
    }
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
      t.equal(stored[0], ted[ROOT_HASH])
    })

    collect(identities.createValueStream(), function (err, stored) {
      if (err) throw err

      t.equal(stored.length, 1)
      t.equal(stored[0].identity, ted)
    })
  }
})
