
var test = require('tape')
var extend = require('extend')
var leveldown = require('memdown')
var constants = require('tradle-constants')
var Log = require('logbase').Log
var Entry = require('logbase').Entry
var fakeKeeper = require('tradle-test-helpers').fakeKeeper
var createIdentityDB = require('../identityDB')
var ted = require('./fixtures/ted-pub')
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH
var TYPE = constants.TYPE

test('identity store', function (t) {
  var log = new Log('log.db', {
    db: leveldown
  })

  var keeperMap = {}
  var hash = ted[ROOT_HASH] = ted[CUR_HASH] = 'abc'
  keeperMap[hash] = ted
  var entry = new Entry(ted)
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

        t.deepEqual(storedTed, ted)
        t.end()
      })
    }
  })
})
