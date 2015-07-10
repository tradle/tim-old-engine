
var test = require('tape')
var extend = require('extend')
var leveldown = require('memdown')
var constants = require('tradle-constants')
var Log = require('logbase').Log
var Entry = require('logbase').Entry
var identityDB = require('../identityDB')
var ted = require('./fixtures/ted-pub')
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH
var TYPE = constants.TYPE

test('identity store', function (t) {
  var log = new Log('log.db', {
    db: leveldown
  })

  ted[ROOT_HASH] = 'abc'
  var entry = new Entry()
    .set(ted)

  log.append(entry)
  var originalName = ted.name
  ted.name = 'bill'
  entry.set(ted)
  log.append(entry)

  var identities = identityDB('identities.db', {
    db: leveldown,
    log: log
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
