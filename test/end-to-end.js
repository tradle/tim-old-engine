var mockery = require('mockery')
mockery.enable({
  warnOnReplace: false,
  warnOnUnregistered: false
})

mockery.registerSubstitute('q', 'bluebird-q')

if (process.env.MULTIPLEX) {
  console.log('multiplex over UTP')
  require('@tradle/multiplex-utp')
}

var path = require('path')
var test = require('tape-extra')
var rimraf = require('rimraf')
var typeforce = require('typeforce')
var find = require('array-find')
var pick = require('object.pick')
var crypto = require('crypto')
var express = require('express')
var extend = require('xtend')
var memdown = require('memdown')
var collect = require('stream-collector')
var map = require('map-stream')
var safe = require('safecb')
var Q = require('q')
Q.onerror = function (e) {
  console.error(e)
  throw e
}

var DHT = require('@tradle/bittorrent-dht')
// var Keeper = require('bitkeeper-js')
var Zlorp = require('zlorp')
Zlorp.ANNOUNCE_INTERVAL = Zlorp.LOOKUP_INTERVAL = 5000
var P2PTransport = require('@tradle/transport-p2p')
var ChainedObj = require('@tradle/chained-obj')
var Builder = ChainedObj.Builder
var kiki = require('@tradle/kiki')
var toKey = kiki.toKey
var ChainLoaderErrs = require('@tradle/chainloader').Errors
var ChainRequest = require('@tradle/bitjoe-js/lib/requests/chain')
var CreateRequest = require('@tradle/bitjoe-js/lib/requests/create')
var origGenerateSymmetricKey = CreateRequest.prototype._generateSymmetricKey
var unhackCreateRequest = function () {
  CreateRequest.prototype._generateSymmetricKey = origGenerateSymmetricKey
}

var hackCreateRequest = function () {
  CreateRequest.prototype._generateSymmetricKey = function () {
    return Q(new Buffer('1111111111111111111111111111111111111111111111111111111111111111', 'hex'))
  }
}

var tradleUtils = require('@tradle/utils')
var encryptAsync = tradleUtils.encryptAsync
var TEST_IV = new Buffer('f5bc75d07a12c86b581c5719e05e9af4', 'hex')
tradleUtils.encryptAsync = function (opts, cb) {
  opts.iv = TEST_IV
  return encryptAsync.call(tradleUtils, opts, cb)
}

var Identity = require('@tradle/identity').Identity
var billPub = require('./fixtures/bill-pub')
var billPriv = require('./fixtures/bill-priv')
var bill = Identity.fromJSON(billPub)
var tedPub = require('./fixtures/ted-pub')
var tedPriv = require('./fixtures/ted-priv')
var ted = Identity.fromJSON(tedPub)
var rufusPub = require('./fixtures/rufus-pub')
var rufusPriv = require('./fixtures/rufus-priv')
var rufus = Identity.fromJSON(rufusPub)
var constants = require('@tradle/constants')
// var testDrivers = require('./helpers/testDriver')
var BASE_PORT = 33333
var billPort = BASE_PORT++
var tedPort = BASE_PORT++
var rufusPort = BASE_PORT++
var bootstrapDHT
var BOOTSTRAP_DHT_PORT = BASE_PORT++
var TYPE = constants.TYPE
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH
var PREV_HASH = constants.PREV_HASH
var NONCE = constants.NONCE
var SIG = constants.SIG
var STORAGE_DIR = path.resolve('./storage')
// var tedHash = tedPub[ROOT_HASH] = 'c67905793f6cc0f0ab8d20aecfec441932ffb13d'
// var billHash = billPub[ROOT_HASH] ='fb07729c0cef307ab7c28cb76088cc60dbc98cdd'
// var tedHash = 'c67905793f6cc0f0ab8d20aecfec441932ffb13d'
// var billHash = 'fb07729c0cef307ab7c28cb76088cc60dbc98cdd'
var help = require('@tradle/test-helpers')
// var fakeKeeper = help.fakeKeeper
var fakeWallet = help.fakeWallet
var FakeKeeper = help.fakeKeeper
// var bill = Identity.fromJSON(billPriv)
// var ted = Identity.fromJSON(tedPriv)
var NETWORK_NAME = 'testnet'
// var blockchain = new Fakechain({ networkName: NETWORK_NAME })
var Driver = require('../')
Driver.CATCH_UP_INTERVAL = 1000
// Driver.SEND_THROTTLE = 1000
// Driver.CHAIN_WRITE_THROTTLE = 1000
// Driver.CHAIN_READ_THROTTLE = 1000
var utils = require('../lib/utils')
var Transport = require('@tradle/transport-http')
var Errors = require('../lib/errors')
Errors.MAX_RESEND = 5
Errors.MAX_CHAIN = 5
Errors.MAX_UNCHAIN = 3
// Driver.enableOptimizations() // can't enable because cache-sharing between instances fakes test results
// var TimeMethod = require('time-method')
// var timTimer = TimeMethod(Driver.prototype)
// for (var p in Driver.prototype) {
//   if (typeof Driver.prototype[p] === 'function') {
//     timTimer.time(p)
//   }
// }

var noop = function () {}

var CONFIGS = {
  bill: {
    identity: bill,
    identityJSON: billPub,
    keys: billPriv,
    port: billPort
  },

  ted: {
    identity: ted,
    identityJSON: tedPub,
    keys: tedPriv,
    port: tedPort
  },

  rufus: {
    identity: rufus,
    identityJSON: rufusPub,
    keys: rufusPriv,
    port: rufusPort
  }
}

var driverBill
var driverTed
var driverRufus
var reinitCount = 0
var nonce
var testTimerName = 'test took'
var sharedKeeper
test.beforeEach = function (cb) {
  nonce = 1
  sharedKeeper = FakeKeeper.empty()
  init(function () {
    console.time(testTimerName)
    cb()
  })
}

test.afterEach = function (cb) {
  teardown(function () {
    console.timeEnd(testTimerName)
    cb()
  })
}

rimraf.sync(STORAGE_DIR)

test('unchained-self event', function (t) {
  t.plan(1)
  t.timeoutAfter(5000)
  driverBill.on('unchained-self', function (info) {
    t.equal(info[CUR_HASH], driverBill.myCurrentHash())
  })

  publishIdentities([driverBill])
})

test('msgDB', function (t) {
  t.plan(6)

  var people = [driverBill, driverTed, driverRufus]
  var me = driverBill
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    people.forEach(function (a) {
      var myRootHash = me.myRootHash()
      var myCurrentHash = me.myCurrentHash()
      Q.ninvoke(me.messages(), 'byRootHash', myRootHash)
        .done(function (msgs) {
          t.equal(msgs[0][ROOT_HASH], myRootHash)
        })

      Q.ninvoke(me.messages(), 'byCurHash', myCurrentHash)
        .done(function (msg) {
          t.equal(msg[CUR_HASH], myCurrentHash)
        })
    })
  })
})

test('typeDB', function (t) {
  var msgs = [
    { [TYPE]: 'a', blah: 1 },
    { [TYPE]: 'b', blah: 2 },
    { [TYPE]: 'c', blah: 3 },
    { [TYPE]: 'a', blah: 4 },
  ].map(toMsg)

  var togo = 4 // 4 msgs to deliver

  t.plan(6) // 2 people * 3 types

  var people = [driverBill, driverTed]
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    // make sure all the combinations work
    // make it easier to check by sending settings as messages
    msgs.forEach(function (msg) {
      driverTed.send({
        msg: msg,
        to: [{
          fingerprint: billPub.pubkeys[0].fingerprint
        }],
        deliver: true
      })
      .done()
    })

    driverBill.on('message', function (msg) {
      if (--togo) return

      people.forEach(function (driver) {
        ;['a', 'b', 'c'].forEach(function (type) {
          collect(driver.byType(type), function (err, results) {
            if (err) throw err

            results = results.map(function (r) {
              return r.parsed.data
            })
            .sort(function (a, b) {
              return a.blah - b.blah
            })

            const expected = msgs.filter(function (msg) {
              return msg[TYPE] === type
            })

            t.same(results, expected)
          })
        })
      })
    })
  })
})

test('export history', function (t) {
  t.timeoutAfter(20000)
  var people = [driverBill, driverTed, driverRufus]
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    var toTed = toMsg({ to: 'ted' })
    var toBill = toMsg({ to: 'bill' })

    driverBill.send({
      msg: toTed,
      deliver: true,
      to: [getIdentifier(tedPub)]
    })
    .then(function () {
      return driverRufus.send({
        msg: toBill,
        deliver: true,
        to: [getIdentifier(billPub)]
      })
    })
    .done()

    var togo = 2
    driverBill.on('message', oneDown)
    driverTed.on('message', oneDown)

    function oneDown (info) {
      if (--togo) return

      Q.all([
        driverBill.history(),
        driverTed.history()
      ])
      .spread(function (billHist, tedHist) {
        t.equal(billHist.length, 5) // 3 identities + 2 msgs
        billHist.sort(byTimestampAsc)

        t.deepEqual(billHist.pop().parsed.data, toBill)
        t.deepEqual(billHist.pop().parsed.data, toTed)

        t.equal(tedHist.length, 4) // 3 identities + 1 msg
        tedHist.sort(byTimestampAsc)

        t.deepEqual(tedHist.pop().parsed.data, toTed)
        return Q.all([
          driverBill.history(driverTed.myRootHash()),
          driverTed.history(driverBill.myRootHash())
        ])
      })
      .spread(function (billHist, tedHist) {
        t.equal(billHist.length, 1)
        t.deepEqual(billHist.pop().parsed.data, toTed)

        t.equal(tedHist.length, 1)
        t.deepEqual(tedHist.pop().parsed.data, toTed)
        t.end()
      })
      .done()
    }
  })
})

test('pause/unpause', function (t) {
  t.timeoutAfter(20000)
  var timesPaused = 0
  monitorIdentityPublishAddr(driverBill, driverTed)
  driverBill.on('pause', function () {
    t.equal(++timesPaused, 1)
  })

  driverBill.pause()
  driverBill.pause() // no double 'pause' events
  driverTed.pause()
  driverBill.publishMyIdentity()
  driverTed.on('unchained', t.fail)

  setTimeout(function () {
    t.ok(driverBill.isPaused())
    t.ok(driverTed.isPaused())
    driverTed.removeListener('unchained', t.fail)
    var timesResumed = 0
    driverBill.on('resume', function () {
      t.equal(++timesResumed, 1)
    })

    driverBill.resume()
    t.notOk(driverBill.isPaused())
    driverBill.resume() // no double 'resume' events
    driverTed.resume()
    driverTed.on('unchained', function () {
      t.pass()
      t.end()
    })
  }, 5000)
})

test('resending & order guarantees', function (t) {
  t.timeoutAfter(20000)
  hackCreateRequest()
  var people = [driverBill, driverTed]
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    var msgs = [
      {
        succeedAfter: Errors.MAX_RESEND - 1
      },
      {
        succeedAfter: 0
      },
      {
        succeedAfter: 0
      },
      {
        succeedAfter: 0
      },
      {
        succeedAfter: 0
      }
    ].map(function (msg) {
      msg[NONCE] = '' + (nonce++)
      return msg
    })

    var encrypted = [
      "9bx10HoSyGtYHFcZ4F6a9E6MuS1bw2i7UxRICFM0elWmkPjEdxWdz8XQRg==",
      "9bx10HoSyGtYHFcZ4F6a9E6MuS1bw2i4UxRICFM0elWmkPjEdxWdz8XURg==",
      "9bx10HoSyGtYHFcZ4F6a9E6MuS1bw2i5UxRICFM0elWmkPjEdxWdz8XURg==",
      "9bx10HoSyGtYHFcZ4F6a9E6MuS1bw2i+UxRICFM0elWmkPjEdxWdz8XURg==",
      "9bx10HoSyGtYHFcZ4F6a9E6MuS1bw2i/UxRICFM0elWmkPjEdxWdz8XURg=="
    ]

    var copy = msgs.map(function (m) {
      return extend(m)
    })

    var z = CONFIGS.ted.state.driver
    var send = z._send
    z._send = function (rh, msg) {
      var eData = JSON.parse(msg).encryptedData
      var idx = encrypted.indexOf(eData)
      var decrypted = copy[idx]
      if (decrypted.succeedAfter < msgs[idx].succeedAfter) {
        t.pass('resending')
      }

      if (decrypted.succeedAfter-- > 0) {
        // should resend after this
        return Q.reject(new Error('failed to send'))
      } else {
        return send.apply(z, arguments)
      }
    }

    Q.all(msgs.map(function (msg) {
        return driverTed.send({
          msg: msg,
          deliver: true,
          to: [{
            fingerprint: billPub.pubkeys[0].fingerprint
          }]
        })
      }))
      .then(function () {
        return driverTed.getSendQueue()
      })
      .then(function (queued) {
        t.equal(queued.length, 5)
      })


    var togo = msgs.length * 2 // send + receive
    var received = 0
    driverBill.on('message', next.bind(null, 'received'))
    driverTed.on('sent', next.bind(null, 'sent'))
    driverBill.on('message', function (info) {
      driverBill.lookupObject(info)
        .done(function (obj) {
          if (msgs[received].succeedAfter) {
            t.pass('msg resent after failed send')
          }

          t.deepEqual(obj.parsed.data, msgs[received])
          received++
        })
    })

    function next (event) {
      // console.log(event)
      if (--togo) return

      killAndRessurrect('ted')
        .then(function () {
          return Q.nfcall(collect, driverTed.messages().getToSendStream({ tail: false }))
        })
        .then(function (results) {
          // msg should not be queued for resend
          t.equal(results.length, 0, 'msg not requeued after succesfully sent')
          t.end()
          unhackCreateRequest()
        })
        .done()
    }
  })
})

test('give up sending after max retries', function (t) {
  t.timeoutAfter(15000)
  var people = [driverBill, driverTed]
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    var msg = toMsg({ hey: 'ho' })
    var tries = 0
    driverBill._send = function (rh, msg) {
      tries++
      t.ok(tries <= Errors.MAX_RESEND)
      if (tries === Errors.MAX_RESEND) {
        setTimeout(t.end, driverBill.sendThrottle * 3)
      }

      return Q.reject(new Error('failed to send'))
    }

    driverBill.send({
      msg: msg,
      to: [{
        fingerprint: tedPub.pubkeys[0].fingerprint
      }],
      deliver: true
    })
    .done()
  })
})

test('give up chaining after max retries', function (t) {
  // t.timeoutAfter(15000)
  var people = [driverBill, driverTed]
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    driverBill.on('error', noop)
    var msg = toMsg({ hey: 'ho' })
    var tries = 0
    var execute = ChainRequest.prototype.execute
    // hack ChainRequest to fail
    ChainRequest.prototype.execute = function () {
      tries++
      t.ok(tries <= Errors.MAX_CHAIN)
      if (tries === Errors.MAX_CHAIN) {
        setTimeout(function () {
          // unhack ChainRequest
          ChainRequest.prototype.execute = execute
          t.end()
        }, driverBill.chainThrottle * 3)
      }

      return Q.reject(new Error('failed to chain'))
    }

    driverBill.send({
      msg: msg,
      to: [{
        fingerprint: tedPub.pubkeys[0].fingerprint
      }],
      chain: true
    })
    .then(function () {
      return driverBill.getChainQueue()
    })
    .then(function (queued) {
      t.equal(queued.length, 1)
    })
    .done()
  })
})

test('give up unchaining after max retries', function (t) {
  t.timeoutAfter(15000)
  var people = [driverBill, driverTed]
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    driverBill.on('error', noop)
    var msg = toMsg({ hey: 'ho' })
    var tries = 0

    driverBill.send({
        msg: msg,
        to: [{
          fingerprint: tedPub.pubkeys[0].fingerprint
        }],
        chain: true
      })
      .done()

    driverBill.once('chained', function (info) {
      var txId = info.txId
      var load = driverBill.chainloader.load
      driverTed.chainloader.load = function (txInfo) {
        if (txInfo.txId !== txId) {
          return load.call(driverTed.chainloader, hash)
        }

        t.ok(++tries <= Errors.MAX_UNCHAIN)
        if (tries === Errors.MAX_UNCHAIN) {
          setTimeout(t.end, driverTed.syncInterval * 3)
        }

        return Q.reject(new ChainLoaderErrs.FileNotFound(new Error('not found')))
      }
    })
  })
})

test('the reader and the writer', function (t) {
  t.timeoutAfter(20000)

  // die rufus! see you in the next test
  driverRufus.destroy()

  var togo = 4
  var reader = driverBill
  reader.readOnly = true

  var readerCoords = [getIdentifier(reader.identityJSON)]
  var writer = driverTed
  var writerCoords = [getIdentifier(writer.identityJSON)]
  // publish to constants.IDENTITY_PUBLISH_ADDRESS
  writer.publishMyIdentity().done()

  monitorIdentityPublishAddr(reader, writer)

  // publish reader's identity for them
  writer.addContactIdentity(reader.identityJSON)
    .then(function () {
      var readerAddr = findBitcoinKey(
        reader.identityJSON.pubkeys,
        Driver.BLOCKCHAIN_KEY_PURPOSE
      ).fingerprint

      return writer.publishIdentity(reader.identityJSON, readerAddr)
    })
    .done()

  reader.on('unchained', onUnchainedIdentity.bind(reader))
  writer.on('unchained', onUnchainedIdentity.bind(writer))
  var msg = toMsg({
    hey: 'ho'
  })

  var signed
  // writer.on('unchained', console.log)
  writer.once('message', function (info) {
    writer.chainExisting(info.uid)
    reader.once('unchained', function (info) {
      t.equal(info.from[ROOT_HASH], reader.myRootHash())
      reader.lookupObject(info)
        .done(function (obj) {
          t.deepEqual(obj.data, signed)
          t.end()
        })
    })
  })

  function onUnchainedIdentity (info) {
    if (--togo) return

    reader.removeListener('unchained', onUnchainedIdentity)
    writer.removeListener('unchained', onUnchainedIdentity)

    reader.identityPublishStatus()
      .then(function (status) {
        t.ok(status.ever)
        t.ok(status.current)
        t.ok(status.txId)
        return reader.sign(msg)
      })
      .then(function (_signed){
        signed = _signed
        return reader.send({
          chain: false,
          deliver: true,
          msg: signed,
          to: writerCoords
        })
      })
      .done()
  }
})

test('wipe dbs, get publish status on reload', function (t) {
  t.plan(4)
  t.timeoutAfter(20000)

  Q.nfcall(publishIdentities, [driverBill])
    .then(driverBill.destroy)
    .then(function () {
      return Q.all(['messages', 'addressBook', 'txs'].map(function (dbName) {
        dbName = 'bill' + reinitCount + '-' + dbName + '.db'
        return Q.ninvoke(memdown, 'destroy', dbName)
      }))
    })
    .then(function () {
      return killAndRessurrect('bill')
    })
    .then(function () {
      driverBill.once('unchained', function (info) {
        t.equal(info[TYPE], constants.TYPES.IDENTITY)
      })

      return driverBill.identityPublishStatus()
    })
    .done(function (status) {
      t.ok(status.ever)
      t.ok(status.current)
      t.notOk(status.queued)
    })
})

test('no chaining in readOnly mode', function (t) {
  driverTed.readOnly = true
  var msg = toMsg({ blah: 'yo' })
  t.throws(function () {
    driverTed.send({
      msg: msg,
      to: [{
        fingerprint: billPub.pubkeys[0].fingerprint
      }],
      chain: true
    })
  })

  // for now
  // TODO: close dbs safely when they're closed before being fully open
  setTimeout(t.end, 300)
})

test('no chaining attempted if low balance', function (t) {
  t.plan(2)
  driverBill.wallet.balance = function (cb) {
    process.nextTick(function () {
      cb(null, 1000)
    })
  }

  driverBill._updateBalance()
    .done(function () {
      driverBill.publishMyIdentity().done()
      driverBill.on('chaining', t.fail)
      driverBill.once('lowbalance', function () {
        t.pass()
        setTimeout(t.pass, 1000)
      })
    })
})

test('delivered/chained/both', function (t) {
  t.timeoutAfter(20000)

  var people = [driverBill, driverTed]
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    // make sure all the combinations work
    // make it easier to check by sending settings as messages
    var msgs = [
      { chain: false, deliver: true },
      { chain: true, deliver: true },
      // will never get unchained, because message body
      // has no way of getting to recipient
      { chain: true, deliver: false },
    ].map(toMsg)

    var togo = {
      message: 2,
      unchained: 1
    }

    msgs.forEach(function (msg) {
      driverTed.send(extend({
        msg: msg,
        to: [{
          fingerprint: billPub.pubkeys[0].fingerprint
        }]
      }, msg))
      .done()
    })

    Object.keys(togo).forEach(function (event) {
      driverBill.on(event, function (info) {
        togo[event]--
        if (!togo[event]) delete togo[event]
        driverBill.lookupObject(info)
          .done(function (chainedObj) {
            if (event === 'message') {
              t.deepEqual(chainedObj.parsed.data.deliver, true)
            } else {
              t.deepEqual(chainedObj.parsed.data.chain, true)
            }

            if (!Object.keys(togo).length) t.end()
          })
      })
    })
  })
})

test('handle non-data tx', function (t) {
  t.plan(3)
  driverBill.wallet.send()
    .to(driverTed.wallet.addressString, 10000)
    .execute()

  driverBill.on('error', rethrow)
  driverTed.on('error', rethrow)
  var stream = driverBill.transactions()
    .liveStream({
      old: true,
      tail: true
    })
    .on('data', function (data) {
      stream.destroy()
      var tx = data.value
      t.notOk('txType' in tx)
      t.notOk('txData' in tx)
      setTimeout(t.pass, 1000)
    })
})

test('self publish, edit, republish', function (t) {
  var people = [driverBill, driverTed]
  monitorIdentityPublishAddr(people)
  publish(function () {
    failToRepeatPublish(function () {
      republish(function () {
        readIdentities()
      })
    })
  })

  var identitiesChecked = 0

  function publish (next) {
    var togo = 2
    driverBill.publishMyIdentity().done()
    ;[driverBill, driverTed].forEach(function (driver) {
      driver.once('unchained', function (info) {
        driver.lookupObject(info)
          .done(function (chainedObj) {
            t.deepEqual(chainedObj.parsed.data, driverBill.identityJSON)
            if (--togo === 0) next()
          })
      })
    })
  }

  function failToRepeatPublish (next) {
    driverTed.on('unchained', t.fail)
    driverBill.publishMyIdentity()
      .then(t.fail)
      .catch(t.pass)
      .done(function () {
        driverTed.removeListener('unchained', t.fail)
        next()
      })

    t.throws(function () {
      // can't publish twice simultaneously
      driverBill.publishMyIdentity()
    })
  }

  function republish (next) {
    var newBill = extend({}, driverBill.identityJSON)
    newBill.name = { firstName: 'Bill 2' }
    newBill[NONCE] = '232'
    driverBill.setIdentity(newBill)
    driverBill.publishMyIdentity()
    driverTed.once('unchained', function (info) {
      driverTed.lookupObject(info)
        .done(function (chainedObj) {
          console.log('Ted unchained latest Bill')
          var loaded = chainedObj.parsed.data
          delete loaded[SIG]
          t.equal(loaded[PREV_HASH], driverBill.identityMeta[PREV_HASH])
          t.equal(loaded[ROOT_HASH], driverBill.identityMeta[ROOT_HASH])
          t.deepEqual(loaded, driverBill.identityJSON)
          next()
        })
    })
  }

  function readIdentities () {
    var bStream = driverBill.identities().createValueStream()
    var tStream = driverTed.identities().createValueStream()
    collect(bStream, checkIdentities.bind(driverBill))
    collect(tStream, checkIdentities.bind(driverTed))
  }

  function checkIdentities (err, results) {
    if (err) throw err

    t.equal(results.length, 1)
    results.forEach(function (r) {
      delete r.identity[SIG]
      t.deepEqual(r.identity, driverBill.identityJSON)
    })

    if (++identitiesChecked === 2) {
      t.end()
    }
  }
})

test('throttle chaining', function (t) {
  t.plan(3)
  t.timeoutAfter(10000)

  var blockchain = driverBill.blockchain
  var propagate = blockchain.transactions.propagate
  var firstErrTime
  blockchain.transactions.propagate = function (txs, cb) {
    cb(new Error('this is a test error'))

    if (!firstErrTime) {
      firstErrTime = utils.now()
    } else {
      blockchain.transactions.propagate = propagate
      t.ok(utils.now() - firstErrTime > driverBill.chainThrottle * 0.8) // fuzzy
    }
  }

  var msg = { blah: 'yo' }
  msg[NONCE] = '123'
  driverBill.publish({
      msg: msg,
      to: [{ fingerprint: driverTed.wallet.addressString }]
    })
    .done()

  driverBill.on('error', function (err) {
    t.ok(/test error/.test(err.message))
  })
})

test('delivery check', function (t) {
  t.plan(2)
  t.timeoutAfter(60000)
  var people = [driverBill, driverTed]
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    var billCoords = getIdentifier(billPub)
    var msg = toMsg({ hey: 'blah' })

    driverTed.send({
      msg: msg,
      to: [billCoords],
      deliver: true,
      chain: false
    }).done()

    driverTed.on('sent', checkReceived)
    driverTed.on('chained', t.fail)
    driverTed.on('unchained', t.fail)
    driverBill.on('unchained', t.fail)

    killAndRessurrect('bill')
      .done(function () {
        driverBill.on('message', checkReceived)
        driverBill.on('unchained', t.fail)
      })

    function checkReceived (info) {
      driverBill.lookupObject(info)
        .done(function (chainedObj) {
          t.deepEqual(chainedObj.parsed.data, msg)
        })
    }
  })
})

test('share chained content with 3rd party', function (t) {
  t.plan(5)
  t.timeoutAfter(60000)

  var people = [driverBill, driverTed, driverRufus]
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    // make sure all the combinations work
    // make it easier to check by sending settings as messages
    var msgs = [
      { chain: false, deliver: true },
      { chain: true, deliver: true }
    ].map(function (m) {
      m[TYPE] = 'message'
      return m
    }).map(toMsg)

    // send all msgs to ted
    msgs.forEach(function (msg) {
      driverTed.send(extend({
        msg: msg,
        to: [{
          fingerprint: billPub.pubkeys[0].fingerprint
        }]
      }, msg))
    })

    var togo = 3 // 2 message, 1 unchained
    ;['message', 'unchained'].forEach(function (event) {
      driverBill.on(event, function () {
        if (--togo) return

        // # 1
        t.pass('2nd party is up to date')

        // share all msgs with rufus
        togo = 3
        share()
      })

      driverRufus.on(event, function (info) {
        driverRufus.lookupObject(info)
          .done(function (chainedObj) {
            var msg = chainedObj.parsed.data
            if (event === 'message') {
              t.equal(msg.deliver, true)
            } else {
              t.equal(msg.chain, true)
            }

            if (--togo === 0) {
              // check for other events coming in
              // # 5
              setTimeout(t.pass, 2000)
            }
          })
      })
    })

    function share () {
      var stream = driverBill.decryptedMessagesStream()
        .pipe(map(function (obj, cb) {
          if (obj[TYPE] !== 'message') {
            cb()
          } else {
            cb(null, obj)
          }
        }))

      collect(stream, function (err, results) {
        if (err) throw err

        var sent = {}
        results.forEach(function (obj) {
          if (sent[obj[CUR_HASH]]) return

          sent[obj[CUR_HASH]] = true
          var shareOpts = extend({
            to: [{
              fingerprint: rufusPub.pubkeys[0].fingerprint
            }]
          }, obj.parsed.data) // msg body

          shareOpts[CUR_HASH] = obj[CUR_HASH]
          driverTed.share(shareOpts)
            .done()
        })
      })
    }
  })
})

test('message resolution - contents match on p2p and chain channels', function (t) {
  t.plan(6)
  t.timeoutAfter(20000)

  var people = [driverBill, driverTed]
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    people.forEach(function (driver) {
      driver.on('unchained', onUnchained.bind(driver))
    })

    ;['message', 'resolved'].forEach(function (event) {
      driverBill.on(event, function (obj) {
        driverBill.lookupObject(obj)
          .done(function (chainedObj) {
            checkMessage(chainedObj.parsed.data)
          })
      })
    })

    var billCoords = getIdentifier(billPub)
    var msg = { hey: 'ho' }
    msg[TYPE] = 'blahblah'
    msg = toMsg(msg)

    var messagesChained = 0
    Builder()
      .data(msg)
      .signWith(getSigningKey(tedPriv))
      .build()
      .then(function (buf) {
        return driverTed.send({
          msg: buf,
          to: [billCoords],
          chain: true,
          deliver: true
        })
      })
      .done()

    function onUnchained (info) {
      this.lookupObject(info)
        .done(function (chainedObj) {
          checkMessage(chainedObj.parsed.data)
          if (++messagesChained === 2) {
            checkMessageDB()
          }
        })
    }

    function checkMessage (m) {
      if (Buffer.isBuffer(m)) m = JSON.parse(m)

      delete m[constants.SIG]
      t.deepEqual(m, msg)
    }

    function checkMessageDB () {
      ;[driverBill, driverTed].forEach(function (driver) {
        collect(driver.decryptedMessagesStream(), checkLast.bind(driver))
      })

      function checkLast (err, messages) {
        if (err) throw err

        messages.sort(function (a, b) {
          return a.dateUnchained - b.dateUnchained
        })

        checkMessage(messages.pop().parsed.data)
      }
    }

  })
})

test('http messenger, recipient-specific', function (t) {
  t.timeoutAfter(20000)

  var people = [driverBill, driverTed]
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    // ted runs an http server
    var app = express()
    var server = app.listen(++BASE_PORT)
    server.once('listening', function () {
      var tedServer = new Transport.HttpServer({
        router: app,
        receive: function (buf, from) {
          t.equal(from[ROOT_HASH], driverBill.myRootHash())
          server.close()
          t.pass()
          t.end()
          return Q()
        }
      })

      // driverTed.setHttpServer(tedServer)

      // bill can contact ted over http
      var httpToTed = new Transport.HttpClient({
        rootHash: driverBill.myRootHash()
      })

      httpToTed.addRecipient(driverTed.myRootHash(), 'http://127.0.0.1:' + BASE_PORT + '/')
      // driverBill.setHttpClient(httpToTed, driverTed.myRootHash())
      driverBill._send = httpToTed.send.bind(httpToTed)
      httpToTed.on('message', driverBill.receiveMsg)

      driverTed._send = tedServer.send.bind(tedServer)

      var msg = toMsg({ hey: 'ho' })
      driverBill.send({
        msg: msg,
        to: [{
          fingerprint: tedPub.pubkeys[0].fingerprint
        }],
        deliver: true
      })
      .done()
    })
  })
})

test('forget contact', function (t) {
  t.timeoutAfter(20000)
  var people = [driverBill, driverTed, driverRufus]
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    var receivePromise = Q.Promise(function (resolve) {
      driverTed.once('message', resolve)
    })

    var rufusRootHash = driverRufus.myRootHash()
    var tedRootHash = driverTed.myRootHash()
    var billRootHash = driverBill.myRootHash()
    var tedMsgHash

    contactBoth()
      .then(forgetRufus)
      .done(function () {
        t.end()
      })

    function contactBoth () {
      return Q.all([
        driverBill.send({
          msg: toMsg({ yo: 'ted' }),
          deliver: true,
          to: [getIdentifier(tedPub)]
        }),
        driverBill.send({
          msg: toMsg({ yo: 'rufus' }),
          deliver: true,
          to: [getIdentifier(rufusPub)]
        }),
        onceReceivedMessage(driverTed),
        onceReceivedMessage(driverRufus)
      ])
      .spread(function (tedEntries) {
        tedMsgHash = tedEntries[0].get(ROOT_HASH)
        return Q.all([
          driverBill.getConversation(tedRootHash),
          driverBill.getConversation(rufusRootHash)
        ])
      })
      .then(function (convs) {
        convs.forEach(function (c) {
          t.equal(c.length, 1)
        })
      })
    }

    function forgetRufus () {
      return driverBill.forget(rufusRootHash)
        .then(function (forgotten) {
          t.equal(forgotten.length, 1)
          return Q.all([
            driverBill.getConversation(tedRootHash),
            driverBill.getConversation(rufusRootHash)
          ])
        })
        .spread(function (tedConv, rufusConv) {
          t.equal(tedConv.length, 1)
          t.equal(rufusConv.length, 0)
          // make sure keeper still has
          // ted conversations messages
          return Q.all(tedConv.map(function (info) {
            return driverBill.lookupObject(info, true) // no cache
          }))
        })
    }
  })
})

test('forget contact does not forget messages with more than one unforgotten recipient', function (t) {
  t.timeoutAfter(20000)
  var people = [driverBill, driverTed, driverRufus]
  monitorIdentityPublishAddr(people)
  publishIdentities(people, function () {
    var rufusRootHash = driverRufus.myRootHash()
    var tedRootHash = driverTed.myRootHash()
    var billRootHash = driverBill.myRootHash()
    var tedMsgHash

    msgTed()
      .then(shareWithRufus)
      .then(forgetTed)
      .done(function () {
        t.end()
      })

    function msgTed () {
      return Q.all([
        driverBill.send({
          msg: toMsg({ yo: 'ted' }),
          deliver: true,
          to: [getIdentifier(tedPub)]
        }),
        onceReceivedMessage(driverTed),
      ])
      .spread(function (entries) {
        tedMsgHash = entries[0].get(ROOT_HASH)
      })
    }

    function shareWithRufus () {
      var share = {
        to: [getIdentifier(rufusPub)],
        deliver: true
      }

      share[CUR_HASH] = tedMsgHash
      return Q.all([
        driverBill.share(share),
        onceReceivedMessage(driverRufus)
      ])
    }

    function forgetTed () {
      // this message should be forgotten
      // before it gets sent
      return driverBill.send({
        msg: toMsg({ yo: 'again' }),
        deliver: true,
        chain: true,
        to: [getIdentifier(tedPub)]
      })
      .then(function () {
        return driverBill.forget(tedRootHash)
      })
      .then(function (forgotten) {
        forgotten.forEach(function (msg) {
          // 1st message shouldn't be deleted as it was shared with rufus
          // 2nd should
          t.equal(msg.deleted, msg[CUR_HASH] !== tedMsgHash)
        })

        return Q.all([
          driverBill.getConversation(tedRootHash),
          driverBill.getConversation(rufusRootHash)
        ])
      })
      .spread(function (tHist, rHist) {
        t.equal(tHist.length, 0)
        t.equal(rHist.length, 1)
        return driverTed.forget(billRootHash)
      })
      .then(function (forgotten) {
        t.equal(forgotten[0].deleted, true)
        return driverTed.getConversation(billRootHash)
      })
      .then(function (msgs) {
        t.equal(msgs.length, 0)
      })
    }
  })
})

test('shouldLoadTx', function (t) {
  t.timeoutAfter(10000)

  monitorIdentityPublishAddr(driverBill, driverTed, driverRufus)
  var billTxId
  var tedTxId
  driverRufus.on('unchained', function (info) {
    t.equal(info.txId, billTxId)
    // make sure ted doesn't get loaded
    setTimeout(t.end, driverRufus.syncInterval * 3)
  })

  driverRufus._shouldLoadTx = function (tx) {
    return tx.txId === billTxId
  }

  driverBill.publishMyIdentity().done()
  driverTed.publishMyIdentity().done()
  driverBill.on('chained', function (info) {
    billTxId = info.txId
  })

  driverTed.on('chained', function (info) {
    tedTxId = info.txId
  })
})

test('watchTxs', function (t) {
  t.plan(2)
  t.timeoutAfter(5000)

  var someAddress = 'n2v6PrjuCFBoCn8MmbaK1bcZfC5aDY3RvP'

  var txId
  driverBill.publish({
    msg: toMsg({ hey: 'ho' }),
    to: [{ fingerprint: someAddress }]
  })

  driverBill.on('chained', function (info) {
    txId = info.txId
    driverTed.watchTxs(info.txId)
  })

  driverTed.miscDB.once('watchedTxs', function (txIds) {
    t.deepEqual(txIds, [txId])
  })

  driverTed.on('unchained', function (info) {
    t.equal(info.txId, txId)
  })
})

test('watchAddresses', function (t) {
  t.timeoutAfter(5000)

  var someAddress = 'n2v6PrjuCFBoCn8MmbaK1bcZfC5aDY3RvP'

  var txId
  driverBill.publish({
    msg: toMsg({ hey: 'ho' }),
    to: [{ fingerprint: someAddress }]
  })

  driverBill.on('chained', function (info) {
    txId = info.txId
  })

  driverTed.watchAddresses(someAddress)
  driverTed.on('unchained', function (info) {
    t.equal(info.txId, txId)
    t.end()
  })
})

function init (cb) {
  reinitCount++

  var billWallet = walletFor(billPriv, null, 'messaging')
  var blockchain = billWallet.blockchain
  var commonOpts = {
    syncInterval: 1000,
    chainThrottle: 1000,
    unchainThrottle: 1000,
    sendThrottle: 1000,
    networkName: NETWORK_NAME,
    // keeper: keeper,
    blockchain: blockchain,
    leveldown: memdown
  }

  bootstrapDHT = new DHT({ bootstrap: false })
  bootstrapDHT.listen(BOOTSTRAP_DHT_PORT)

  for (var name in CONFIGS) {
    var config = CONFIGS[name]
    var dht = dhtFor(CONFIGS[name].identityJSON)
    dht.listen(config.port)
    var messenger = newMessenger(extend(config, {
      dht: dht
    }))

    var driver = new Driver(extend(config, {
      pathPrefix: name + reinitCount,
      keeper: newKeeper(),
      wallet: name === 'bill' ? billWallet : walletFor(config.keys, blockchain, 'messaging'),
      _send: messenger.send.bind(messenger)
    }, commonOpts))

    messenger.on('message', driver.receiveMsg)

    CONFIGS[name].state = {
      dht: dht,
      messenger: messenger,
      driver: driver
    }
  }

  driverBill = CONFIGS.bill.state.driver
  driverTed = CONFIGS.ted.state.driver
  driverRufus = CONFIGS.rufus.state.driver

  return Q.all(Object.keys(CONFIGS).map(function (name) {
    return CONFIGS[name].state.driver.ready()
  })).done(cb)
}

function teardown (cb) {
  return Q.all(Object.keys(CONFIGS).map(function (name) {
      return destroyStuff(CONFIGS[name].state)
    }))
    .then(function () {
      return Q.ninvoke(bootstrapDHT, 'destroy')
    })
    .done(function () {
      rimraf.sync(STORAGE_DIR)
      for (var name in CONFIGS) {
        delete CONFIGS[name].state
      }

      driverBill = driverTed = driverRufus = null
      // printStats()
      safe(cb)()
    })
}

function destroyStuff (stuff) {
  return stuff.driver.destroy()
    .then(function () {
      return stuff.messenger.destroy()
    })
    .then(function () {
      return Q.ninvoke(stuff.dht, 'destroy')
    })
}

function printStats () {
  var stats = timTimer.getStats()
    .filter(function (s) {
      return s.timePerInvocation > 20000000 // 20 ms
    })

  stats.forEach(function (s) {
    s.time /= 1e6
    s.timePerInvocation /= 1e6
  })

  console.log(stats)
  timTimer.reset()
}

function publishIdentities (drivers, cb) {
  var defer = Q.defer()
  var togo = drivers.length * drivers.length
  drivers.forEach(function (d) {
    global.d = d
    d.on('unchained', onUnchained)
    d.publishMyIdentity().done()
  })

  return defer.promise.nodeify(cb || noop)

  function onUnchained (info) {
    if (--togo) return

    drivers.forEach(function (d) {
      d.removeListener('unchained', onUnchained)
    })

    defer.resolve()
  }
}

function dhtFor (pub) {
  return new DHT({
    nodeId: nodeIdFor(pub),
    bootstrap: ['127.0.0.1:' + BOOTSTRAP_DHT_PORT]
    // ,
    // bootstrap: ['tradle.io:25778']
  })
}

function nodeIdFor (pub) {
  return crypto.createHash('sha256')
    .update(getDSAKey(pub.pubkeys).fingerprint)
    .digest()
    .slice(0, 20)
}

function walletFor (keys, blockchain, purpose) {
  var unspents = []
  for (var i = 0; i < 20; i++) {
    unspents.push(100000)
  }

  return fakeWallet({
    blockchain: blockchain,
    unspents: unspents,
    priv: findBitcoinKey(keys, purpose).priv
  })
}

function rethrow (err) {
  if (err) throw err
}

function getSigningKey (keys) {
  var key = find(keys, function (k) {
    return k.type === 'ec' && k.purpose === 'sign'
  })

  return key && toKey(key)
}

function toMsg (msg) {
  msg[NONCE] = '' + nonce++
  return msg
}

function cloneDeadDriver (driver) {
  return new Driver(pick(driver, [
    'pathPrefix',
    'identity',
    'keys',
    'wallet',
    // 'dht',
    // 'port',
    'networkName',
    'blockchain',
    'leveldown',
    'syncInterval',
    'chainThrottle',
    'unchainThrottle',
    'sendThrottle',
    'keeper',
    '_send'
  ]))
}

function getIdentifier (identity) {
  return {
    fingerprint: identity.pubkeys[0].fingerprint
  }
}

function getRHIdentifier (driver) {
  return utils.toObj(ROOT_HASH, driver.myRootHash())
}

function getFunctionName(fn) {
  return fn.name || fn.toString().match(/function (.*?)\s*\(/)[1];
}

/**
 * returns mock keeper with fallback to sharedKeeper (which hosts identities)
 */
function newKeeper () {
  var k = FakeKeeper.empty()
  var getOne = k.getOne
  k.getOne = function (key) {
    return getOne.apply(this, arguments)
      .catch(function (err) {
        return sharedKeeper.getOne(key)
      })
  }

  k.push = function (opts) {
    typeforce({
      key: 'String',
      value: 'Buffer'
    }, opts)

    return sharedKeeper.put(opts.key, opts.value)
  }

  return k
}

function newMessenger (opts) {
  typeforce({
    identityJSON: 'Object',
    keys: 'Array',
    port: 'Number',
    dht: 'Object'
  }, opts)

  var otrKey = getDSAKey(opts.keys)
  return new P2PTransport({
    zlorp: new Zlorp({
      available: true,
      leveldown: memdown,
      port: opts.port,
      dht: opts.dht,
      key: toKey(otrKey).priv()
    })
  })
}

function getDSAKey (keys) {
  return keys.filter(function (p) {
    return p.type === 'dsa' && p.purpose === 'sign'
  })[0]
}

function killAndRessurrect (name) {
  var config = CONFIGS[name]
  var state = config.state
  var driver = state.driver
  return destroyStuff(state)
    .then(function () {
      driver = cloneDeadDriver(driver)
      var dht = dhtFor(config.identityJSON)
      dht.listen(config.port)

      var messenger = state.messenger = newMessenger(extend(config, {
        dht: dht
      }))

      config.state = {
        dht: dht,
        messenger: messenger,
        driver: driver
      }

      switch (name) {
        case 'bill':
          driverBill = driver
          break
        case 'ted':
          driverTed = driver
          break
        case 'rufus':
          driverRufus = driver
          break
      }

      messenger.on('message', driver.receiveMsg)
    })
}

function byTimestampAsc (a, b) {
  return parseFloat(a.timestamp) - parseFloat(b.timestamp)
}

function findBitcoinKey (keys, purpose) {
  return find(keys, function (k) {
    return k.type === 'bitcoin' &&
      k.networkName === NETWORK_NAME &&
      k.purpose === purpose
  })
}

function onceReceivedMessage (driver) {
  var defer = Q.defer()
  driver.once('message', defer.resolve)
  return defer.promise
}

function monitorIdentityPublishAddr (/* people */) {
  utils.argsToArray(arguments).forEach(function (tim) {
    tim.watchAddresses(constants.IDENTITY_PUBLISH_ADDRESS)
  })
}
