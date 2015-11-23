
if (process.env.MULTIPLEX) {
  console.log('multiplex over UTP')
  require('@tradle/multiplex-utp')
}

var path = require('path')
var test = require('tape-extra')
var rimraf = require('rimraf')
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
var DHT = require('@tradle/bittorrent-dht')
// var Keeper = require('bitkeeper-js')
var Zlorp = require('zlorp')
Zlorp.ANNOUNCE_INTERVAL = Zlorp.LOOKUP_INTERVAL = 5000
var ChainedObj = require('@tradle/chained-obj')
var Builder = ChainedObj.Builder
var kiki = require('@tradle/kiki')
var toKey = kiki.toKey
var CreateRequest = require('@tradle/bitjoe-js/lib/requests/create')
CreateRequest.prototype._generateSymmetricKey = function () {
  return new Buffer('1111111111111111111111111111111111111111111111111111111111111111', 'hex')
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
var networkName = 'testnet'
// var blockchain = new Fakechain({ networkName: networkName })
var Driver = require('../')
Driver.CATCH_UP_INTERVAL = 1000
Driver.SEND_THROTTLE = 1000
var utils = require('../lib/utils')
var Messengers = require('../lib/messengers')
// var TimeMethod = require('time-method')
// var timTimer = TimeMethod(Driver.prototype)
// for (var p in Driver.prototype) {
//   if (typeof Driver.prototype[p] === 'function') {
//     timTimer.time(p)
//   }
// }

var noop = function () {}

var driverBill
var driverTed
var driverRufus
var reinitCount = 0
var nonce
var chainThrottle = 1000
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

test('pause/unpause', function (t) {
  t.timeoutAfter(20000)
  driverBill.pause()
  driverTed.pause()
  driverBill.publishMyIdentity()
  driverTed.on('unchained', t.fail)
  setTimeout(function () {
    driverTed.removeListener('unchained', t.fail)
    driverBill.resume()
    driverTed.resume()
    driverTed.on('unchained', function () {
      t.pass()
      t.end()
    })
  }, 5000)
})

test('resending & order guarantees', function (t) {
  t.timeoutAfter(20000)
  publishIdentities([driverBill, driverTed], function () {
    var msgs = [
      {
        succeedAfter: 5
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
      "P767yN9gJZpg6DPFoXt+g5Ho8ZDPt9Bp1KQI",
      "P767yN9gJZlg6DPFoXt+g5Ho8ZDPt9Bp1KEI",
      "P767yN9gJZhg6DPFoXt+g5Ho8ZDPt9Bp1KEI",
      "P767yN9gJZ9g6DPFoXt+g5Ho8ZDPt9Bp1KEI",
      "P767yN9gJZ5g6DPFoXt+g5Ho8ZDPt9Bp1KEI"
    ]

    var copy = msgs.map(function (m) {
      return extend(m)
    })

    var z = driverTed.messenger
    var send = z.send
    z.send = function (rh, msg) {
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

    msgs.forEach(function (msg) {
      driverTed.send({
        msg: msg,
        deliver: true,
        to: [{
          fingerprint: billPub.pubkeys[0].fingerprint
        }]
      })
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

      driverTed.destroy()
        .then(function () {
          driverTed = cloneDeadDriver(driverTed)
          return Q.nfcall(collect, driverTed.messages().getToSendStream({ tail: false }))
        })
        .then(function (results) {
          // msg should not be queued for resend
          t.equal(results.length, 0, 'msg not requeued after succesfully sent')
          t.end()
        })
        .done()
    }
  })
})

test('the reader and the writer', function (t) {
  t.timeoutAfter(20000)

  var togo = 4
  var reader = driverBill
  reader.readOnly = true

  var readerCoords = [{
    fingerprint: reader.identityJSON.pubkeys[0].fingerprint
  }]

  var writer = driverTed
  var writerCoords = [{
    fingerprint: writer.identityJSON.pubkeys[0].fingerprint
  }]

  writer.publishMyIdentity().done()
  // publish reader's identity for them
  writer.publishIdentity(reader.identityJSON)
  reader.on('unchained', onUnchainedIdentity)
  writer.on('unchained', onUnchainedIdentity)
  var msg = toMsg({
    hey: 'ho'
  })

  writer.once('message', function (info) {
    writer.lookupObject(info)
      .then(function (obj) {
        return writer.send({
          chain: true,
          deliver: false,
          public: info.public,
          msg: obj.parsed.data,
          to: readerCoords
        })
      })
      .done()

    reader.once('unchained', function (info) {
      reader.lookupObject(info)
        .done(function (obj) {
          t.deepEqual(obj.parsed.data, msg)
          t.end()
        })
    })
  })

  function onUnchainedIdentity () {
    if (--togo) return

    reader.removeListener('unchained', onUnchainedIdentity)
    writer.removeListener('unchained', onUnchainedIdentity)

    reader.identityPublishStatus()
      .then(function (status) {
        t.ok(status.ever)
        t.ok(status.current)
      })
      .then(function () {
        return reader.send({
          chain: false,
          deliver: true,
          msg: msg,
          to: writerCoords
        })
      })
      .done()

    // reader.send({
    //   chain: false,
    //   deliver: true,
    //   public: true,
    //   msg: extend(reader.identityJSON),
    //   to: writerCoords
    // })
    // .then(function () {

    // })
  }
})

test('wipe dbs, get publish status on reload', function (t) {
  t.plan(4)

  Q.nfcall(publishIdentities, [driverBill])
    .then(driverBill.destroy)
    .then(function () {
      return Q.all(['messages', 'addressBook', 'txs'].map(function (dbName) {
        dbName = 'bill' + reinitCount + '-' + dbName + '.db'
        return Q.ninvoke(memdown, 'destroy', dbName)
      }))
    })
    .then(function () {
      driverBill = cloneDeadDriver(driverBill)
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
  t.plan(4)
  //t.timeoutAfter(60000)
  publishIdentities([driverBill, driverTed], function () {
    var msgs = [
      { chain: true, deliver: false },
      { chain: false, deliver: true },
      { chain: true, deliver: true }
    ].map(toMsg)

    msgs.forEach(function (msg) {
      driverTed.send(extend({
        msg: msg,
        to: [{
          fingerprint: billPub.pubkeys[0].fingerprint
        }]
      }, msg))
    })

    ;['message', 'unchained'].forEach(function (event) {
      driverBill.on(event, function (info) {
        driverBill.lookupObject(info)
          .done(function (chainedObj) {
            if (event === 'message') {
              t.deepEqual(chainedObj.parsed.data.deliver, true)
            } else {
              t.deepEqual(chainedObj.parsed.data.chain, true)
            }
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
      t.ok(utils.now() - firstErrTime > chainThrottle * 0.8) // fuzzy
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
  //t.timeoutAfter(60000)
  publishIdentities([driverBill, driverTed], function () {
    var billCoords = {
      fingerprint: billPub.pubkeys[0].fingerprint
    }

    var msg = toMsg({ hey: 'blah' })

    driverTed.send({
      msg: msg,
      to: [billCoords],
      deliver: true,
      chain: false
    })

    driverTed.on('sent', checkReceived)
    driverTed.on('chained', t.fail)
    driverTed.on('unchained', t.fail)
    driverBill.on('unchained', t.fail)
    driverBill.destroy()
      .done(function () {
        driverBill = cloneDeadDriver(driverBill)
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
  t.plan(6)
  t.timeoutAfter(60000)
  publishIdentities([driverBill, driverTed, driverRufus], function () {
    // make sure all the combinations work
    // make it easier to check by sending settings as messages
    var msgs = [
      { chain: true, deliver: false },
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

    var togo = 4
    ;['message', 'unchained'].forEach(function (event) {
      driverBill.on(event, function () {
        if (--togo) return

        t.pass('2nd party is up to date')

        // share all msgs with rufus
        togo = 4
        share()
      })

      driverRufus.on(event, function (info) {
        // console.log(event)
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
  t.timeoutAfter(60000)

  publishIdentities([driverBill, driverTed], function () {
    ;[driverBill, driverTed].forEach(function (driver) {
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

    var billCoords = {
      fingerprint: billPub.pubkeys[0].fingerprint
    }

    var msg = { hey: 'ho' }
    msg[TYPE] = 'blahblah'
    msg = toMsg(msg)

    var messagesChained = 0
    Builder()
      .data(msg)
      .signWith(getSigningKey(tedPriv))
      .build(function (err, result) {
        if (err) throw err

        driverTed.send({
          msg: result.form,
          to: [billCoords],
          chain: true,
          deliver: true
        }).done()
      })

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

test('recipient-specific messengers', function (t) {
  t.timeoutAfter(20000)

  publishIdentities([driverBill, driverTed], function () {
    // ted runs an http server
    var app = express()
    var server = app.listen(++BASE_PORT)
    server.once('listening', function () {
      var tedServer = new Messengers.HttpServer({
        router: app,
        receive: function (buf, from) {
          t.equal(from[ROOT_HASH], driverBill.myRootHash())
          server.close()
          t.pass()
          t.end()
          return Q()
        }
      })

      driverTed.addReceiver(tedServer)

      // bill can contact ted over http
      var httpToTed = new Messengers.HttpClient({
        rootHash: driverBill.myRootHash()
      })

      httpToTed.addRecipient(driverTed.myRootHash(), 'http://127.0.0.1:' + BASE_PORT + '/')
      driverBill.addSender(httpToTed, driverTed.myRootHash())

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

function init (cb) {
  reinitCount++

  var billWallet = walletFor(billPriv, null, 'messaging')
  var blockchain = billWallet.blockchain
  var commonOpts = {
    networkName: networkName,
    // keeper: keeper,
    blockchain: blockchain,
    leveldown: memdown,
    syncInterval: 3000,
    chainThrottle: chainThrottle
  }

  bootstrapDHT = new DHT({ bootstrap: false })
  bootstrapDHT.listen(BOOTSTRAP_DHT_PORT)

  var billDHT = dhtFor(bill)
  billDHT.listen(billPort)

  var tedDHT = dhtFor(ted)
  tedDHT.listen(tedPort)

  var rufusDHT = dhtFor(rufus)
  rufusDHT.listen(rufusPort)

  driverBill = new Driver(extend({
    pathPrefix: 'bill' + reinitCount,
    identity: bill,
    identityKeys: billPriv,
    keeper: sharedKeeper,
    // keeper: new Keeper({ dht: billDHT, storage: STORAGE_DIR + '/bill' }),
    // kiki: kiki.kiki(billPriv),
    wallet: billWallet,
    dht: billDHT,
    port: billPort
  }, commonOpts))

  driverTed = new Driver(extend({
    pathPrefix: 'ted' + reinitCount,
    identity: ted,
    identityKeys: tedPriv,
    keeper: sharedKeeper,
    // keeper: new Keeper({ dht: tedDHT, storage: STORAGE_DIR + '/ted' }),
    // kiki: kiki.kiki(tedPriv),
    wallet: walletFor(tedPriv, blockchain, 'messaging'),
    dht: tedDHT,
    port: tedPort
  }, commonOpts))

  driverRufus = new Driver(extend({
    pathPrefix: 'rufus' + reinitCount,
    identity: rufus,
    identityKeys: rufusPriv,
    keeper: sharedKeeper,
    // keeper: new Keeper({ dht: rufusDHT, storage: STORAGE_DIR + '/rufus' }),
    // kiki: kiki.kiki(rufusPriv),
    wallet: walletFor(rufusPriv, blockchain, 'messaging'),
    dht: rufusDHT,
    port: rufusPort
  }, commonOpts))

  return Q.all([
    driverBill.ready(),
    driverTed.ready(),
    driverRufus.ready()
  ]).done(cb)
}

function teardown (cb) {
  Q.all([
      driverBill.destroy(),
      driverTed.destroy(),
      driverRufus.destroy()
    ])
    .then(function () {
      return Q.all([
        Q.ninvoke(driverBill.dht, 'destroy'),
        Q.ninvoke(driverTed.dht, 'destroy'),
        Q.ninvoke(driverRufus.dht, 'destroy'),
        Q.ninvoke(bootstrapDHT, 'destroy')
        // Q.nfcall(rimraf, STORAGE_DIR)
      ])
    })
    .done(function () {
      rimraf.sync(STORAGE_DIR)
      driverBill = driverTed = driverRufus = null
      // printStats()
      safe(cb)()
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

function dhtFor (identity) {
  return new DHT({
    nodeId: nodeIdFor(identity),
    bootstrap: ['127.0.0.1:' + BOOTSTRAP_DHT_PORT]
    // ,
    // bootstrap: ['tradle.io:25778']
  })
}

function nodeIdFor (identity) {
  return crypto.createHash('sha256')
    .update(identity.keys({ type: 'dsa' })[0].fingerprint())
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
    priv: find(keys, function (k) {
      return k.type === 'bitcoin' &&
        k.networkName === networkName &&
        k.purpose === purpose
    }).priv
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
  return new Driver(extend(pick(driver, [
    'pathPrefix',
    'identity',
    'identityKeys',
    'wallet',
    'dht',
    'port',
    'networkName',
    'blockchain',
    'leveldown',
    'syncInterval',
    'chainThrottle'
  ]), {
    // keeper: new Keeper({ dht: driverBill.dht, storage: STORAGE_DIR })
    keeper: sharedKeeper
  }))
}

function getFunctionName(fn) {
  return fn.name || fn.toString().match(/function (.*?)\s*\(/)[1];
}
