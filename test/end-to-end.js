
if (process.env.MULTIPLEX) {
  console.log('multiplex over UTP')
  require('multiplex-utp')
}

var path = require('path')
var test = require('tape')
var rimraf = require('rimraf')
var find = require('array-find')
var pick = require('object.pick')
var crypto = require('crypto')
var extend = require('extend')
var memdown = require('memdown')
var collect = require('stream-collector')
var map = require('map-stream')
var safe = require('safecb')
var Q = require('q')
var DHT = require('bittorrent-dht')
var Keeper = require('bitkeeper-js')
var Zlorp = require('zlorp')
Zlorp.ANNOUNCE_INTERVAL = Zlorp.LOOKUP_INTERVAL = 2000
var ChainedObj = require('chained-obj')
var Builder = ChainedObj.Builder
var kiki = require('kiki')
var toKey = kiki.toKey
var Identity = require('midentity').Identity
var billPub = require('./fixtures/bill-pub')
var billPriv = require('./fixtures/bill-priv')
var bill = Identity.fromJSON(billPub)
var tedPub = require('./fixtures/ted-pub')
var tedPriv = require('./fixtures/ted-priv')
var ted = Identity.fromJSON(tedPub)
var rufusPub = require('./fixtures/rufus-pub')
var rufusPriv = require('./fixtures/rufus-priv')
var rufus = Identity.fromJSON(rufusPub)
var constants = require('tradle-constants')
// var testDrivers = require('./helpers/testDriver')
var billPort = 51086
var tedPort = 51087
var rufusPort = 51088
var bootstrapDHT
var BOOTSTRAP_DHT_PORT = 54321
var TYPE = constants.TYPE
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH
var PREV_HASH = constants.PREV_HASH
var STORAGE_DIR = path.resolve('./storage')
// var tedHash = tedPub[ROOT_HASH] = 'c67905793f6cc0f0ab8d20aecfec441932ffb13d'
// var billHash = billPub[ROOT_HASH] ='fb07729c0cef307ab7c28cb76088cc60dbc98cdd'
// var tedHash = 'c67905793f6cc0f0ab8d20aecfec441932ffb13d'
// var billHash = 'fb07729c0cef307ab7c28cb76088cc60dbc98cdd'
var help = require('tradle-test-helpers')
// var fakeKeeper = help.fakeKeeper
var fakeWallet = help.fakeWallet
// var bill = Identity.fromJSON(billPriv)
// var ted = Identity.fromJSON(tedPriv)
var networkName = 'testnet'
// var blockchain = new Fakechain({ networkName: networkName })
var Driver = require('../')
var currentTime = require('../lib/now')
// var TestDriver = require('./helpers/testDriver')

var driverBill
var driverTed
var driverRufus
var reinitCount = 0
var chainThrottle = 5000

// var NAMES = ['bill', 'ted', 'rufus']
// var basePort = 51086

// test('zlorp', function (t) {
//   var billDHT = dhtFor(bill)
//   billDHT.listen(billPort)
//   var a = new Zlorp({
//     name: 'bill',
//     available: true,
//     leveldown: memdown,
//     port: billPort,
//     dht: billDHT,
//     key: toKey(find(billPriv, function (k) {
//       return k.type === 'dsa' && k.purpose === 'sign'
//     })).priv()
//   })

//   var tedDHT = dhtFor(ted)
//   tedDHT.listen(tedPort)
//   var b = new Zlorp({
//     name: 'ted',
//     available: true,
//     leveldown: memdown,
//     port: tedPort,
//     dht: tedDHT,
//     key: toKey(find(tedPriv, function (k) {
//       return k.type === 'dsa' && k.purpose === 'sign'
//     })).priv()
//   })

//   billDHT.addNode('127.0.0.1:' + tedPort, tedDHT.nodeId)
//   tedDHT.addNode('127.0.0.1:' + billPort, billDHT.nodeId)

//   a.send(new Buffer('yo'), b.fingerprint)
//   b.on('data', function () {
//     a.destroy()
//     b.destroy()
//     t.end()
//   })

//   b.on('connect', function () {
//     console.log('connect')
//   })

//   b.on('knock', function () {
//     console.log('knock')
//   })

//   console.log('BILL', a.key.fingerprint())
//   console.log('TED', b.key.fingerprint())
// })

reinitAndTest('delivered/chained/both', function (t) {
  t.plan(4)
  t.timeoutAfter(25000)
  publishAll([driverBill, driverTed], function () {
    var msgs = [
      { chain: true, deliver: false },
      { chain: false, deliver: true },
      { chain: true, deliver: true }
    ]

    msgs.forEach(function (msg) {
      driverTed.send(extend({
        msg: msg,
        to: [{
          fingerprint: billPub.pubkeys[0].fingerprint
        }],
        chain: true,
        deliver: false
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

reinitAndTest('handle non-data tx', function (t) {
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
      setTimeout(t.end, 1000)
    })
})

reinitAndTest('self publish, edit, republish', function (t) {
  publish(function () {
    failToRepeatPublish(function () {
      republish(function () {
        readIdentities(t.end)
      })
    })
  })

  var identitiesChecked = 0

  function publish (next) {
    var togo = 2
    driverBill.publishMyIdentity()
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
    driverBill.publishMyIdentity()
    driverBill.publishMyIdentity()
    setTimeout(function () {
      driverTed.removeListener('unchained', t.fail)
      next()
    }, 3000)
  }

  function republish (next) {
    driverBill.identityJSON.name.firstName = 'blah'
    driverBill.publishMyIdentity()
    driverTed.once('unchained', function (info) {
      driverTed.lookupObject(info)
        .done(function (chainedObj) {
          var loaded = chainedObj.parsed.data
          t.equal(loaded[PREV_HASH], driverBill.identityMeta[PREV_HASH])
          t.equal(loaded[ROOT_HASH], driverBill.identityMeta[ROOT_HASH])
          t.deepEqual(loaded, driverBill.identityJSON)
          next()
        })
    })
  }

  function readIdentities (next) {
    var bStream = driverBill.identities().createValueStream()
    var tStream = driverTed.identities().createValueStream()
    collect(bStream, checkIdentities.bind(driverBill))
    collect(tStream, checkIdentities.bind(driverTed))
  }

  function checkIdentities (err, identities) {
    if (err) throw err

    // console.log(this.identityJSON.name.firstName)
    t.equal(identities.length, 1)
    identities.forEach(function (ident) {
      t.deepEqual(ident, driverBill.identityJSON)
    })

    if (++identitiesChecked === 2) {
      t.end()
    }
  }
})

reinitAndTest('throttle chaining', function (t) {
  t.plan(3)
  t.timeoutAfter(10000)

  var blockchain = driverBill.blockchain
  var propagate = blockchain.transactions.propagate
  var firstErrTime
  blockchain.transactions.propagate = function (txs, cb) {
    cb(new Error('this is a test error'))

    if (!firstErrTime) {
      firstErrTime = currentTime()
    } else {
      blockchain.transactions.propagate = propagate
      t.ok(currentTime() - firstErrTime > chainThrottle * 0.9) // fuzzy
    }
  }

  driverBill.publish({
      msg: { blah: 'yo' },
      to: [{ fingerprint: driverTed.wallet.addressString }]
    })
    .done()

  driverBill.on('error', function (err) {
    t.ok(/test error/.test(err.message))
  })
})

reinitAndTest('delivery check', function (t) {
  t.plan(3)
  t.timeoutAfter(30000)
  publishAll([driverBill, driverTed], function () {
    var billCoords = {
      fingerprint: billPub.pubkeys[0].fingerprint
    }

    var msg = { hey: 'blah' }

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
        driverBill = new Driver(extend(pick(driverBill, [
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
          keeper: new Keeper({ dht: driverBill.dht, storage: STORAGE_DIR })
        }))

        driverBill.on('message', checkReceived)
        driverBill.on('unchained', t.fail)
      })

    var togo = 2
    function checkReceived (info) {
      if (--togo === 0) {
        setTimeout(t.pass, 2000)
      }

      driverBill.lookupObject(info)
        .done(function (chainedObj) {
          t.deepEqual(chainedObj.parsed.data, msg)
        })
    }
  })
})

reinitAndTest('share chained content with 3rd party', function (t) {
  t.plan(5)
  t.timeoutAfter(25000)
  publishAll([driverBill, driverTed, driverRufus], function () {
    // make sure all the combinations work
    // make it easier to check by sending settings as messages
    var msgs = [
      { chain: true, deliver: false },
      { chain: false, deliver: true },
      { chain: true, deliver: true }
    ].map(function (m) {
      m[TYPE] = 'message'
      return m
    })

    // send all msgs to ted
    msgs.forEach(function (msg) {
      driverTed.send(extend({
        msg: msg,
        to: [{
          fingerprint: billPub.pubkeys[0].fingerprint
        }],
        chain: true,
        deliver: false
      }, msg))
    })

    var togo = 4
    ;['message', 'unchained'].forEach(function (event) {
      driverBill.on(event, function () {
        if (--togo === 0) {
          // share all msgs with rufus
          togo = 4
          share()
        }
      })

      driverRufus.on(event, function (info) {
        driverRufus.lookupObject(info)
          .done(function (chainedObj) {
            if (event === 'message') {
              t.deepEqual(chainedObj.parsed.data.deliver, true)
            } else {
              t.deepEqual(chainedObj.parsed.data.chain, true)
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

        results.forEach(function (obj) {
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

reinitAndTest('message resolution - contents match on p2p and chain channels', function (t) {
  t.plan(6)
  t.timeoutAfter(25000)

  publishAll([driverBill, driverTed], function () {
    ;[driverBill, driverTed].forEach(function (driver) {
      driver.on('unchained', onUnchained.bind(driver))
    })

    ;['message', 'resolved'].forEach(function (event) {
      driverBill.on(event, function (obj) {
        driverBill.lookupObject(obj)
          .then(function (chainedObj) {
            checkMessage(chainedObj.data)
          })
          .done()
      })
    })

    var billCoords = {
      fingerprint: billPub.pubkeys[0].fingerprint
    }

    var msg = { hey: 'ho' }
    msg[TYPE] = 'blahblah'

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
          var parsed = chainedObj.parsed
          checkMessage(parsed.data)
          if (++messagesChained === 2) {
            checkMessageDB()
          }
        })
    }

    function checkMessage (m) {
      // console.log('check')
      if (Buffer.isBuffer(m)) m = JSON.parse(m)

      // delete m[constants.SIG]
      t.deepEqual(m, msg)
    }

    function checkMessageDB () {
      ;[driverBill, driverTed].forEach(function (driver) {
        collect(driver.decryptedMessagesStream(), checkLast)
      })

      function checkLast (err, messages) {
        if (err) throw err

        checkMessage(messages.pop().parsed.data)
      }
    }

  })
})

test('teardown', function (t) {
  t.timeoutAfter(10000)
  teardown(function () {
    t.end()
  })
})

function reinit (cb) {
  if (driverBill) {
    return teardown(function () {
      driverBill = driverTed = driverRufus = null
      reinit(cb)
    })
  } else {
    return init(cb)
  }
}

function init (cb) {
  reinitCount++

  // commonOpts = {
  //   networkName: networkName,
  //   leveldown: memdown,
  //   syncInterval: 100,
  //   chainThrottle: chainThrottle
  // }

  // driverBill = TestDriver.fake(extend({
  //   identity: bill,
  //   identityKeys: billPriv,
  //   port: billPort,
  //   syncInterval: 100
  // }, commonOpts))

  // driverTed = TestDriver.fake(extend({
  //   identity: ted,
  //   identityKeys: tedPriv,
  //   port: tedPort,
  //   blockchain: driverBill.blockchain
  // }, commonOpts))

  // ;[driverBill, driverTed].forEach(function (d) {
  //   d.dht.addNode('127.0.0.1:' + BOOTSTRAP_DHT_PORT, bootstrapDHT.nodeId)
  // })

  var billWallet = walletFor(billPriv, null, 'messaging')
  var blockchain = billWallet.blockchain
  var commonOpts = {
    networkName: networkName,
    // keeper: keeper,
    blockchain: blockchain,
    leveldown: memdown,
    syncInterval: 1000,
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
    keeper: new Keeper({ dht: billDHT, storage: STORAGE_DIR + '/bill' }),
    // kiki: kiki.kiki(billPriv),
    wallet: billWallet,
    dht: billDHT,
    port: billPort
  }, commonOpts))

  driverTed = new Driver(extend({
    pathPrefix: 'ted' + reinitCount,
    identity: ted,
    identityKeys: tedPriv,
    keeper: new Keeper({ dht: tedDHT, storage: STORAGE_DIR + '/ted' }),
    // kiki: kiki.kiki(tedPriv),
    wallet: walletFor(tedPriv, blockchain, 'messaging'),
    dht: tedDHT,
    port: tedPort
  }, commonOpts))

  driverRufus = new Driver(extend({
    pathPrefix: 'rufus' + reinitCount,
    identity: rufus,
    identityKeys: rufusPriv,
    keeper: new Keeper({ dht: rufusDHT, storage: STORAGE_DIR + '/rufus' }),
    // kiki: kiki.kiki(rufusPriv),
    wallet: walletFor(rufusPriv, blockchain, 'messaging'),
    dht: rufusDHT,
    port: rufusPort
  }, commonOpts))

  var togo = 3

  driverBill.once('ready', finish)
  driverTed.once('ready', finish)
  driverRufus.once('ready', finish)

  function finish () {
    if (--togo === 0) cb()
  }
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
        Q.ninvoke(bootstrapDHT, 'destroy'),
        Q.nfcall(rimraf, STORAGE_DIR)
      ])
    })
    .done(safe(cb))
}

function reinitAndTest (name, testFn) {
  test(name, function (t) {
    var ctx = this
    var args = arguments
    reinit(function () {
      testFn.apply(ctx, args)
    })
  })
}

function publishAll (drivers, cb) {
  var togo = drivers.length * drivers.length
  drivers.forEach(function (d) {
    d.on('unchained', onUnchained)
    d.publishMyIdentity()
  })

  function onUnchained (info) {
    if (--togo) return

    drivers.forEach(function (d) {
      d.removeListener('unchained', onUnchained)
    })

    cb()
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
  return fakeWallet({
    blockchain: blockchain,
    unspents: [100000, 100000, 100000, 100000],
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

// function overrideDgram () {
//   var createSocket = dgram.createSocket
//   var id = 0
//   dgram.createSocket = function () {
//     var s = createSocket.apply(dgram, arguments)
//     s.once('listening', function () {
//       s.socket._id = s._id
//     })

//     s._id = id++
//     if ([51, 55, 75, 79].indexOf(s._id) !== -1) {
//       console.log('socket', s._id)
//       printStack()
//     }

//     return s
//   }
// }

// function printStack (from, to) {
//   var trace = stackTrace.get()
//   if (!from) from = 0
//   if (!to) to = trace.length - 1
//   trace = trace.slice(from + 1, to + 1)
//   if (trace.length) {
//     trace.forEach(function (t) {
//       console.log(t.toString())
//     })
//   }
// }

// setInterval(function () {
//   var handles = process._getActiveHandles()
//   console.log(handles.length, 'handles open')
//   var types = handles.map(function (h) {
//     var type = h.constructor.toString().match(/function (.*?)\s*\(/)[1]
//     if (type === 'Socket') {
//       if (h instanceof dgram.Socket) type += ' (raw)'
//       if (h._tag) type += ' ' + h._tag
//     }

//     return type + h._id
//   })

//   console.log(types)
// }, 2000).unref()
