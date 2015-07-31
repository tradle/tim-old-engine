var test = require('tape')
var find = require('array-find')
var crypto = require('crypto')
var extend = require('extend')
var memdown = require('memdown')
var collect = require('stream-collector')
var map = require('map-stream')
var safe = require('safecb')
var Q = require('q')
var DHT = require('bittorrent-dht')
var utils = require('tradle-utils')
var ChainedObj = require('chained-obj')
var Builder = ChainedObj.Builder
var kiki = require('kiki')
var toKey = kiki.toKey
var billPub = require('./fixtures/bill-pub.json')
var tedPub = require('./fixtures/ted-pub.json')
var billPriv = require('./fixtures/bill-priv')
var tedPriv = require('./fixtures/ted-priv')
var constants = require('tradle-constants')
var TYPE = constants.TYPE
var ROOT_HASH = constants.ROOT_HASH
// var CUR_HASH = constants.CUR_HASH
var PREV_HASH = constants.PREV_HASH
// var tedHash = tedPub[ROOT_HASH] = 'c67905793f6cc0f0ab8d20aecfec441932ffb13d'
// var billHash = billPub[ROOT_HASH] ='fb07729c0cef307ab7c28cb76088cc60dbc98cdd'
// var tedHash = 'c67905793f6cc0f0ab8d20aecfec441932ffb13d'
// var billHash = 'fb07729c0cef307ab7c28cb76088cc60dbc98cdd'
var Identity = require('midentity').Identity
var help = require('tradle-test-helpers')
var fakeKeeper = help.fakeKeeper
var fakeWallet = help.fakeWallet
// var bill = Identity.fromJSON(billPriv)
// var ted = Identity.fromJSON(tedPriv)
var networkName = 'testnet'
// var blockchain = new Fakechain({ networkName: networkName })
var Driver = require('../')
var currentTime = require('../now')

var driverBill
var driverTed
var reinitCount = 0
var chainThrottle = 5000

test('setup', function (t) {
  t.plan(1)
  t.timeoutAfter(2000)

  init(t.pass)
})

reinitAndTest('don\'t choke on non-data tx', function (t) {
  driverBill.wallet.send()
    .to(driverTed.wallet.addressString, 10000)
    .execute()

  driverBill.on('error', rethrow)
  driverTed.on('error', rethrow)
  setTimeout(function () {
    collect(driverBill.transactions().createValueStream(), function (err, txs) {
      t.equal(txs.length, 1)
      var tx = txs[0]
      t.notOk('txData' in tx)
      t.notOk('txData' in tx)
      t.end()
    })
  }, 1000)
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
    driverBill.publishMyIdentity()
    driverTed.once('unchained', function (info) {
      driverTed.lookupObject(info)
        .done(function (chainedObj) {
          t.deepEqual(chainedObj.parsed.data, driverBill.identityJSON)
          next()
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
    collect(bStream, checkIdentities)
    collect(tStream, checkIdentities)
  }

  function checkIdentities (err, identities) {
    if (err) throw err

    t.equal(identities.length, 1)
    identities.forEach(function (ident) {
      // if (ident.name.firstName === driverBill.identityJSON.name.firstName) {
      t.deepEqual(ident, driverBill.identityJSON)
      // } else {
      //   t.deepEqual(ident, driverTed.identityJSON)
      // }
    })

    if (++identitiesChecked === 2) t.end()
  }
})

reinitAndTest('throttle chaining', function (t) {
  t.plan(3)
  t.timeoutAfter(10000)

  var blockchain = driverBill.blockchain
  var propagate = blockchain.transactions.propagate
  var firstErrTime
  blockchain.transactions.propagate = function (txs, cb) {
    cb(new Error('something went horribly wrong'))

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
    t.ok(/horribly/.test(err.message))
  })
})

reinitAndTest('structured but not chained message', function (t) {
  t.plan(2)
  t.timeoutAfter(20000)
  publishBoth(function () {
    var billCoords = {
      fingerprint: billPub.pubkeys[0].fingerprint
    }

    var msg = { hey: 'blah' }

    driverTed.send({
      msg: msg,
      to: [billCoords],
      chain: false
    })

    driverBill.on('message', function (info) {
      driverBill.lookupObject(info)
        .done(function (chainedObj) {
          t.deepEqual(chainedObj.parsed.data, msg)
        })
    })

    driverTed.on('chained', t.fail)
    driverTed.on('unchained', t.fail)
    driverBill.on('unchained', t.fail)
    setTimeout(t.pass, 3000)
  })
})

reinitAndTest('chained message', function (t) {
  t.plan(6)
  t.timeoutAfter(15000)

  publishBoth(function () {
    driverTed.on('unchained', onUnchained.bind(driverTed))
    driverBill.on('unchained', onUnchained.bind(driverBill))

    driverBill.on('message', function (obj) {
      driverBill.lookupObject(obj)
        .then(function (chainedObj) {
          checkMessage(chainedObj.data)
        })
        .done()
    })

    driverBill.on('resolved', function (obj) {
      driverBill.lookupObject(obj)
        .then(function (chainedObj) {
          checkMessage(chainedObj.data)
        })
        .done()
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
          chain: true
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
      collect(driverBill.messages().createValueStream().pipe(
        map(function (data, cb) {
          driverBill.lookupObject(data)
            .nodeify(cb)
        })
      ), checkLast)

      collect(driverTed.messages().createValueStream().pipe(
        map(function (data, cb) {
          driverTed.lookupObject(data)
            .nodeify(cb)
        })
      ), checkLast)

      function checkLast (err, messages) {
        if (err) throw err

        // console.log(JSON.stringify(messages, null, 2))
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
      driverBill = driverTed = null
      reinit(cb)
    })
  } else {
    return init(cb)
  }
}

function init (cb) {
  reinitCount++

  var bill = Identity.fromJSON(billPub)
  var ted = Identity.fromJSON(tedPub)
  var keeper = fakeKeeper.empty()
  var billPort = 51086
  var tedPort = 51087
  var billWallet = walletFor(billPriv, null, 'messaging')
  var blockchain = billWallet.blockchain
  var tedWallet = walletFor(tedPriv, blockchain, 'messaging')
  var commonOpts = {
    networkName: networkName,
    keeper: keeper,
    blockchain: blockchain,
    leveldown: memdown,
    syncInterval: 1000,
    chainThrottle: chainThrottle
  }

  var billDHT = dhtFor(bill)
  billDHT.listen(billPort)

  var tedDHT = dhtFor(ted)
  tedDHT.listen(tedPort)

  billDHT.addNode('127.0.0.1:' + tedPort, tedDHT.nodeId)
  tedDHT.addNode('127.0.0.1:' + billPort, billDHT.nodeId)

  driverBill = new Driver(extend({
    pathPrefix: 'bill' + reinitCount,
    identity: bill,
    identityKeys: billPriv,
    // kiki: kiki.kiki(billPriv),
    wallet: billWallet,
    dht: billDHT,
    port: billPort
  }, commonOpts))

  driverTed = new Driver(extend({
    pathPrefix: 'ted' + reinitCount,
    identity: ted,
    identityKeys: tedPriv,
    // kiki: kiki.kiki(tedPriv),
    wallet: tedWallet,
    dht: tedDHT,
    port: tedPort
  }, commonOpts))

  var togo = 2

  driverBill.once('ready', finish)
  driverTed.once('ready', finish)

  function finish () {
    if (--togo === 0) cb()
  }
}

function teardown (cb) {
  Q.all([
    driverBill.destroy(),
    driverTed.destroy()
  ]).done(safe(cb))
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

function publishBoth (cb) {
  driverBill.publishMyIdentity()
  driverTed.publishMyIdentity()

  var identitiesChained = 0
  var tedUnchained = onUnchained.bind(driverTed)
  var billUnchained = onUnchained.bind(driverBill)
  driverTed.on('unchained', tedUnchained)
  driverBill.on('unchained', billUnchained)

  function onUnchained (info) {
    // var self = this
    this.lookupObject(info)
      .then(function (chainedObj) {
        var parsed = chainedObj.parsed
        // console.log(self.identityJSON.name.formatted, 'unchained', parsed.data[TYPE])
        if (parsed.data[TYPE] === Identity.TYPE) {
          // console.log(self.identityJSON.name.formatted, 'unchained', parsed.data.name.formatted)
          if (++identitiesChained === 4) done() // 2 each, because both also detect themselves
        }
      })
      .done()
  }

  function done () {
    driverTed.removeListener('unchained', tedUnchained)
    driverBill.removeListener('unchained', billUnchained)
    cb()
  }
}

function dhtFor (identity) {
  return new DHT({
    nodeId: nodeIdFor(identity),
    bootstrap: false
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
