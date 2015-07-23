var test = require('tape')
var find = require('array-find')
var crypto = require('crypto')
var extend = require('extend')
var memdown = require('memdown')
var Q = require('q')
var DHT = require('bittorrent-dht')
var ChainedObj = require('chained-obj')
var Builder = ChainedObj.Builder
// var CreateReq = require('bitjoe-js/lib/requests/create')
// CreateReq.prototype._generateSymmetricKey = function () {
//   return new Buffer('1111111111111111111111111111111111111111111111111111111111111111')
// }

var billPub = require('./fixtures/bill-pub.json')
var tedPub = require('./fixtures/ted-pub.json')
var billPriv = require('./fixtures/bill-priv')
var tedPriv = require('./fixtures/ted-priv')
var constants = require('tradle-constants')
var TYPE = constants.TYPE
// var ROOT_HASH = constants.ROOT_HASH
// var CUR_HASH = constants.CUR_HASH
// var tedHash = tedPub[ROOT_HASH] = 'c67905793f6cc0f0ab8d20aecfec441932ffb13d'
// var billHash = billPub[ROOT_HASH] ='fb07729c0cef307ab7c28cb76088cc60dbc98cdd'
// var tedHash = 'c67905793f6cc0f0ab8d20aecfec441932ffb13d'
// var billHash = 'fb07729c0cef307ab7c28cb76088cc60dbc98cdd'
var mi = require('midentity')
var Identity = mi.Identity
var toKey = mi.toKey
var help = require('tradle-test-helpers')
var fakeKeeper = help.fakeKeeper
var fakeWallet = help.fakeWallet
var bill = Identity.fromJSON(billPub)
var ted = Identity.fromJSON(tedPub)
// var bill = Identity.fromJSON(billPriv)
// var ted = Identity.fromJSON(tedPriv)
var networkName = 'testnet'
// var blockchain = new Fakechain({ networkName: networkName })
var Driver = require('../')
var keeper = fakeKeeper.empty()
var billPort = 51086
var tedPort = 51087

var billWallet = walletFor(billPriv)
var blockchain = billWallet.blockchain
var tedWallet = walletFor(tedPriv)

var billDHT = dhtFor(bill)
billDHT.listen(billPort)

var tedDHT = dhtFor(ted)
tedDHT.listen(tedPort)

billDHT.addNode('127.0.0.1:' + tedPort, tedDHT.nodeId)
tedDHT.addNode('127.0.0.1:' + billPort, billDHT.nodeId)

var commonOpts = {
  networkName: networkName,
  keeper: keeper,
  blockchain: blockchain,
  leveldown: memdown,
  syncInterval: 1000
}

var driverBill = new Driver(extend({
  pathPrefix: 'bill',
  identityJSON: billPub,
  identityKeys: billPriv,
  wallet: billWallet,
  dht: billDHT,
  port: billPort
}, commonOpts))

// driverBill.on('message', function (msg) {
//   debugger
// })

var driverTed = new Driver(extend({
  pathPrefix: 'ted',
  identityJSON: tedPub,
  identityKeys: tedPriv,
  wallet: tedWallet,
  dht: tedDHT,
  port: tedPort
}, commonOpts))

;['warn', 'error'].forEach(function (bad) {
  driverBill.on(bad, rethrow)
  driverTed.on(bad, rethrow)
})

test('setup', function (t) {
  t.plan(2)

  driverBill.once('ready', t.pass)
  driverTed.once('ready', t.pass)
})

test('chained message', function (t) {
  t.plan(4)
  // t.timeoutAfter(15000)

  Builder()
    .data(tedPub)
    .signWith(getSigningKey(tedPriv))
    .build(function (err, result) {
      if (err) throw err

      driverTed.publish({
        msg: result.form,
        to: [{ fingerprint: constants.IDENTITY_PUBLISH_ADDRESS }]
      }).done()
    })

  Builder()
    .data(billPub)
    .signWith(getSigningKey(billPriv))
    .build(function (err, result) {
      if (err) throw err

      driverBill.publish({
        msg: result.form,
        to: [{ fingerprint: constants.IDENTITY_PUBLISH_ADDRESS }]
      }).done()
    })

  driverTed.on('unchained', onUnchained.bind(driverTed))
  driverBill.on('unchained', onUnchained.bind(driverBill))

  // driverTed.on('chained', function (entry) {
  //   // t.equal(entry[CUR_HASH], tedHash)
  //   console.log('ted chained', entry[CUR_HASH])
  // })

  // driverBill.on('chained', function (entry) {
  //   console.log('bill chained', entry[CUR_HASH])
  // })

  driverBill.on('message', function (obj) {
    driverBill.lookupChainedObj(obj)
      .then(function (chainedObj) {
        checkMessage(chainedObj.data)
      })
      .done()
  })

  driverBill.on('resolved', function (obj) {
    driverBill.lookupChainedObj(obj)
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

  var identitiesChained = 0

  function onUnchained (info) {
    this.lookupChainedObj(info)
      .then(function (chainedObj) {
        var parsed = chainedObj.parsed
        // console.log(self.identityJSON.name.formatted, 'unchained', parsed.data[TYPE])
        if (parsed.data[TYPE] === Identity.TYPE) {
          onIdentityChained(parsed.data)
        } else {
          checkMessage(parsed.data)
        }
      })
      .done()
  }

  function checkMessage (m) {
    // console.log('check')
    if (Buffer.isBuffer(m)) m = JSON.parse(m)

    // delete m[constants.SIG]
    t.deepEqual(m, msg)
  }

  function onIdentityChained (i) {
    if (++identitiesChained !== 4) return // 2 each, because both also detect themselves

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
  }
})

test('teardown', function (t) {
  t.timeoutAfter(10000)
  Q.all([
    driverBill.destroy(),
    driverTed.destroy()
  ]).done(function () {
    t.end()
  })
})

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

function walletFor (keys) {
  return fakeWallet({
    blockchain: blockchain,
    unspents: [100000, 100000, 100000, 100000],
    priv: find(keys, function (k) {
      return k.type === 'bitcoin' && k.networkName === networkName
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
