
// var test = require('tape')
var fs = require('fs')
var path = require('path')
var crypto = require('crypto')
var parallel = require('run-parallel')
var extend = require('extend')
var DHT = require('bittorrent-dht')
var loadComponents = require('../components')
var Builder = require('chained-obj').Builder
var billPub = fs.readFileSync(path.join(__dirname, './fixtures/bill-pub.json'))
var billPriv = require('./fixtures/bill-priv')
var tedPriv = require('./fixtures/ted-priv')
var Identity = require('midentity').Identity
var Fakechain = require('blockloader/fakechain')
var help = require('tradle-test-helpers')
var FakeKeeper = help.FakeKeeper
var fakeWallet = help.fakeWallet
var bill = Identity.fromJSON(billPriv)
var ted = Identity.fromJSON(tedPriv)
var networkName = 'testnet'
// var blockchain = new Fakechain({ networkName: networkName })
var Driver = require('../')
var keeper = FakeKeeper.empty()
var billPort = 51086
var tedPort = 51087

var billWallet = walletFor(bill)
var blockchain = billWallet.blockchain
var tedWallet = walletFor(ted)

var billDHT = dhtFor(bill)
billDHT.listen(billPort)

var tedDHT = dhtFor(ted)
tedDHT.listen(tedPort)

billDHT.addNode('127.0.0.1:' + tedPort, tedDHT.nodeId)
tedDHT.addNode('127.0.0.1:' + billPort, billDHT.nodeId)

var commonOpts = {
  networkName: networkName,
  keeper: keeper,
  blockchain: blockchain
}

var driverBill = new Driver(extend({
  pathPrefix: 'bill',
  identity: bill,
  wallet: billWallet,
  dht: billDHT
}, commonOpts))

// driverBill.on('message', function (msg) {
//   debugger
// })

var driverTed = new Driver(extend({
  pathPrefix: 'ted',
  identity: ted,
  wallet: tedWallet,
  dht: tedDHT
}, commonOpts))

// driverTed.on('message', function (msg) {
//   debugger
// })

driverTed.on('resolved', function (chainedObj) {
  console.log('resolved', chainedObj)
})

parallel([
  driverBill.once.bind(driverBill, 'ready'),
  driverTed.once.bind(driverTed, 'ready')
], function (err) {
  if (err) throw err

  // regular message

  // publishIdentity(sendChainedMsg)
  // driverBill.getPending(console.log)
  driverBill.getResolved(function (r) {
    console.log(r)
    driverBill.destroy(function () {
      process.exit()
    })
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

function walletFor (identity) {
  return fakeWallet({
    blockchain: blockchain,
    unspents: [100000, 100000, 100000, 100000],
    priv: identity.keys({
      type: 'bitcoin',
      networkName: networkName
    })[0].priv()
  })
}

function publishIdentity (cb) {
  var builder = new Builder()
    .data(billPub)
    .build(function (err, buf) {
      if (err) throw err

      driverBill.send({
        msg: buf,
        to: ted,
        chain: {
          public: true
        }
      })

      if (cb) cb()
    })
}

function sendChainedMsg (cb) {
  var builder = new Builder()
    .data({
      hey: 'ho'
    })
    .signWith(bill.keys({ type: 'dsa' })[0])
    .build(function (err, buf) {
      if (err) throw err

      driverBill.send({
        msg: buf,
        to: ted,
        chain: {
          public: true,
          recipients: ted.keys({
            type: 'bitcoin',
            networkName: 'testnet',
            purpose: 'payment'
          }).map(function (k) { return k.pubKeyString() })
        }
      })
    })
}
