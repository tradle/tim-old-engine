
var crypto = require('crypto')
var find = require('array-find')
var DHT = require('bittorrent-dht')
var leveldown = require('memdown')
var utils = require('tradle-utils')
var Driver = require('../')
var Identity = require('midentity').Identity
// var tedPriv = require('chained-chat/test/fixtures/ted-priv')
// var Fakechain = require('blockloader/fakechain')
var Blockchain = require('cb-blockr')
var Keeper = require('bitkeeper-js')
// var Wallet = require('simple-wallet')
// var fakeKeeper = help.fakeKeeper
// var fakeWallet = help.fakeWallet
// var ted = Identity.fromJSON(tedPriv)
var billPriv = require('./fixtures/bill-priv')
var billPub = require('./fixtures/bill-pub.json')
var networkName = 'testnet'
var BILL_PORT = 51086
// var keeper = fakeKeeper.empty()

function buildDriver (identity, keys, port) {
  var iJSON = identity.toJSON()
  var dht = dhtFor(iJSON)
  dht.listen(port)

  var keeper = new Keeper({
    dht: dht
  })

  var blockchain = new Blockchain(networkName)

  return new Driver({
    pathPrefix: iJSON.name.firstName.toLowerCase(),
    networkName: networkName,
    keeper: keeper,
    blockchain: blockchain,
    leveldown: leveldown,
    identity: identity,
    identityKeys: keys,
    dht: dht,
    port: port,
    syncInterval: 60000
  })

}

// var tedPub = new Buffer(stringify(require('./fixtures/ted-pub.json')), 'binary')
// var tedPriv = require('./fixtures/ted-priv')
// var ted = Identity.fromJSON(tedPriv)
// var tedPort = 51087
// var tedWallet = realWalletFor(ted)
// var blockchain = tedWallet.blockchain
// var tedWallet = walletFor(ted)

var driverBill = buildDriver(Identity.fromJSON(billPub), billPriv, BILL_PORT)
driverBill.once('ready', function () {
  driverBill.on('chained', function (obj) {
    console.log('chained', obj)
    debugger
  })

  debugger
  driverBill.publishMyIdentity()
  driverBill.on('error', function (err) {
    console.error(err)
    debugger
  })
})

// driverTed.on('ready', function () {
//   var msg = new Buffer(stringify({
//     hey: 'ho'
//   }), 'binary')

//   driverTed.send({
//     msg: msg,
//     to: bill
//   })
// })

function dhtFor (identity) {
  return new DHT({
    nodeId: nodeIdFor(identity),
    bootstrap: ['tradle.io:25778']
  })
}

function nodeIdFor (identity) {
  return crypto.createHash('sha256')
    .update(findKey(identity.pubkeys, { type: 'dsa' }).fingerprint)
    .digest()
    .slice(0, 20)
}

function findKey (keys, where) {
  return find(keys, function (k) {
    for (var p in where) {
      if (k[p] !== where[p]) return false
    }

    return true
  })
}

// function fakeWalletFor (identity) {
//   return fakeWallet({
//     blockchain: blockchain,
//     unspents: [100000, 100000, 100000, 100000],
//     priv: identity.keys({
//       type: 'bitcoin',
//       networkName: networkName
//     })[0].priv()
//   })
// }
