
var leveldown = require('memdown')
// var utils = require('tradle-utils')
var Identity = require('midentity').Identity
// var tedPriv = require('chained-chat/test/fixtures/ted-priv')
// var Fakechain = require('blockloader/fakechain')
// var Blockchain = require('cb-blockr')
// var Keeper = require('bitkeeper-js')
// var Wallet = require('simple-wallet')
// var fakeKeeper = help.fakeKeeper
// var fakeWallet = help.fakeWallet
// var ted = Identity.fromJSON(tedPriv)
var billPriv = require('./fixtures/bill-priv')
billPriv.forEach(function (k) {
  if (k.type === 'bitcoin' && k.purpose === 'messaging') {
    // invalidate key
    k.fingerprint = 'mxFoKKA7R8CbjZk1ybaXwgrF8c8QnvnkdD'
  }
})

var billPub = require('./fixtures/bill-pub.json')
// var networkName = 'testnet'
var BILL_PORT = 51086
var TestDriver = require('./helpers/testDriver')
// var keeper = fakeKeeper.empty()

// function buildDriver (identity, keys, port) {
//   var iJSON = identity.toJSON()
//   var dht = dhtFor(iJSON)
//   dht.listen(port)

//   var keeper = new Keeper({
//     dht: dht
//   })

//   var blockchain = new Blockchain(networkName)

//   return new Driver({
//     pathPrefix: iJSON.name.firstName.toLowerCase(),
//     networkName: networkName,
//     keeper: keeper,
//     blockchain: blockchain,
//     leveldown: leveldown,
//     identity: identity,
//     identityKeys: keys,
//     dht: dht,
//     port: port,
//     syncInterval: 60000
//   })

// }

// var tedPub = new Buffer(stringify(require('./fixtures/ted-pub.json')), 'binary')
// var tedPriv = require('./fixtures/ted-priv')
// var ted = Identity.fromJSON(tedPriv)
// var tedPort = 51087
// var tedWallet = realWalletFor(ted)
// var blockchain = tedWallet.blockchain
// var tedWallet = walletFor(ted)

var driverBill = TestDriver.real({
  identity: Identity.fromJSON(billPub),
  identityKeys: billPriv,
  port: BILL_PORT,
  leveldown: leveldown
})

driverBill.once('ready', function () {
  driverBill.on('chained', function (obj) {
    console.log('chained', obj)
  })

  debugger
  driverBill.publishMyIdentity()
  driverBill.on('error', function (err) {
    console.error(err)
  })
})

console.log('Send money to', driverBill.wallet.addressString)
printBalance()
setInterval(printBalance, 60000).unref()

// setTimeout(function () {
//   driverBill.destroy()
//     .then(function () {
//       driverBill.dht.destroy()
//     })
// }, 5000)

// driverTed.on('ready', function () {
//   var msg = new Buffer(stringify({
//     hey: 'ho'
//   }), 'binary')

//   driverTed.send({
//     msg: msg,
//     to: bill
//   })
// })

function printBalance () {
  driverBill.wallet.balance(function (err, balance) {
    if (err) console.error('failed to get balance', err.message)
    else console.log('balance', balance)
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
