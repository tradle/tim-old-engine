
var test = require('tape')
var fs = require('fs')
var find = require('array-find')
var path = require('path')
var crypto = require('crypto')
var bufferEqual = require('buffer-equal')
var parallel = require('run-parallel')
var extend = require('extend')
var memdown = require('memdown')
var stringify = require('tradle-utils').stringify
var DHT = require('bittorrent-dht')
var ChainedObj = require('chained-obj')
var Builder = ChainedObj.Builder
var Parser = ChainedObj.Parser
// var CreateReq = require('bitjoe-js/lib/requests/create')
// CreateReq.prototype._generateSymmetricKey = function () {
//   return new Buffer('1111111111111111111111111111111111111111111111111111111111111111')
// }

var billPub = require('./fixtures/bill-pub.json')
var tedPub = require('./fixtures/ted-pub.json')
var billPriv = require('./fixtures/bill-priv')
var tedPriv = require('./fixtures/ted-priv')
var constants = require('tradle-constants')
// var tedHash = tedPub[constants.ROOT_HASH] = 'c67905793f6cc0f0ab8d20aecfec441932ffb13d'
// var billHash = billPub[constants.ROOT_HASH] ='fb07729c0cef307ab7c28cb76088cc60dbc98cdd'
var tedHash = 'c67905793f6cc0f0ab8d20aecfec441932ffb13d'
var billHash = 'fb07729c0cef307ab7c28cb76088cc60dbc98cdd'
var mi = require('midentity')
var Identity = mi.Identity
var toKey = mi.toKey
var Fakechain = require('blockloader/fakechain')
var help = require('tradle-test-helpers')
var publishIdentity = require('../publishIdentity')
var getDHTKey = require('../getDHTKey')
var FakeKeeper = help.FakeKeeper
var fakeWallet = help.fakeWallet
var bill = Identity.fromJSON(billPub)
var ted = Identity.fromJSON(tedPub)
// var bill = Identity.fromJSON(billPriv)
// var ted = Identity.fromJSON(tedPriv)
var networkName = 'testnet'
// var blockchain = new Fakechain({ networkName: networkName })
var Driver = require('../')
var keeper = FakeKeeper.empty()
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

// driverTed.on('message', function (msg) {
//   console.log('message', msg)
// })

// driverTed.on('resolved', function (chainedObj) {
//   console.log('resolved', chainedObj)
// })

test('setup', function (t) {
  t.plan(2)

  driverBill.once('ready', t.pass)
  driverTed.once('ready', t.pass)
  // driverBill._addressBook.update(tedPub)
  //   .then(t.pass)
  //   .done()
  // driverTed._addressBook.update(billPub)
  //   .then(t.pass)
  //   .done()
})

// test('regular message', function (t) {
//   t.plan(1)
//   // t.timeoutAfter(10000)

//   // regular message
//   var msg = new Buffer(stringify({
//     hey: 'ho'
//   }), 'binary')

//   driverTed.once('message', function (m) {
//     t.deepEqual(m.data, msg)
//   })

//   // driverBill._addressBook.update(tedPub, function (err) {
//   //   if (err) throw err

//   //   driverTed._addressBook.update(billPub, function (err) {
//   //     if (err) throw err

//       driverBill.sendPlaintext({
//         msg: msg,
//         to: tedHash
//       })
//     // })
//   // })
// })

test('chained message', function (t) {
  t.plan(2)
  // t.timeoutAfter(15000)

  // chained msg
  var msg = {
    hey: 'ho'
  }

  msg[constants.TYPE] = 'blahblah'
  var signed
  var num = 0


  driverTed.on('saved', function (obj) {
    Parser.parse(obj.data, function (err, parsed) {
      t.equal(parsed.data.name.firstName, 'Bill')

      driverTed.on('resolved', function (obj) {
        t.equal(obj, 'blah')
      })

      var billInfo = {}
      billInfo[constants.ROOT_HASH] = billHash
      driverBill.sendStructured({
        msg: msg,
        to: [billInfo],
        sign: true,
        chain: true
      })
      .done()
    })

  })

  driverBill.publish({
    msg: billPub,
    sign: true
  })

  // publishIdentity(billPub, driverBill.chainwriterq, rethrow)
})

test('teardown', function (t) {
  t.plan(1)

  parallel([
    driverBill.destroy.bind(driverBill),
    driverTed.destroy.bind(driverTed)
  ], t.error)
})

// function publishIdentity (cb) {
//   var builder = new Builder()
//     .data(billPub)
//     .build(function (err, buf) {
//       if (err) throw err

//       driverBill.send({
//         msg: buf,
//         to: ted,
//         chain: {
//           public: true
//         }
//       })

//       if (cb) cb()
//     })
// }

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
