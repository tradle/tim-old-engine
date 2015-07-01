
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
var TYPE = constants.TYPE
var ROOT_HASH = constants.ROOT_HASH
// var tedHash = tedPub[ROOT_HASH] = 'c67905793f6cc0f0ab8d20aecfec441932ffb13d'
// var billHash = billPub[ROOT_HASH] ='fb07729c0cef307ab7c28cb76088cc60dbc98cdd'
// var tedHash = 'c67905793f6cc0f0ab8d20aecfec441932ffb13d'
// var billHash = 'fb07729c0cef307ab7c28cb76088cc60dbc98cdd'
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
//     driverBill._addressBook.clear(function () {
//       t.end()
//     })
//   })

//   var betterTed = extend(true, {}, tedPub)
//   var betterBill = extend(true, {}, billPub)
//   betterTed[ROOT_HASH] = tedHash
//   betterBill[ROOT_HASH] = billHash
//   parallel([
//     function (cb) {
//       driverBill._addressBook.update(betterTed, cb)
//     },
//     function (cb) {
//       driverTed._addressBook.update(betterBill, cb)
//     }
//   ], function (err) {
//     if (err) throw err

//     var tedInfo = {}
//     tedInfo[ROOT_HASH] = tedHash
//     driverBill.sendPlaintext({
//       msg: msg,
//       to: [tedInfo]
//     })
//   })
// })

test('chained message', function (t) {
  // t.timeoutAfter(15000)

  // chained msg
  var msg = {
    hey: 'ho'
  }

  msg[TYPE] = 'blahblah'
  var signed
  var num = 0
  var identitiesChained = 0

  driverBill.on('chained', function (obj) {
    Parser.parse(obj.data, function (err, parsed) {
      if (parsed.data[TYPE] === Identity.TYPE) onIdentityChained(parsed.data)

      // t.equal(parsed.data.name.firstName, 'Ted')
    })
  })

  // driverTed.on('chained', function (obj) {
  //   Parser.parse(obj.data, function (err, parsed) {
  //     if (parsed.data[TYPE] === Identity.TYPE) onIdentityChained(parsed.data)

  //     // t.equal(parsed.data.name.firstName, 'Bill')
  //   })
  // })

  function onIdentityChained (i) {
    console.log('DETECTED', i.name.formatted)
    if (++identitiesChained !== 4) return // 2 each, because both also detect themselves

    driverTed.on('resolved', function (obj) {
      debugger
      t.equal(obj, 'blah')
      t.end()
    })

    driverBill.on('resolved', function (obj) {
      debugger
      t.equal(obj, 'blah')
      t.end()
    })

    var billInfo = {
      fingerprint: billPub.pubkeys[0].fingerprint
    }

    debugger
    driverTed.sendStructured({
      msg: msg,
      to: [billInfo],
      sign: true,
      chain: true
    })
  }

  driverTed.publish({
    msg: tedPub,
    sign: true
  })

  driverBill.publish({
    msg: billPub,
    sign: true
  })

  // publishIdentity(billPub, driverBill.chainwriterq, rethrow)
})

test('teardown', function (t) {
  Q.all([
    driverBill.destroy(),
    driverTed.destroy()
  ]).done(t.end)
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
