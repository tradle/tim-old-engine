#!/usr/bin/env node

var mockery = require('mockery')
mockery.enable({
  warnOnReplace: false,
  warnOnUnregistered: false
})

mockery.registerSubstitute('q', 'bluebird-q')

var fs = require('fs')
var path = require('path')
var test = require('tape')
var find = require('array-find')
var typeforce = require('typeforce')
var extend = require('xtend')
var rimraf = require('rimraf')
var clone = require('clone')
var Q = require('q')
var express = require('express')
var leveldown = require('memdown')
var argv = require('minimist')(process.argv.slice(2), {
  alias: {
    g: 'gen',
    n: 'number',
    s: 'sign',
    c: 'cache-path'
  },
  boolean: ['gen']
})

var numUsers = Number(argv.number)
if (!numUsers) {
  console.log('-n value must be positive')
  process.exit(1)
}

var cachePath = argv['cache-path']
if (!cachePath) {
  throw new Error('expected "cache-path"')
}

cachePath = path.resolve(cachePath)

var bitcoin = require('@tradle/bitcoinjs-lib')
var helpers = require('@tradle/test-helpers')
var constants = require('@tradle/constants')
var utils = require('../lib/utils')
var tradleUtils = require('@tradle/utils')
var ROOT_HASH = constants.ROOT_HASH
var Identity = require('@tradle/identity').Identity
var FakeKeeper = helpers.fakeKeeper
var fakeWallet = helpers.fakeWallet
var Transport = require('@tradle/transport-http')
var Tim = require('../')
Tim.enableOptimizations()

var NONCE = 0
var BASE_PORT = 22222
var networkName = 'testnet'
var users = require('./users')
var ONE
var MANY
var STORAGE_PATH = './storage'
var sharedKeeper = FakeKeeper.empty()
var DAY_MILLIS = 24 * 60 * 1000 * 1000
Tim.CATCH_UP_INTERVAL = DAY_MILLIS
var commonOpts = {
  syncInterval: DAY_MILLIS,
  chainThrottle: DAY_MILLIS,
  unchainThrottle: DAY_MILLIS,
  sendThrottle: DAY_MILLIS,
  networkName: networkName,
  leveldown: leveldown
}

var serverInfo = users.shift()
var serverHash = serverInfo.rootHash

run()

function printStats (timer) {
  var stats = timer.getStats()
    // .filter(function (s) {
    //   return s.time > 200000000 // 200 ms
    // })

  stats.forEach(function (s) {
    s.time /= 1e6
    s.timePerInvocation /= 1e6
  })

  console.log(stats)
  timer.reset()
}

function clearStorage () {
  rimraf.sync(path.join(STORAGE_PATH, '*'))
}

function run () {
  // TODO: skip initializing tims
  var identities = users.slice(0, numUsers)
  if (argv.gen) {
    genMessages()
  } else {
    sendMessages()
  }

  function genMessages () {
    if (identities.length < numUsers) {
      throw new Error('not enough users')
    }

    var CACHE = {}
    console.log('generating', numUsers, 'messages')
    var batches = []
    var batchSize = 25
    for (var i = 0; i < numUsers; i += batchSize) {
      batches.push(identities.slice(i, i + batchSize))
    }

    return batches.reduce(function (prev, batch) {
      return prev.then(function () {
        return Q.all(batch.map(sendAndCache))
      })
    }, Q())
    .done(function () {
      fs.writeFileSync(cachePath, JSON.stringify(CACHE))
      process.exit(0)
    })

    function sendAndCache (userInfo, i) {
      var defer = Q.defer()
      var wallet = walletFor(userInfo.priv, null, 'messaging')
      var pub = userInfo.pub
      var user = new Tim(extend(commonOpts, {
        keys: userInfo.priv,
        identity: Identity.fromJSON(pub),
        pathPrefix: getPathPrefix(pub.name.firstName),
        wallet: wallet,
        blockchain: wallet.blockchain,
        keeper: keeperWithFallback(sharedKeeper),
        _send: function (rh, msg) {
          CACHE[user.myRootHash()] = msg.toString()
          // pretend we sent
          // we're only generating messages
          // but the caller expects to get a promise back
          defer.resolve()
          return defer.promise
        }
      }))

      user.ready()
        .then(function () {
          return user.addContactIdentity(serverInfo.pub)
        })
        .then(function () {
          var msg = {
            _t: 'tradle.SimpleMessage',
            _z: '' + i,
            hey: 'ho'
          }

          return argv.sign
            ? user.sign(msg)
            : msg
        })
        .then(function (signed) {
          return user.send({
            msg: signed,
            to: [{ _r: serverHash }],
            deliver: true
          })
        })

      return defer.promise
    }
  }

  function sendMessages () {
    var CACHE = fs.readFileSync(cachePath)
    CACHE = JSON.parse(CACHE)
    for (var key in CACHE) {
      CACHE[key] = new Buffer(CACHE[key])
    }

    var times = identities.map(function (i) {}) // pre-fill with undefined
    var togo = identities.length
    identities.forEach(function (userInfo, i) {
      var cached = CACHE[userInfo.rootHash]
      var httpClient = new Transport.HttpClient({ rootHash: userInfo.rootHash })
      httpClient.addRecipient(serverHash, 'http://127.0.0.1:' + BASE_PORT + '/' + serverHash)

      var name = userInfo.pub.name.formatted
      var start = process.hrtime()
      httpClient.send(serverHash, cached, serverInfo).done()
      httpClient.once('message', function () {
        var time = process.hrtime(start)
        times[i] = time[0] * 1e3 + time[1] / 1e6 | 0
        if (--togo === 0) {
          printTotals(times)
        }
      })
    })
  }

  function printTotals (times) {
    var sum = times.reduce(function (a, b) { return a + b })
    var avg = sum / times.length | 0
    var min = Math.min.apply(Math, times)
    var max = Math.max.apply(Math, times)
    console.log('avg roundtrip time', avg)
    console.log('min roundtrip time', min)
    console.log('max roundtrip time', max)
  }
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

function randomMessage () {
  return {
    _t: 'tradle.SimpleMessage',
    _z: '' + NONCE++,
    message: 'something random: ' + Math.random()
  }
}

function getPathPrefix (name) {
  return path.join(STORAGE_PATH, name.replace(/\s/g, ''))
}

function keeperWithFallback (fallbackKeeper) {
  var k = FakeKeeper.empty()
  var getOne = k.getOne
  k.getOne = function (key) {
    return getOne.apply(this, arguments)
      .catch(function (err) {
        return fallbackKeeper.getOne(key)
      })
  }

  k.push = function (opts) {
    typeforce({
      key: 'String',
      value: 'Buffer'
    }, opts)

    return fallbackKeeper.put(opts.key, opts.value)
  }

  return k
}
