var mockery = require('mockery')
mockery.enable({
  warnOnReplace: false,
  warnOnUnregistered: false
})

mockery.registerSubstitute('q', 'bluebird-q')

// global.Buffer = require('../node_modules/buffer').Buffer
// var http = require('http')
// http.globalAgent.maxSockets = 100
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
var helpers = require('@tradle/test-helpers')
var constants = require('@tradle/constants')
var utils = require('../lib/utils')
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
var bitcoin = require('@tradle/bitcoinjs-lib')
var ONE
var MANY
var STORAGE_PATH = './storage'
var sharedKeeper = FakeKeeper.empty()
var numUsers = Number(process.argv[2]) || 10
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

var CACHE_PATH = './test/cache.json'
try {
  var CACHE = fs.readFileSync(CACHE_PATH)
  CACHE = JSON.parse(CACHE.toString('binary'))
  CACHE = require('logbase').rebuf(CACHE)
} catch (err) {
  CACHE = {}
}

var serverInfo = users.shift()
var serverHash = serverInfo.rootHash
var TimeMethod = require('time-method')
var timTimer
// var processTimer = TimeMethod.timerFor(process)
// processTimer.time('nextTick')

init()

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

function init () {
  // TODO: skip initializing tims
  var identities = users.slice(0, numUsers)
  var uncached = identities.filter(function (userInfo) {
    return !CACHE[userInfo.rootHash]
  })

  if (uncached.length) {
    console.log('caching', uncached.length, 'uncached')
    var batches = []
    var batchSize = 25
    for (var i = 0; i < uncached.length; i += batchSize) {
      batches.push(uncached.slice(i, i + batchSize))
    }

    return batches.reduce(function (prev, batch) {
      return prev.then(function () {
        console.log('caching', batch.length)
        return Q.all(batch.map(sendAndCache))
      })
    }, Q())
    .done(function () {
      fs.writeFileSync(CACHE_PATH, JSON.stringify(CACHE))
      process.exit(0)
    })
  }

  return Q.all(identities.map(function (userInfo, i) {
      var cached = CACHE[userInfo.rootHash]
      var httpClient = new Transport.HttpClient({ rootHash: userInfo.rootHash })
      httpClient.addRecipient(serverHash, 'http://127.0.0.1:' + BASE_PORT + '/' + serverHash)

      var defer = Q.defer()
      var name = userInfo.pub.name.formatted

      console.time(name + ' roundtrip')
      httpClient.send(serverHash, cached, serverInfo).done()
      httpClient.once('message', function () {
        console.timeEnd(name + ' roundtrip')
        defer.resolve()
      })

      return defer.promise
    }))
    .catch(function (err) {
      console.error(err)
      throw err
    })
    .done()

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
      keeper: newKeeper(),
      _send: function (rh, msg) {
        CACHE[user.myRootHash()] = msg
        defer.resolve()
        return Q()
      }
    }))

    user.ready()
    .then(function () {
      return user.addContactIdentity(serverInfo.pub)
    })
    .then(function () {
      user.send({
        msg: {
          _t: 'tradle.SimpleMessage',
          _z: '' + i,
          hey: 'ho'
        },
        to: [{ _r: serverHash }],
        deliver: true
      })
    })

    return defer.promise
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

function newKeeper () {
  var k = FakeKeeper.empty()
  var getOne = k.getOne
  k.getOne = function (key) {
    return getOne.apply(this, arguments)
      .catch(function (err) {
        return sharedKeeper.getOne(key)
      })
  }

  k.push = function (opts) {
    typeforce({
      key: 'String',
      value: 'Buffer'
    }, opts)

    return sharedKeeper.put(opts.key, opts.value)
  }

  return k
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
