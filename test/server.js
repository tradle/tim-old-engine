
var mockery = require('mockery')
mockery.enable({
  warnOnReplace: false,
  warnOnUnregistered: false
})

mockery.registerSubstitute('q', 'bluebird-q')

var http = require('http')
// http.globalAgent.maxSockets = 100
var path = require('path')
var test = require('tape')
var find = require('array-find')
var typeforce = require('typeforce')
var extend = require('xtend')
var rimraf = require('rimraf')
var clone = require('clone')
var Q = require('q')
var express = require('express')
var leveldown = require('leveldown')
var helpers = require('@tradle/test-helpers')
var constants = require('@tradle/constants')
var ROOT_HASH = constants.ROOT_HASH
var Identity = require('@tradle/identity').Identity
var FakeKeeper = helpers.fakeKeeper
var fakeWallet = helpers.fakeWallet
var Transport = require('@tradle/transport-http')
var Tim = require('../')
var DAY_MILLIS = 24 * 3600 * 1000
Tim.CATCH_UP_INTERVAL = DAY_MILLIS
Tim.enableOptimizations()
var NONCE = 0
var BASE_PORT = 22222
var networkName = 'testnet'
var users = require('./users')
var bitcoin = require('@tradle/bitcoinjs-lib')
var kiki = require('@tradle/kiki')

// pre-cache key-parsing
users.forEach(function (u) {
  u.pub.pubkeys.forEach(function (k) {
    kiki.toKey(k)
  })
})

var ONE
var MANY
var STORAGE_PATH = './server-storage'
var sharedKeeper = FakeKeeper.empty()
var commonOpts = {
  syncInterval: DAY_MILLIS,
  chainThrottle: DAY_MILLIS,
  unchainThrottle: DAY_MILLIS,
  sendThrottle: DAY_MILLIS,
  networkName: networkName,
  leveldown: leveldown
}

var TimeMethod = require('time-method')
// var timTimer = TimeMethod.timeFunctions(Tim.prototype)
// var processTimer = TimeMethod.timerFor(process)
// processTimer.time('nextTick')

init()
// setInterval(process.exit.bind(process, 0), 10000)
process.on('SIGINT', function () {
  // printStats(timTimer)
  process.exit(1)
})

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
  clearStorage()
  var userInfo = users[0]
  var wallet = walletFor(userInfo.priv, null, 'messaging')
  var pub = userInfo.pub
  var app = express()
  var router = express.Router()
  var server = app.listen(BASE_PORT)
  var httpServer = new Transport.HttpServer({
    router: router,
    receive: function (msg, from) {
      return respondTo(msg, from)
    }
  })

  var serverMan = new Tim(extend(commonOpts, {
    keys: userInfo.priv,
    identity: Identity.fromJSON(pub),
    pathPrefix: getPathPrefix(pub.name.firstName),
    wallet: wallet,
    blockchain: wallet.blockchain,
    keeper: newKeeper(),
    _send: function (rh, msg) {
      var recipientInfo = {}
      recipientInfo[ROOT_HASH] = serverMan.myRootHash()
      return httpServer.send(rh, msg, recipientInfo)
    }
  }))

  users.slice(1).forEach(function (u) {
    serverMan.addContactIdentity(u.pub)
  })

  serverMan.ready().then(function () {
    app.use('/' + serverMan.myRootHash(), router)
    console.log(serverMan.name(), 'is ready!')
  })

  function respondTo (msg, from) {
    var defer = Q.defer()
    serverMan.receiveMsg(msg, from).done()
    serverMan.on('message', handler)
    return defer.promise

    function handler (info) {
      if (info.from[ROOT_HASH] !== from[ROOT_HASH]) return

      serverMan.lookupObject(info)
        .then(function (obj) {
          var msg = obj.parsed.data
          msg.ack = true
          // TODO: sign
          return serverMan.send({
            msg: msg,
            to: [from],
            deliver: true
          })
        })
        .then(function (entries) {
          var entry = entries[0]
          serverMan.on('sent', function (info) {
            if (info.uid === entry.get('uid')) {
              serverMan.removeListener('message', handler)
              defer.resolve()
            }
          })
        })
        .done()

    }
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
