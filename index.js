var EventEmitter = require('events').EventEmitter
var assert = require('assert')
var util = require('util')
var omit = require('object.omit')
var Q = require('q')
var typeforce = require('typeforce')
var debug = require('debug')('tim')
var reemit = require('re-emitter')
var bitcoin = require('bitcoinjs-lib')
var extend = require('xtend/mutable')
var collect = require('stream-collector')
var tutils = require('tradle-utils')
var map = require('map-stream')
var pump = require('pump')
var find = require('array-find')
var deepEqual = require('deep-equal')
var ChainedObj = require('chained-obj')
var TxData = require('tradle-tx-data').TxData
var ChainWriter = require('bitjoe-js')
var ChainLoader = require('chainloader')
var Permission = require('tradle-permission')
var Wallet = require('simple-wallet')
var cbstreams = require('cb-streams')
var Zlorp = require('zlorp')
var Messengers = require('./lib/messengers')
var mi = require('midentity')
var Identity = mi.Identity
var kiki = require('kiki')
var toKey = kiki.toKey
var Builder = ChainedObj.Builder
var Parser = ChainedObj.Parser
var lb = require('logbase')
var Entry = lb.Entry
var unchainer = require('./lib/unchainer')
var constants = require('tradle-constants')
var EventType = require('./lib/eventType')
var Dir = require('./lib/dir')
var createIdentityDB = require('./lib/identityDB')
var createMsgDB = require('./lib/msgDB')
var createTxDB = require('./lib/txDB')
var Errors = require('./lib/errors')
var utils = require('./lib/utils')
var RETRY_UNCHAIN_ERRORS = [
  ChainLoader.Errors.ParticipantsNotFound,
  ChainLoader.Errors.FileNotFound
].map(function (ErrType) {
  return ErrType.type
})

var TYPE = constants.TYPE
var SIGNEE = constants.SIGNEE
var ROOT_HASH = constants.ROOT_HASH
var PREV_HASH = constants.PREV_HASH
var CUR_HASH = constants.CUR_HASH
var PREFIX = constants.OP_RETURN_PREFIX
var NONCE = constants.NONCE
var CONFIRMATIONS_BEFORE_CONFIRMED = 10
var MAX_CHAIN_RETRIES = 3
var MAX_UNCHAIN_RETRIES = 10
var MAX_RESEND_RETRIES = 10
var MIN_BALANCE = 10000
var KEY_PURPOSE = 'messaging'
Driver.CHAIN_WRITE_THROTTLE = 60000
Driver.CHAIN_READ_THROTTLE = 60000
Driver.CATCH_UP_INTERVAL = 2000
var noop = function () {}

// var MessageType = Driver.MessageType = {
//   plaintext: 1 << 1,
//   chained: 1 << 2
// }

module.exports = Driver
util.inherits(Driver, EventEmitter)

// TODO: export other deps
Driver.Zlorp = Zlorp
Driver.Kiki = kiki
Driver.Identity = Identity
Driver.Wallet = Wallet
Driver.Messengers = Messengers

function Driver (options) {
  var self = this

  typeforce({
    // maybe allow read-only mode if this is missing
    // TODO: replace with kiki (will need to adjust otr, zlorp for async APIs)
    identityKeys: 'Array',
    identity: 'Identity',
    blockchain: 'Object',
    networkName: 'String',
    keeper: 'Object',
    dht: 'Object',
    leveldown: 'Function',
    port: 'Number',
    pathPrefix: 'String',
    messenger: '?Object',
    syncInterval: '?Number',
    chainThrottle: '?Number',
    readOnly: '?Boolean',
    relay: '?Object',
    afterBlockTimestamp: '?Number'
  }, options)

  EventEmitter.call(this)
  tutils.bindPrototypeFunctions(this)
  extend(this, options)

  this._otrKey = toKey(
    this.getPrivateKey({
      type: 'dsa',
      purpose: 'sign'
    })
  )

  this._signingKey = toKey(
    this.getPrivateKey({
      type: 'ec',
      purpose: 'sign'
    })
  )

  // copy
  this.identityMeta = {}

  this.setIdentity(options.identity.toJSON())
  this.afterBlockTimestamp = this.afterBlockTimestamp || 0
  if (this.afterBlockTimestamp) {
    this._debug('ignoring txs before', new Date(this.afterBlockTimestamp * 1000).toString())
  }

  var networkName = this.networkName
  var keeper = this.keeper
  var dht = this.dht
  var blockchain = this.blockchain
  var leveldown = this.leveldown
  var wallet = this.wallet = this.wallet || new Wallet({
    networkName: networkName,
    blockchain: blockchain,
    priv: this.getBlockchainKey().priv
  })

  // init balance while we rely on blockr for this info
  this._balance = 0

  // this._monkeypatchWallet()
  this.messenger = options.messenger || new Messengers.P2P({
    zlorp: new Zlorp({
      name: this.name(),
      available: true,
      leveldown: leveldown,
      port: this.port,
      dht: dht,
      key: this._otrKey.priv(),
      relay: this.relay
    })
  })

  this.messenger.on('message', this.receiveMsg)

  typeforce({
    send: 'Function',
    on: 'Function',
    removeListener: 'Function',
    destroy: 'Function'
  }, this.messenger)

  this.chainwriter = new ChainWriter({
    wallet: wallet,
    keeper: keeper,
    networkName: networkName,
    minConf: 0,
    prefix: PREFIX
  })

  this.chainloader = new ChainLoader({
    keeper: keeper,
    networkName: networkName,
    prefix: PREFIX,
    lookup: this.getKeyAndIdentity2
  })

  this.unchainer = unchainer({
    chainloader: this.chainloader,
    lookup: this.lookupByDHTKey
  })

  // in-memory cache of recent conversants
  this._catchUpWithBlockchain()
  this._fingerprintToIdentity = {}
  this._pendingTxs = []

  this._setupDBs()
  var keeperReadyDfd = Q.defer()
  var keeperReady = keeperReadyDfd.promise
  if (!keeper.isReady || keeper.isReady()) keeperReadyDfd.resolve()
  else keeper.once('ready', keeperReadyDfd.resolve)

  this._readyPromise = Q.all([
      self._prepIdentity(),
      self._setupTxStream(),
      keeperReady,
      this._updateBalance()
        .catch(function (e) {
          self._debug('unable to get balance')
        })
    ])
    .then(function () {
      if (self._destroyed) {
        return Q.reject(new Error('destroyed'))
      }

      self.msgDB.start()
      self.txDB.start()
      self.addressBook.start()

      self._ready = true
      self._debug('ready')
      self.emit('ready')
      // self.publishMyIdentity()
      self._writeToChain()
      self._readFromChain()
      self._sendTheUnsent()
      // self._watchMsgStatuses()
    })
}

Driver.prototype.ready = function () {
  return this._readyPromise
}

Driver.prototype.isReady = function () {
  return this._ready
}

Driver.prototype._prepIdentity = function () {
  var self = this
  return utils.getDHTKey(this.identityJSON)
    .then(function (key) {
      copyDHTKeys(self.identityMeta, key)
    })
}

Driver.prototype._updateBalance = function () {
  var self = this
  return Q.ninvoke(this.wallet, 'balance')
    .then(function (balance) {
      self._balance = balance
    })
}

/**
 * read from chain
 */
Driver.prototype._readFromChain = function () {
  var self = this

  if (this._destroyed) return
  if (!this.txDB.isLive()) {
    return this.txDB.once('live', this._readFromChain)
  }

  if (this._queuedUnchains) return

  this._queuedUnchains = {}

  pump(
    this.txDB.liveStream({
      old: true,
      tail: true
    }),
    toObjectStream(),
    map(function (entry, cb) {
      // was read from chain and hasn't been processed yet
      // self._debug('unchaining tx', entry.txId)
      var txId = entry.txId
      if (!entry.dateDetected || entry.dateUnchained) {
        return finish()
      }

      // clear old errors
      var errs = entry.errors
      delete entry.errors

      if (!errs || !errs.length) return finish(null, entry)

      var shouldTryAgain = errs.every(function (err) {
        return RETRY_UNCHAIN_ERRORS.indexOf(err.type) !== -1
      })

      if (!shouldTryAgain) return finish()

      if (errs.length >= MAX_UNCHAIN_RETRIES) {
        // console.log(entry.errors, entry.id)
        self._debug('skipping unchain after', errs.length, 'errors for tx:', txId)
        self._remove(entry)
        return finish()
      }

      if (self._queuedUnchains[txId]) {
        return self._debug('already schedulded unchaining!')
      }

      self._queuedUnchains[txId] = true
      self._debug('throttling unchain retry of tx', txId)
      throttleIfRetrying(errs, Driver.CHAIN_READ_THROTTLE, function () {
        finish(null, entry)
      })

      function finish (err, ret) {
        if (err || !ret) self._rmPending(txId)

        delete self._queuedUnchains[txId]
        cb(err, ret)
      }
    }),
    this.unchainer,
    map(function (chainedObj, cb) {
      // if (chainedObj.parsed) {
      //   self._debug('unchained (read)', chainedObj.key, chainedObj.errors)
      // }

      // if (!chainedObj.errors.length && chainedObj.parsed) {
      //   if (chainedObj.txType === TxData.types.public) {
      //     self.keeper.put(chainedObj.key, chainedObj.data)
      //       .then(function () {
      //         self._push(chainedObj.key, chainedObj.data)
      //       })
      //   } else {
      //     self.keeper.put(chainedObj.key, chainedObj.encryptedData)
      //     self.keeper.put(chainedObj.permissionKey, chainedObj.encryptedPermission)
      //   }
      // }

      self.unchainResultToEntry(chainedObj)
        .done(function (entry) {
          self._rmPending(entry.txId)
          cb(null, entry)
        })
    }),
    jsonifyTransform(),
    this._log,
    this._rethrow
  )
}

Driver.prototype._remove = function (info) {
  var self = this
  this.lookupObject(info)
    .catch(function (err) {
      return err.progress
    })
    .then(function (chainedObj) {
      var tasks = ['key', 'permissionKey']
        .map(function (p) {
          return chainedObj[p]
        })
        .filter(function (key) {
          return !!key
        })

      return Q.all(tasks)
    })
}

Driver.prototype._rmPending = function (txId) {
  var idx = this._pendingTxs.indexOf(txId)
  if (idx !== -1) {
    this._pendingTxs.splice(idx, 1)
  }
}

Driver.prototype._rethrow = function (err) {
  if (err) {
    this._debug('experienced an error', err)
    if (!this._destroyed && err) throw err
  }
}

Driver.prototype._catchUpWithBlockchain = function () {
  var self = this
  if (this._caughtUpPromise) return this._caughtUpPromise

  var done
  var txIds
  var catchUp = Q.defer()
  this._caughtUpPromise = catchUp.promise
    .then(function () {
      self._debug('caught up with blockchain')
    })
    .finally(function () {
      done = true
    })

  tryAgain()
  return this._caughtUpPromise

  function tryAgain () {
    if (self._destroyed) return
    if (txIds) return checkDBs()

    var stream = cbstreams.stream.txs({
      api: self.blockchain,
      confirmations: CONFIRMATIONS_BEFORE_CONFIRMED,
      addresses: [
        self.wallet.addressString,
        constants.IDENTITY_PUBLISH_ADDRESS
      ]
    })

    stream.on('error', function (err) {
      if (/no txs/.test(err.message)) {
        done = true
        stream.close()
        catchUp.resolve()
      }
    })

    collect(stream, function (err, txInfos) {
      if (done) return

      if (err) return scheduleRetry()

      txIds = txInfos
        .filter(function (txInfo) {
          return txInfo.blockTimestamp > self.afterBlockTimestamp
        })
        .map(function (txInfo) {
          return txInfo.tx.getId()
        })

      checkDBs()
    })
  }

  function checkDBs () {
    var tasks = txIds.map(function (txId) {
      return Q.ninvoke(self.txDB, 'get', txId)
        .then(function (entry) {
          // var erroredOut
          // if (entry.errors && entry.errors.length) {
          //   var maxRetries = entry.dir === Dir.outbound
          //     ? MAX_CHAIN_RETRIES
          //     : MAX_UNCHAIN_RETRIES

          //   erroredOut = entry.errors.length >= maxRetries
          // }

          var hasErrors = entry.errors && entry.errors.length
          var processed = entry.dateUnchained || entry.dateChained || hasErrors
          if (!processed) throw new Error('not ready')
        })
    })

    Q.all(tasks)
      .then(catchUp.resolve)
      .catch(scheduleRetry)
  }

  function scheduleRetry () {
    self._debug('not caught up yet with blockchain...')
    setTimeout(tryAgain, Driver.CATCH_UP_INTERVAL)
  }
}

Driver.prototype.identityPublishStatus = function () {
  var self = this
  // check if we've already chained it
  if (!this._ready) {
    return this._readyPromise.then(this.identityPublishStatus)
  }

  var rh = this.myRootHash()
  var me = this.identityJSON
  var status = {
    ever: false,
    current: false,
    queued: false
  }

  return this._catchUpWithBlockchain()
    .then(function () {
      return Q.all([
        Q.ninvoke(self.msgDB, 'byRootHash', rh),
        Q.ninvoke(tutils, 'getStorageKeyFor', utils.toBuffer(me))
      ])
    })
    .spread(function (entries, curHash) {
      curHash = curHash.toString('hex')

      var unchained = entries.filter(function (e) {
        return e.dateUnchained
      })

      status.ever = !!unchained.length
      status.current = unchained.some(function (e) {
        return e[CUR_HASH] === curHash
      })

      status.queued = !status.current && entries.some(function (e) {
        return e[CUR_HASH] === curHash
      })

      // curHash = curHash.toString('hex')
      // status.ever = true

      // var timestamp = 0
      // var last
      // entries.forEach(function (e) {
      //   if (e.timestamp > timestamp) {
      //     last = e
      //     timestamp = e.timestamp
      //   }
      // })

      // if (last[CUR_HASH] === curHash) {
      //   status.queued = true
      //   if (last.tx) {
      //     status.current = true
      //   }
      // }

      return status
    })
    .catch(function (err) {
      if (!err.notFound) throw err

      return status
    })
}

Driver.prototype.publishIdentity = function (identity) {
  identity = identity || this.identityJSON
  return this.publish({
    msg: identity,
    to: [{ fingerprint: constants.IDENTITY_PUBLISH_ADDRESS }]
  })
}

Driver.prototype.setIdentity = function (identityJSON) {
  if (deepEqual(this.identityJSON, identityJSON)) return

  this.identity = Identity.fromJSON(identityJSON)
  this.identityJSON = this.identity.toJSON()
}

Driver.prototype.publishMyIdentity = function () {
  var self = this

  if (this._publishingIdentity) {
    throw new Error('wait till current publishing process ends')
  }

  if (!this._ready) {
    return this._readyPromise.then(this.publishMyIdentity)
  }

  this._publishingIdentity = true
  return this.identityPublishStatus()
    .then(function (status) {
      if (!status.ever) return self.publishIdentity()
      if (status.queued) {
        return Q.reject(new Error('already publishing this version'))
      }

      if (status.current) {
        return Q.reject(new Error('already published this version'))
      }

      return publish()
    })
    .finally(function () {
      delete self._publishingIdentity
    })

  function publish () {
    var priv = self.getPrivateKey({ purpose: 'update' })
    var update = extend({}, self.identityJSON)
    var prevHash = self.myCurrentHash() || self.myRootHash()
    utils.updateChainedObj(update, prevHash)

    return Q.ninvoke(Builder, 'addNonce', update)
      .then(function (nonce) {
        var builder = Builder()
          .data(update)
          .signWith(toKey(priv))

        return Q.ninvoke(builder, 'build')
      })
      .then(function (result) {
        self.setIdentity(update)
        extend(self.identityMeta, utils.pick(update, PREV_HASH, ROOT_HASH))
        return Q.all([
          self._prepIdentity(),
          self.publishIdentity(result.form)
        ])
      })
  }
}

Driver.prototype.identities = function () {
  return this.addressBook
}

Driver.prototype.messages = function () {
  return this.msgDB
}

Driver.prototype.decryptedMessagesStream = function () {
  var self = this
  return this.msgDB.createValueStream()
    .pipe(map(function (info, cb) {
      // console.log(info)
      self.lookupObject(info)
        .nodeify(cb)
    }))
}

Driver.prototype.transactions = function () {
  return this.txDB
}

Driver.prototype.unchainResultToEntry = function (chainedObj) {
  var self = this
  var success = !(chainedObj.errors && chainedObj.errors.length)
  var type = success ?
    EventType.chain.readSuccess :
    EventType.chain.readError

  // no decrypted data should be stored in the log
  var safeProps = omit(chainedObj, [
    'type',
    'parsed',
    'key',
    'data',
    'encryptedData', // stored in keeper
    'permission',
    'encryptedPermission' // stored in keeper
  ])

  var entry = new Entry(safeProps)
    .set('type', type)

  if ('key' in chainedObj) {
    entry.set(CUR_HASH, chainedObj.key)
  }

  if ('parsed' in chainedObj) {
    entry
      .set(ROOT_HASH, chainedObj.parsed.data[ROOT_HASH] || chainedObj.key)
      .set(TYPE, chainedObj.parsed.data[TYPE])
      .set('public', chainedObj.type === TxData.types.public)
  }

  if ('tx' in chainedObj) {
    entry.set('tx', utils.toBuffer(chainedObj.tx))
  }

  // if ('id' in chainedObj) {
  //   entry.prev(chainedObj.id)
  // }

  var tasks = ['from', 'to'].map(function (party) {
    return chainedObj[party]
  })
  .map(function (party) {
    if (!party) return

    if (party[ROOT_HASH]) return party[ROOT_HASH]

    // a bit scary
    var fingerprint = party.identity.keys()[0].toJSON().fingerprint
    return self.lookupRootHash(fingerprint)
  })

  return Q.allSettled(tasks)
    .spread(function (from, to) {
      if (from.value) {
        entry.set('from', utils.toObj(ROOT_HASH, from.value))
      }

      if (to.value) {
        entry.set('to', utils.toObj(ROOT_HASH, to.value))
      }

      if (success) utils.setUID(entry)
      return entry
    })
}

Driver.prototype._getToChainStream = function () {
  return pump(
    this.msgDB.liveStream({
      old: true,
      tail: true
    }),
    toObjectStream(),
    utils.filterStream(function (entry) {
      return !entry.tx &&
              entry.chain &&
             !entry.dateChained &&
              entry.dir === Dir.outbound &&
              (!entry.errors || entry.errors.length < MAX_CHAIN_RETRIES)
    }),
    this._rethrow
  )
}

/**
 * write to chain
 */
Driver.prototype._writeToChain = function () {
  var self = this
  if (this._destroyed) return

  var db = this.msgDB

  if (!db.isLive()) return db.once('live', this._writeToChain)
  if (this._chaining) return

  this._chaining = true
  var throttle = this.chainThrottle || Driver.CHAIN_WRITE_THROTTLE

  pump(
    this._getToChainStream(),
    map(function (entry, cb) {
      var errs = entry.errors
      if (errs && errs.length) {
        self._debug('throttling chaining')
      }

      // don't write same errors into next log entry
      delete entry.errors
      throttleIfRetrying(errs, throttle, tryAgain)

      function tryAgain () {
        if (self._balance < MIN_BALANCE) {
          self.emit('lowbalance')
          return tryAgainSoon()
        }

        var nextEntry
        self.putOnChain(new Entry(entry))
          .then(function (_nextEntry) {
            nextEntry = _nextEntry
            return self._updateBalance()
          })
          .done(function () {
            cb(null, nextEntry)
          })
      }

      function tryAgainSoon () {
        self._debug('paused chaining, low balance')
        var timeout = setTimeout(function () {
          if (self._destroyed) return

          self._updateBalance()
            .finally(tryAgain)
        }, throttle)

        if (timeout.unref) timeout.unref()
      }
    }),
    // filter(function (data) {
    //   console.log('after chain write', data.toJSON())
    //   return true
    // }),
    jsonifyTransform(),
    this._log,
    this._rethrow
  )
}

Driver.prototype._getUnsentStream = function (options) {
  var self = this
  return pump(
    this.msgDB.liveStream(extend({
      tail: true,
      old: true
    }, options || {})),
    toObjectStream(),
    utils.filterStream(function (entry) {
      if (entry.dateSent ||
//           entry.txType === TxData.types.public ||
          !entry.to ||
          !entry.deliver ||
          entry.dir !== Dir.outbound ||
          self._currentlySending.indexOf(entry.uid) !== -1) {
        return
      }

      if (entry.errors && entry.errors.length > MAX_RESEND_RETRIES) {
        if (entry.errors.length === MAX_RESEND_RETRIES) {
          self._debug('giving up on sending message', entry)
        }

        return
      }

      return true
    }),
    this._rethrow
  )
}

Driver.prototype.name = function () {
  var name = this.identityJSON.name
  if (name) {
    return name.firstName
  } else {
    return this.identityJSON.pubkeys[0].fingerprint
  }
}

Driver.prototype._markSending = function (entry) {
  this._currentlySending.push(entry.uid)
}

Driver.prototype._markNotSending = function (entry) {
  var idx = this._currentlySending.indexOf(entry.uid)
  this._currentlySending.splice(idx, 1)
}

Driver.prototype._sendTheUnsent = function () {
  var self = this
  if (this._destroyed) return

  var db = this.msgDB

  if (!db.isLive()) return db.once('live', this._sendTheUnsent)

  if (this._sending) return

  this._sending = true
  this._currentlySending = []
  this.msgDB.on('sent', this._markNotSending)

  pump(
    this._getUnsentStream(),
    map(function (entry, cb) {
      var nextEntry = new Entry()
        .set(ROOT_HASH, entry[ROOT_HASH])
        .set('uid', entry.uid)

      self._markSending(entry)
      self._doSend(entry)
        .then(function () {
          return nextEntry.set('type', EventType.msg.sendSuccess)
        })
        .catch(function (err) {
          nextEntry.set({
            type: EventType.msg.sendError
          })

          utils.addError(nextEntry, err)
          self._markNotSending(nextEntry)
          return nextEntry
        })
        .done(function (entry) {
          cb(null, entry)
        })
    }),
    // filter(function (data) {
    //   console.log('after sendTheUnsent', data.toJSON())
    //   return true
    // }),
    jsonifyTransform(),
    this._log,
    this._rethrow
  )
}

Driver.prototype._setupTxStream = function () {
  // Uncomment when Blockr supports querying by height

  // TODO: use txDB for this instead
  // var self = this
  // var defer = Q.defer()
  // var lastBlock
  // var lastBlockTxIds = []
  // var chainTypes = EventType.chain
  // var rs = this._log.createReadStream({ reverse: true })
  //   .pipe(filter(function (entry, cb) {
  //     var eType = entry.get('type')
  //     return eType === chainTypes.readSuccess || eType === chainTypes.readError
  //   }))
  //   .on('data', function (entry) {
  //     var txId = bitcoin.Transaction.fromBuffer(entry.get('tx')).getId()
  //     lastBlockTxIds.unshift(txId)
  //     if (typeof lastBlock === 'undefined') {
  //       lastBlock = entry.get('height')
  //     } else {
  //       if (entry.get('height') < lastBlock) {
  //         rs.destroy()
  //       }
  //     }
  //   })
  //   .on('error', this._rethrow)
  //   .once('close', function () {
  //     // start CONFIRMATIONS_BEFORE_CONFIRMED blocks back
  //     lastBlock = lastBlock || 0
  //     lastBlock = Math.max(0, lastBlock - CONFIRMATIONS_BEFORE_CONFIRMED)
  //     self._streamTxs(lastBlock, lastBlockTxIds)
  //     defer.resolve()
  //   })
  //
  // return defer.promise

  this._streamTxs(0, [])
  return Q.resolve()
}

Driver.prototype._streamTxs = function (fromHeight, skipIds) {
  var self = this
  if (this._destroyed) return

  if (!fromHeight) fromHeight = 0

  this._rawTxStream = cbstreams.stream.txs({
    live: true,
    interval: this.syncInterval || 60000,
    api: this.blockchain,
    height: fromHeight,
    confirmations: CONFIRMATIONS_BEFORE_CONFIRMED,
    addresses: [
      this.wallet.addressString,
      constants.IDENTITY_PUBLISH_ADDRESS
    ]
  })

  pump(
    this._rawTxStream,
    map(function (txInfo, cb) {
      // TODO: make more efficient
      // when we have a source of txs that
      // consistently provides block height
      if (txInfo.blockTimestamp &&
          txInfo.blockTimestamp <= self.afterBlockTimestamp) {
        return cb()
      }

      var id = txInfo.tx.getId()
      if (self._pendingTxs.indexOf(id) !== -1) {
        return cb() // already handled this one
      }

      self._pendingTxs.push(id)
      self.txDB.get(id, function (err, entry) {
        var unexpectedErr = err && !err.notFound
        if (unexpectedErr) {
          self._debug('unexpected txDB error: ' + JSON.stringify(unexpectedError))
        }

        var handled = entry && entry.confirmations > CONFIRMATIONS_BEFORE_CONFIRMED
        if (unexpectedErr || handled) {
          self._rmPending(id)
          return cb()
        }

        save(entry)
      })

      function save (entry) {
        var type = entry ?
          EventType.tx.confirmation :
          EventType.tx.new
          // we may have chained this tx
          // but this is the first time we're
          // getting it FROM the chain

        var nextEntry = new Entry(extend(entry || {}, txInfo, {
          type: type,
          txId: id,
          tx: utils.toBuffer(txInfo.tx),
          dir: self._getTxDir(txInfo.tx)
        }))

        // clear errors
        nextEntry.set('errors', [])

        // if (entry) nextEntry.prev(new Entry(entry))
        cb(null, nextEntry)
      }
    }),
    jsonifyTransform(),
    this._log,
    this._rethrow
  )
}

Driver.prototype._setupDBs = function () {
  var self = this

  this._log = new lb.Log(this._prefix('msg-log.db'), {
    db: this.leveldown
  })

  this._log.setMaxListeners(0)

  this.addressBook = createIdentityDB(this._prefix('addressBook.db'), {
    leveldown: this.leveldown,
    log: this._log,
    keeper: this.keeper,
    timeout: false,
    autostart: false
  })

  this.addressBook.name = this.name()
  this._identityCache = {}
  this.msgDB = createMsgDB(this._prefix('messages.db'), {
    leveldown: this.leveldown,
    log: this._log,
    timeout: false,
    autostart: false
  })

  this.msgDB.name = this.name()

  var msgDBEvents = [
    'chained',
    'unchained',
    'message',
    'sent'
  ]

  reemit(this.msgDB, this, msgDBEvents)
  ;['message', 'unchained'].forEach(function (event) {
    self.msgDB.on(event, function (entry) {
      if (entry.tx && entry.dateReceived) {
        self.emit('resolved', entry)
      }
    })
  })

  this.txDB = createTxDB(this._prefix('txs.db'), {
    leveldown: this.leveldown,
    log: this._log,
    timeout: false,
    autostart: false
  })

  this.txDB.name = this.name()
}

Driver.prototype._doSend = function (entry) {
  // TODO:
  //   do we log that we sent it?
  //   do we log when we delivered it? How do we know it was delivered?
  var self = this
  return Q.all([
      this.lookupIdentity(entry.to),
      this.lookupObject(entry)
    ])
    .spread(function (identityInfo, chainedObj) {
      self._debug('sending msg to peer', chainedObj.parsed.data[TYPE])
      var msg = utils.msgToBuffer(utils.getMsgProps(chainedObj))
      return self.messenger.send(identityInfo[ROOT_HASH], msg, identityInfo)
    })
}

Driver.prototype.lookupObjectByRootHash = function (rootHash) {
  return Q.ninvoke(this.messages(), 'byRootHash', rootHash)
    .then(this.lookupObject)
}

Driver.prototype.lookupObjectByCurHash = function (curHash) {
  return Q.ninvoke(this.messages(), 'byCurHash', curHash)
    .then(this.lookupObject)
}

Driver.prototype.lookupObject = function (info) {
  var self = this

  // TODO: this unfortunately duplicates part of unchainer.js
  if (!info.txData) {
    if (info.tx) {
      info = bitcoin.Transaction.fromBuffer(info.tx)
    } else {
      throw new Error('missing required info to lookup chained obj')
    }
  }

  var chainedObj
  return this.chainloader.load(info)
    .then(function (obj) {
      chainedObj = obj
      return Q.ninvoke(Parser, 'parse', obj.data)
    })
    .then(function (parsed) {
      chainedObj.parsed = parsed
      return chainedObj
    })
    .catch(function (err) {
      // repeats unchainer
      err.progress = chainedObj || info
      throw err
    })
}

Driver.prototype.lookupRootHash = function (fingerprint) {
  var pub = this.getPublicKey(fingerprint)
  if (pub) return Q.resolve(this.myRootHash())

  return Q.ninvoke(this.addressBook, 'rootHashByFingerprint', fingerprint)
}

Driver.prototype.lookupByFingerprint = function (fingerprint) {
  return this.lookupIdentity({
    fingerprint: fingerprint
  })
}

Driver.prototype.getKeyAndIdentity = function (fingerprint, returnPrivate) {
  var self = this
  return this.lookupByFingerprint(fingerprint)
    .then(function (result) {
      var identity = result.identity
      var key = returnPrivate && self.getPrivateKey(fingerprint)
      key = key || utils.keyForFingerprint(identity, fingerprint)
      var ret = {
        key: key,
        identity: identity
      }

      ret[ROOT_HASH] = result[ROOT_HASH]
      return ret
    })
}

Driver.prototype.getKeyAndIdentity2 = function (fingerprint, returnPrivate) {
  return this.getKeyAndIdentity.apply(this, arguments)
    .then(function (result) {
      result.identity = Identity.fromJSON(result.identity)
      return result
    })
}

/**
 * Will look up latest version of an object
 */
Driver.prototype.lookupByDHTKey = function (key, cb) {
  var self = this
  cb = cb || noop
  return Q.ninvoke(self.msgDB, 'byCurHash', key)
    .then(this.lookupObject)
    .nodeify(cb)
}

Driver.prototype.getPublicKey = function (fingerprint, identity) {
  identity = identity || this.identityJSON
  return find(identity.pubkeys, function (k) {
    return k.fingerprint === fingerprint
  })
}

Driver.prototype.getPrivateKey = function (where) {
  return utils.firstKey(this.identityKeys, where)
}

Driver.prototype.getBlockchainKey = function () {
  return this.getPrivateKey({
    networkName: this.networkName,
    type: 'bitcoin',
    purpose: KEY_PURPOSE
  })
}

Driver.prototype.getCachedIdentity = function (query) {
  return this._identityCache[tutils.stringify(query)]
}

Driver.prototype._cacheIdentity = function (query, value) {
  this._identityCache[tutils.stringify(query)] = value
}

Driver.prototype.lookupIdentity = function (query) {
  var self = this
  return this._lookupIdentity(query)
    .then(function (result) {
      self._cacheIdentity(query, result)
      return result
    })
}

Driver.prototype._lookupIdentity = function (query) {
  var me = this.identityJSON
  var valid = !!query.fingerprint ^ !!query[ROOT_HASH]
  if (!valid) {
    return Q.reject(new Error('query by "fingerprint" OR "' + ROOT_HASH + '" (root hash)'))
  }

  var isMe = query[ROOT_HASH] === this.myRootHash() ||
    (query.fingerprint && this.getPublicKey(query.fingerprint))

  if (isMe) {
    var ret = {
      identity: me
    }

    ret[ROOT_HASH] = this.myRootHash()
    ret[CUR_HASH] = this.myCurrentHash()
    return Q.resolve(ret)
  }

  var cached = this.getCachedIdentity(query)
  if (cached) return Q(cached)

  return Q.ninvoke(this.addressBook, 'query', query)
}

Driver.prototype.log = function (entry) {
  jsonifyErrors(entry)
  return Q.ninvoke(this._log, 'append', entry)
    .then(function () {
      // pass through for convenience
      return entry
    })
}

Driver.prototype.createReadStream = function (options) {
  return this._log.createReadStream(options)
}

Driver.prototype._prefix = function (path) {
  return this.pathPrefix + '-' + path
}

Driver.prototype.receiveMsg = function (buf, senderInfo) {
  var self = this
  var msg

  validateRecipients(senderInfo)

  try {
    msg = utils.bufferToMsg(buf)
  } catch (err) {
    return this.emit('warn', 'received message not in JSON format', buf)
  }

  this._debug('received msg', msg)

  // this thing repeats work all over the place
  var txInfo
  var promiseValid
  var valid = utils.validateMsg(msg)
  if (valid) promiseValid = Q.resolve()
  else promiseValid = Q.reject(new Error('received invalid msg'))

  var from
  return promiseValid
    .then(this.lookupIdentity.bind(this, senderInfo))
    .then(function (result) {
      from = result
      var fromKey = utils.firstKey(result.identity.pubkeys, {
        type: 'bitcoin',
        networkName: self.networkName,
        purpose: 'messaging'
      })

      txInfo = {
        addressesFrom: [fromKey.fingerprint],
        addressesTo: [self.wallet.addressString],
        txData: msg.txData,
        txType: msg.txType
      }

      // TODO: rethink how chainloader should work
      // this doesn't look right
      return self.chainloader._processTxInfo(txInfo)
    })
    .then(function (parsed) {
      var permission = Permission.recover(msg.encryptedPermission, parsed.sharedKey)
      return Q.all([
        self.keeper.put(parsed.permissionKey.toString('hex'), msg.encryptedPermission),
        self.keeper.put(permission.fileKeyString(), msg.encryptedData)
      ])
    })
    .then(function () {
      // yes, it repeats work
      // but it makes the code simpler
      // TODO optimize
      return self.lookupObject(txInfo)
    })
    .then(self.unchainResultToEntry)
    .then(function (entry) {
      return entry.set({
        type: EventType.msg.receivedValid,
        dir: Dir.inbound
      })
    })
    .catch(function (err) {
      // TODO: retry
      self._debug('failed to process inbound msg', err)
      return new Entry({
        type: EventType.msg.receivedInvalid,
        msg: msg,
        from: utils.toObj(ROOT_HASH, from && from[ROOT_HASH]),
        to: utils.toObj(ROOT_HASH, self.myRootHash()),
        dir: Dir.inbound,
        errors: [err]
      })
    })
    .then(function (entry) {
      jsonifyErrors(entry)
      return self.log(entry)
    })
}

Driver.prototype.myRootHash = function () {
  return this.identityMeta[ROOT_HASH]
}

Driver.prototype.myCurrentHash = function () {
  return this.identityMeta[CUR_HASH]
}

// TODO: enforce order
Driver.prototype.putOnChain = function (entry) {
  var self = this
  assert(entry.get(ROOT_HASH) && entry.get(CUR_HASH), 'missing required fields')

  var type = entry.get('txType')
  var data = entry.get('txData')
  var nextEntry = new Entry()
    .set(ROOT_HASH, entry.get(ROOT_HASH))
    .set(CUR_HASH, entry.get(CUR_HASH))
    .set({
      uid: entry.get('uid'),
      // from: entry.get('from'),
      // to: entry.get('to'),
      txType: type,
      chain: true,
      dir: Dir.outbound
    })

//   return this.lookupBTCAddress(to)
//     .then(shareWith)
  // console.log(entry.toJSON())
  var addr = entry.get('addressesTo')[0]
  this.emit('chaining')
  return self.chainwriter.chain()
    .type(type)
    .data(data)
    .address(addr)
    .execute()
    .then(function (tx) {
      // ugly!
      nextEntry.set({
        type: EventType.chain.writeSuccess,
        tx: utils.toBuffer(tx),
        txId: tx.getId()
      })

      // self._debug('chained (write)', nextEntry.get(CUR_HASH), 'tx: ' + nextEntry.get('txId'))
    })
    .catch(function (err) {
      err = Errors.ChainWrite({
        message: Errors.getMessage(err)
      })

      self._debug('chaining failed', err)
      self.emit('error', err)

      nextEntry.set({
        type: EventType.chain.writeError
      })

      utils.addError(nextEntry, err)
    })
    .then(function () {
      return nextEntry
    })
}

Driver.prototype.sign = function (msg) {
  typeforce('Object', msg, true) // strict
  if (!msg[SIGNEE]) {
    msg[SIGNEE] = this.myRootHash() + ':' + this.myCurrentHash()
  }

  var b = Builder()
    .data(msg)
    .signWith(this._signingKey)

  return Q.ninvoke(b, 'build')
    .then(function (result) {
      return result.form
    })
}

Driver.prototype.chain = function (options) {
  return this.send(extend({
    public: false,
    chain: true,
    deliver: false
  }, options))
}

Driver.prototype.publish = function (options) {
  return this.send(extend({
    public: true,
    chain: true
  }, options))
}

Driver.prototype.share = function (options) {
  var self = this

  typeforce({
    to: 'Array',
    chain: '?Boolean',
    deliver: '?Boolean'
  }, options)

  assert(CUR_HASH in options, 'expected current hash of object being shared')

  var to = options.to
  validateRecipients(to)

  var curHash = options[CUR_HASH]
  var entry = new Entry({
    type: EventType.msg.new, // msg.shared maybe?
    dir: Dir.outbound,
    public: false,
    chain: !!options.chain,
    deliver: !!options.deliver,
    from: utils.toObj(ROOT_HASH, this.myRootHash())
  })

  var recipients
  return Q.all(to.map(this.lookupIdentity, this))
    .then(function (_recipients) {
      recipients = _recipients
      return Q.ninvoke(self.msgDB, 'byCurHash', curHash)
    })
    .then(self.lookupObject)
    .then(function (obj) {
      entry.set(CUR_HASH, curHash)
        .set(ROOT_HASH, obj[ROOT_HASH])

      var symmetricKey = obj.permission.body().decryptionKey
      return Q.all(recipients.map(function (r) {
        var pubkey = self._getBTCKey(r.identity)
        return self.chainwriter.share()
          .shareAccessTo(curHash, symmetricKey)
          .shareAccessWith(pubkey.value)
          .execute()
      }))
    })
    .then(function (shares) {
      // TODO: rethink this repeated code from send()
      var entries = shares.map(function (share, i) {
        return entry.clone().set({
          to: utils.toObj(ROOT_HASH, recipients[i][ROOT_HASH]),
          addressesTo: [share.address],
          addressesFrom: [self.wallet.addressString],
          txType: TxData.types.permission,
          txData: utils.toBuffer(share.encryptedKey, 'hex')
        })
      })

      entries.forEach(utils.setUID)
      return Q.all(entries.map(self.log, self))
    })
}

// Driver.prototype.chain = function (info) {
//   var self = this
//   var getInfo = typeof info === 'string'
//     ? Q.ninvoke(this.msgDB, 'get', info)
//     : Q.resolve(info)

//   getInfo
//     .then(function (info) {
//       if (info.tx) throw new Error('already chained')

//       return self.log(new Entry({
//         type: EventType.msg.new,
//         deliver: false,
//         chain: true
//       }))
//     })
//     .then(function () {

//     })
// }


/**
 * send an object (and optionally chain it)
 * @param {Object} options
 * @param {Object|Buffer} options.msg - message to send (to be chainable, it should pass Parser.parse())
 * @param {Array} options.to (optional) - recipients
 * @param {Boolean} options.public (optional) - whether this message should be publicly visible
 * @param {Boolean} options.chain (optional) - whether to put this message on chain
 * @param {Boolean} options.deliver (optional) - whether to deliver this message p2p
 */
Driver.prototype.send = function (options) {
  var self = this

  typeforce({
    msg: 'Object',
    to: 'Array',
    public: '?Boolean',
    chain: '?Boolean',
    deliver: '?Boolean'
  }, options)

  if (!options.deliver && !options.chain) {
    throw new Error('expected "deliver" and/or "chain"')
  }

  if (options.chain && this.readOnly) {
    this._debug('chain write prevented')
    throw new Error('this instance is readOnly, it cannot write to the blockchain')
  }

  var data = utils.toBuffer(options.msg)
  // assert(TYPE in data, 'structured messages must specify type property: ' + TYPE)

  // either "public" or it has recipients
  var isPublic = !!options.public
  // assert(isPublic ^ !!options.to, 'private msgs must have recipients, public msgs cannot')

  var to = options.to
  if (!to && isPublic) {
    var me = utils.toObj(ROOT_HASH, this.myRootHash())
    to = [me]
  }

  validateRecipients(to)

  var entry = new Entry({
    type: EventType.msg.new,
    dir: Dir.outbound,
    public: isPublic,
    chain: !!options.chain,
    deliver: !!options.deliver,
    from: utils.toObj(ROOT_HASH, this.myRootHash())
  })

  var recipients
  var btcKeys
  return this._readyPromise
    // validate
    .then(Q.ninvoke(Parser, 'parse', data))
    .then(function (parsed) {
      return isPublic
        ? Q.resolve(to)
        : Q.all(to.map(self.lookupIdentity, self))
    })
    .then(function (_recipients) {
      recipients = _recipients
      if (isPublic) {
        btcKeys = to
      } else {
        btcKeys = utils.pluck(recipients, 'identity')
          .map(self._getBTCKey, self)
          .map(function (k) {
            return k.value
          })
      }

      return self.chainwriter.create()
        .data(data)
        .setPublic(isPublic)
        .recipients(btcKeys)
        .execute()
    })
    .then(function (resp) {
      copyDHTKeys(entry, resp.key)
      self._debug('stored (write)', entry.get(ROOT_HASH))

      var entries
      if (isPublic) {
        self._push(resp)
        entries = to.map(function (contact, i) {
          return entry.clone().set({
            to: contact,
            addressesFrom: [self.wallet.addressString],
            addressesTo: [btcKeys[i].fingerprint],
            txType: TxData.types.public,
            txData: utils.toBuffer(resp.key, 'hex')
          })
        })
      } else {
        entries = resp.shares.map(function (share, i) {
          return entry.clone().set({
            to: utils.toObj(ROOT_HASH, recipients[i][ROOT_HASH]),
            addressesTo: [share.address],
            addressesFrom: [self.wallet.addressString],
            txType: TxData.types.permission,
            txData: utils.toBuffer(share.encryptedKey, 'hex')
          })
        })
      }

      entries.forEach(utils.setUID)
      return Q.all(entries.map(self.log, self))
    })
}

Driver.prototype._push = function () {
  if (this.keeper.push) {
    this.keeper.push.apply(this.keeper, arguments)
  }
}

Driver.prototype._getBTCKey = function (identity) {
  return utils.firstKey(identity.pubkeys, {
    type: 'bitcoin',
    networkName: this.networkName,
    purpose: KEY_PURPOSE
  })
}

Driver.prototype.lookupBTCKey = function (recipient) {
  var self = this
  return this.lookupIdentity(recipient)
    .then(function (result) {
      return utils.firstKey(result.identity.pubkeys, {
        type: 'bitcoin',
        networkName: self.networkName,
        purpose: KEY_PURPOSE
      })
    })
}

Driver.prototype.lookupBTCPubKey = function (recipient) {
  return this.lookupBTCKey(recipient).then(function (k) {
    return k.value
  })
}

Driver.prototype.lookupBTCAddress = function (recipient) {
  return this.lookupBTCKey(recipient)
    .then(function (k) {
      return k.fingerprint
    })
}

Driver.prototype.destroy = function () {
  var self = this

  this._debug('self-destructing')
  this.emit('destroy')

  delete this._fingerprintToIdentity
  this._destroyed = true

  // sync
  this.chainwriter.destroy()
  if (this._rawTxStream) {
    this._rawTxStream.close() // custom close method
  }

  delete this._identityCache
  // async
  return Q.all([
      self.keeper.destroy(),
      self.messenger.destroy(),
      Q.ninvoke(self.addressBook, 'close'),
      Q.ninvoke(self.msgDB, 'close'),
      Q.ninvoke(self.txDB, 'close'),
      Q.ninvoke(self._log, 'close')
    ])
    // .then(function () {
    //   self.removeAllListeners()
    // })
// .done(console.log.bind(console, this.pathPrefix + ' is dead'))
}

Driver.prototype._debug = function () {
  var args = [].slice.call(arguments)
  args.unshift(this.name())
  return debug.apply(null, args)
}

Driver.prototype._getTxDir = function (tx) {
  var self = this
  var isOutbound = tx.ins.some(function (input) {
    var addr = tutils.getAddressFromInput(input, self.networkName)
    return addr === self.wallet.addressString
  })

  return isOutbound ? Dir.outbound : Dir.inbound
}

function copyDHTKeys (dest, src, curHash) {
  if (typeof curHash === 'undefined') {
    if (typeof src === 'string') {
      curHash = src
    } else {
      curHash = utils.getEntryProp(src, CUR_HASH) || utils.getEntryProp(src, ROOT_HASH)
    }

    src = dest
  }

  var rh = utils.getEntryProp(src, ROOT_HASH)
  var ph = utils.getEntryProp(src, PREV_HASH)
  utils.setEntryProp(dest, ROOT_HASH, rh || curHash)
  utils.setEntryProp(dest, PREV_HASH, ph)
  utils.setEntryProp(dest, CUR_HASH, curHash)
}

function validateRecipients (recipients) {
  if (!Array.isArray(recipients)) recipients = [recipients]

  recipients.every(function (r) {
    assert(r.fingerprint || r.pubKey || r[ROOT_HASH],
      'invalid recipient, must be an object with a "fingerprint", "pubKey" or ' + ROOT_HASH + ' property')
  })
}

function throttleIfRetrying (errors, throttle, cb) {
  if (!errors || !errors.length) {
    return cb()
  }

  var lastErr = errors[errors.length - 1]
  if (!lastErr.timestamp) {
    debug('bad error: ', lastErr)
    throw new Error('error is missing timestamp')
  }

  var now = utils.now()
  var wait = lastErr.timestamp + throttle - now
  if (wait < 0) {
    return cb()
  }

  // just in case the device clock time-traveled
  wait = Math.min(wait, throttle)
  setTimeout(cb, wait)
}

// function prettyPrint (json) {
//   console.log(JSON.stringify(json, null, 2))
// }

var toObjectStream = map.bind(null, function (data, cb) {
  if (typeof data.value !== 'object') {
    return cb()
  }

  cb(null, data.value)
})

var jsonifyTransform = map.bind(null, function (entry, cb) {
  cb(null, jsonifyErrors(entry))
})

var jsonifyErrors = function (entry) {
  var errs = utils.getEntryProp(entry, 'errors')
  if (errs) {
    errs.forEach(function (err) {
      if (!err.timestamp) err.timestamp = utils.now()
    })

    utils.setEntryProp(entry, 'errors', errs.map(utils.errToJSON))
  }

  return entry
}
