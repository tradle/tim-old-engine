var EventEmitter = require('events').EventEmitter
var assert = require('assert')
var util = require('util')
var omit = require('object.omit')
var Q = require('q')
var typeforce = require('typeforce')
var debug = require('debug')('tim')
var PassThrough = require('readable-stream').PassThrough
var reemit = require('re-emitter')
var bitcoin = require('@tradle/bitcoinjs-lib')
var extend = require('xtend/mutable')
var clone = require('xtend/immutable')
var collect = require('stream-collector')
var tradleUtils = require('@tradle/utils')
var map = require('map-stream')
var pump = require('pump')
var find = require('array-find')
var deepEqual = require('deep-equal')
var Cache = require('lru-cache')
var ChainedObj = require('@tradle/chained-obj')
var TxData = require('@tradle/tx-data').TxData
var TxInfo = require('@tradle/tx-data').TxInfo
var ChainWriter = require('@tradle/bitjoe-js')
var ChainLoader = require('@tradle/chainloader')
var Permission = require('@tradle/permission')
var Wallet = require('@tradle/simple-wallet')
var hrtime = require('monotonic-timestamp')
var mi = require('@tradle/identity')
var Identity = mi.Identity
var kiki = require('@tradle/kiki')
var toKey = kiki.toKey
var Builder = ChainedObj.Builder
var Parser = ChainedObj.Parser
var lb = require('logbase')
var Entry = lb.Entry
var unchainer = require('./lib/unchainer')
var constants = require('@tradle/constants')
var EventType = require('./lib/eventType')
var Dir = require('./lib/dir')
var createIdentityDB = require('./lib/identityDB')
var createMsgDB = require('./lib/msgDB')
var createTypeDB = require('./lib/typeDB')
var createTxDB = require('./lib/txDB')
var createMiscDB = require('./lib/miscDB')
var Errors = require('./lib/errors')
var utils = require('./lib/utils')
var getViaCache = require('./lib/getViaCache')
var RETRY_UNCHAIN_ERRORS = [
  ChainLoader.Errors.ParticipantsNotFound,
  ChainLoader.Errors.FileNotFound
].map(function (ErrType) {
  return ErrType.type
})

var DEV = process.env.NODE_ENV !== 'production'
var TYPE = constants.TYPE
var TYPES = constants.TYPES
var SIGNEE = constants.SIGNEE
var ROOT_HASH = constants.ROOT_HASH
var PREV_HASH = constants.PREV_HASH
var CUR_HASH = constants.CUR_HASH
var NONCE = constants.NONCE
var CONFIRMATIONS_BEFORE_CONFIRMED = 10
Driver.BLOCKCHAIN_KEY_PURPOSE = 'messaging'
Driver.MIN_BALANCE = 10000
Driver.CHAIN_WRITE_THROTTLE = 60000
Driver.CHAIN_READ_THROTTLE = 300000
Driver.SEND_THROTTLE = 10000
Driver.CATCH_UP_INTERVAL = 2000
Driver.UNCHAIN_THROTTLE = 20000
// Driver.Zlorp = Zlorp
Driver.Kiki = kiki
Driver.Identity = Identity
Driver.Wallet = Wallet
Driver.EventType = EventType
Driver.PROTOCOL_VERSION = '1.0.0'
// TODO: export other deps

var DEFAULT_OPTIONS = {
  opReturnPrefix: '',
  chainThrottle: Driver.CHAIN_WRITE_THROTTLE,
  syncInterval: Driver.CHAIN_READ_THROTTLE,
  unchainThrottle: Driver.UNCHAIN_THROTTLE,
  sendThrottle: Driver.SEND_THROTTLE,
  afterBlockTimestamp: 0
}

var optionsTypes = {
  // maybe allow read-only mode if this is missing
  // TODO: replace with kiki (will need to adjust otr, zlorp for async APIs)
  keys: 'Array',
  identity: 'Identity',
  blockchain: 'Object',
  networkName: 'String',
  keeper: 'Object',
  leveldown: 'Function',
  pathPrefix: 'String',
  opReturnPrefix: '?String',
  syncInterval: '?Number',
  chainThrottle: '?Number',
  unchainThrottle: '?Number',
  sendThrottle: '?Number',
  readOnly: '?Boolean',
  afterBlockTimestamp: '?Number',
  name: '?String'
}

var noop = function () {}

module.exports = Driver
util.inherits(Driver, EventEmitter)
Driver.enableOptimizations = require('./lib/optimizations')

function Driver (options) {
  var self = this

  typeforce(optionsTypes, options)

  EventEmitter.call(this)
  this.setMaxListeners(0)

  tradleUtils.bindPrototypeFunctions(this)
  // this._fetchAndReschedule = promiseDebounce(this._fetchAndReschedule, this)

  options = clone(DEFAULT_OPTIONS, options)
  extend(this, omit(options, ['name']))

  this._options = utils.pick(this, Object.keys(optionsTypes))
  this._name = options.name

  this._signingKey = toKey(
    this.getPrivateKey({
      type: 'ec',
      purpose: 'sign'
    })
  )

  // copy
  this.identityMeta = {}

  this.setIdentity(options.identity.toJSON())
  if (this.afterBlockTimestamp) {
    this._debug('ignoring txs before', new Date(this.afterBlockTimestamp * 1000).toString())
  }

  var networkName = this.networkName
  var keeper = this.keeper
  var blockchain = this.blockchain
  var leveldown = this.leveldown
  var wallet = this.wallet = this.wallet || new Wallet({
    networkName: networkName,
    blockchain: blockchain,
    priv: this.getBlockchainKey().priv()
  })

  // init balance while we rely on blockr for this info
  this._balance = 0

  this._paused = false

  this.chainwriter = new ChainWriter({
    wallet: wallet,
    keeper: keeper,
    networkName: networkName,
    minConf: 0,
    prefix: this.opReturnPrefix
  })

  this.chainloader = new ChainLoader({
    keeper: keeper,
    networkName: networkName,
    prefix: this.opReturnPrefix,
    lookup: this.getKeyAndIdentity2
  })

  this.unchainer = unchainer({
    chainloader: this.chainloader,
    lookup: this.lookupByDHTKey
  })

  this._streams = {}
  this._ignoreTxs = []
  this._watchedAddresses = [
    this.wallet.addressString
  ]

  this._watchedTxs = []

  // in-memory cache of recent conversants
  this._catchUpWithBlockchain()
  this._fingerprintToIdentity = {}
  this._pendingTxs = []

  this._setupDBs()
  this._setupTxFetching()

  var keeperReadyDfd = Q.defer()
  var keeperReady = keeperReadyDfd.promise
  if (!keeper.isReady || keeper.isReady()) keeperReadyDfd.resolve()
  else keeper.once('ready', keeperReadyDfd.resolve)

  this._readyPromise = Q.all([
      self._prepIdentity(),
      self._setupTxStream(),
      keeperReady,
      this._loadWatches(),
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
      self.typeDB.start()
      self.miscDB.start()

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

/**
 * specify txIds for which to load txs
 * @param  {Array} txIds - transaction ids
 */
Driver.prototype.loadTxs = function (txIds) {
  var self = this
  Q.ninvoke(this.blockchain.transactions, 'get', txIds)
    .then(function (txInfos) {
      // TODO: get rid of this parse everywhere
      txInfos = utils.parseCommonBlockchainTxs(txInfos)
      return self._processTxs(txInfos)
    })
}

Driver.prototype.ready = function () {
  return this._readyPromise
}

Driver.prototype.isReady = function () {
  return this._ready
}

Driver.prototype.isPaused = function () {
  return this._paused
}

Driver.prototype.pause = function (resumeTimeout) {
  if (this._paused) return

  this._debug('pausing...')
  this._paused = true
  for (var name in this._streams) {
    this._streams[name].pause()
  }

  if (typeof resumeTimeout === 'number') {
    setTimeout(this.resume, resumeTimeout)
  }

  this.emit('pause')
}

Driver.prototype.resume = function () {
  if (!this._paused) return

  this._debug('resuming...')
  this._paused = false
  for (var name in this._streams) {
    this._streams[name].resume()
  }

  this.emit('resume')
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
      if (balance < Driver.MIN_BALANCE) {
        self.emit('lowbalance')
      }
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

  var stream = this.txDB.liveStream({
    old: true,
    tail: true
  })

  this._streams.txDB = stream
  pump(
    stream,
    map(function (data, cb) {
      if (self._destroyed || typeof data.value !== 'object') {
        return cb()
      }

      cb(null, data.value)
    }),
    map(function (entry, cb) {
      // was read from chain and hasn't been processed yet
      var txId = entry.txId
      if (!entry.dateDetected || entry.dateUnchained || entry.ignore) {
        return finish()
      }

      // no body yet
      if (!entry.tx) {
        return finish()
      }

      // clear old errors
      var errs = utils.getErrors(entry, 'unchain')
      delete entry.errors

      if (!errs || !errs.length) return finish(null, entry)

      var shouldTryAgain = errs.every(function (err) {
        return RETRY_UNCHAIN_ERRORS.indexOf(err.type) !== -1
      })

      if (!shouldTryAgain) return finish()

      if (errs.length >= Errors.MAX_UNCHAIN) {
        // console.log(entry.errors, entry.id)
        self._debug('skipping unchain after', errs.length, 'errors for tx:', txId)
        // TODO: remove
        // self._remove(entry)
        return finish()
      }

      if (self._queuedUnchains[txId]) {
        finish()
        return self._debug('already schedulded unchaining!')
      }

      self._queuedUnchains[txId] = true
      var throttled = throttleIfRetrying(errs, self.unchainThrottle, function () {
        finish(null, entry)
      })

      if (throttled) {
        self._debug('throttling unchain retry of tx', txId, throttled, 'millis')
      }

      function finish (err, ret) {
        if (err || !ret) self._rmPending(txId)

        delete self._queuedUnchains[txId]
        if (!ret) {
          self._debug('skipping unchain of tx', txId)
        }

        cb(err, ret)
      }
    }),
    map(function (txEntry, cb) {
      if (self._destroyed) return cb()

      var chainedObj
      self.lookupObject(txEntry, true) // verify=true
        .then(function (result) {
          chainedObj = result
          if (chainedObj.txType === TxData.types.public) {
            self._debug('saving to keeper')
            self.keeper.put(chainedObj.key, chainedObj.data)
          }

          return chainedObj
        })
        .catch(function (err) {
          chainedObj = err.progress
        })
        // record failed unchains as well
        .then(function () {
          return self.unchainResultToEntry(chainedObj)
        })
        .done(function (entry) {
          self._rmPending(utils.getEntryProp(entry, 'txId'))
          cb(null, entry)
        })
    }),
    utils.jsonify(),
    this._log,
    this._rethrow
  )

  this._pauseStreamIfPaused(stream)
}

Driver.prototype._pauseStreamIfPaused = function (stream) {
  if (this._paused) {
    stream.pause()
  }
}

// Driver.prototype._remove = function (info, lookup) {
//   var self = this
//   this.lookupObject(info)
//     .catch(function (err) {
//       return err.progress
//     })
//     .then(function (chainedObj) {
//       var tasks = ['key', 'permissionKey']
//         .map(function (p) {
//           return chainedObj[p]
//         })
//         .filter(function (key) {
//           return !!key
//         })

//       return Q.all(tasks)
//     })
// }

Driver.prototype._rmPending = function (txId) {
  typeforce('String', txId)

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

Driver.prototype._setupTxFetching = function () {
  var self = this
  var txDB = this.txDB
  var txSubmodule = this.blockchain.transactions
  txSubmodule.get = getViaCache(
    txSubmodule.get.bind(txSubmodule),
    getFromCache
  )

  function getFromCache (ids, cb) {
    // ids = ids.filter(function (id) {
    //   return self._ignoreTxs.indexOf(id) === -1
    // })

    // var togo = ids.length
    // if (togo === 0) return cb(null, [])

    self._multiGetFromDB({
      db: self.txDB,
      keys: ids
    })
    .then(function (results) {
      results = utils.pluck(results, 'value')
      results.forEach(function (r, i) {
        if (r && r.ignore) {
          var id = ids[i]
          if (self._ignoreTxs.indexOf(id) === -1) {
            self._ignoreTxs.push(id)
          }
        }
      })

      results = results.map(function (v) {
        return v && v.tx && v.confirmations >= CONFIRMATIONS_BEFORE_CONFIRMED ? v : undefined
      })

      cb(null, results)
    })
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

  this.once('txs', function (txInfos) {
    txIds = txInfos
      .filter(function (txInfo) {
        return txInfo.blockTimestamp > self.afterBlockTimestamp
      })
      .map(function (txInfo) {
        return txInfo.txId
      })

    checkDBs()
  })

  return this._caughtUpPromise

  function checkDBs () {
    if (self._destroyed) return

    self._multiGetFromDB({
        db: self.txDB,
        keys: txIds,
        strict: true,
        then: function (entry) {
          var hasErrors = !!utils.countErrors(entry)
          var processed = entry.dateUnchained || entry.dateChained || entry.ignore || hasErrors
          if (!processed) {
            self._debug('still waiting for tx', entry.txId)
            throw new Error('not ready')
          }
        }
      })
      .then(catchUp.resolve)
      .catch(scheduleRetry)
  }

  function scheduleRetry () {
    self._debug('not caught up yet with blockchain...')
    setTimeout(checkDBs, Driver.CATCH_UP_INTERVAL)
  }
}

// Driver.prototype.unignoreTxs = function (txIds) {
//   this.log(new Entry({
//     type: EventType.tx.load,
//     ignore: false,
//     txIds: txIds
//   }))
// }

Driver.prototype._sortParsedTxs = function (txInfos) {
  return txInfos
}

Driver.prototype._shouldLoadTx = function (txInfo) {
  // override this if you want to be more choosy
  return true
}

Driver.prototype._willLoadTx = function (txInfo) {
  var txId = txInfo.txId
  if (this._ignoreTxs.indexOf(txId) !== -1) return false
  if (this._watchedTxs.indexOf(txId) !== -1) return true

  return this._shouldLoadTx(txInfo)
}

// Driver.prototype.shouldLoadTxData = function (txData) {
//   // override this if you want to be more choosy
//   return true
// }

Driver.prototype._multiGetFromDB = function (opts) {
  // override this if you have a better way
  // e.g. in react-native, with AsyncStorage.multiGet
  typeforce({
    db: 'Object',
    keys: 'Array',
    then: '?Function',
    strict: '?Boolean'
  }, opts)

  return Q[opts.strict ? 'all' : 'allSettled'](opts.keys.map(function (key) {
    var promise = Q.ninvoke(opts.db, 'get', key)
    return opts.then ? promise.then(opts.then) : promise
  }))
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
        Q.ninvoke(tradleUtils, 'getStorageKeyFor', utils.toBuffer(me))
      ])
    })
    .spread(function (entries, curHash) {
      curHash = curHash.toString('hex')

      var unchained = entries.filter(function (e) {
        return e.dateUnchained
      })

      status.ever = !!unchained.length
      unchained.some(function (e) {
        if (e[CUR_HASH] === curHash) {
          status.txId = e.txId
          status.current = true
          return true
        }
      })

      status.queued = !status.current && entries.some(function (e) {
        return e[CUR_HASH] === curHash
      })

      return status
    })
    .catch(function (err) {
      if (!err.notFound) throw err

      return status
    })
}

Driver.prototype.publishIdentity = function (identity, toAddress) {
  identity = identity || this.identityJSON
  return this.publish({
    msg: identity,
    to: [{ fingerprint: toAddress || constants.IDENTITY_PUBLISH_ADDRESS }]
  })
}

Driver.prototype.setIdentity = function (identityJSON) {
  if (deepEqual(this.identityJSON, identityJSON)) return

  this.identity = Identity.fromJSON(identityJSON)
  this.identityJSON = this.identity.toJSON()
}

Driver.prototype.publishMyIdentity = function (toAddr) {
  var self = this

  if (this._publishingIdentity) {
    throw new Error('wait till current publishing process ends')
  }

  if (!this._ready) {
    return this._readyPromise.then(this.publishMyIdentity)
  }

  if (toAddr) {
    this.watchAddresses(toAddr)
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
    return Builder.addNonce(update)
      .then(function () {
        return Builder()
          .data(update)
          .signWith(toKey(priv))
          .build()
      })
      .then(function (buf) {
        self.setIdentity(update)
        extend(self.identityMeta, utils.pick(update, PREV_HASH, ROOT_HASH))
        return Q.all([
          self._prepIdentity(),
          self.publishIdentity(buf, toAddr)
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
      self.lookupObject(info)
        .then(function (obj) {
          cb(null, obj)
        }, function (err) {
          self._debug('failed to lookup object: ' + info[CUR_HASH])
          cb()
        })
    }))
}

Driver.prototype.transactions = function () {
  return this.txDB
}

Driver.prototype.unchainResultToEntry = function (chainedObj) {
  var self = this
  var success = !utils.countErrors(chainedObj)
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

  // if ('key' in chainedObj) {
  //   entry.set(CUR_HASH, chainedObj.key)
  // }

  if ('parsed' in chainedObj) {
    entry.set('public', chainedObj.txType === TxData.types.public)
  }

  if ('tx' in chainedObj) {
    entry.set('tx', utils.toBuffer(chainedObj.tx))
  }

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

Driver.prototype._writeToChain = function () {
  var self = this
  var getStream = this.msgDB.getToChainStream.bind(this.msgDB, {
    old: true,
    tail: true
  })

  var throttle = this.chainThrottle
  this._processQueue({
    name: 'writeToChain',
    getStream: getStream,
    maxErrors: Errors.MAX_CHAIN,
    errorsGroup: Errors.group.chain,
    processItem: this.putOnChain,
    throttle: throttle,
    retryDelay: throttle,
    successType: EventType.chain.writeSuccess,
    // shouldSkipQueue: function (state) {
    //   return state.public
    // },
    shouldTryLater: function (state) {
      if (self._balance < Driver.MIN_BALANCE) {
        self._updateBalance()
        return true
      }
    }
  })
}

Driver.prototype.getSendQueue = function () {
  var msgDB = this.msgDB
  var stream = pump(
    msgDB.getToSendStream({
      old: true,
      tail: false
    }),
    map(function (data, cb) {
      msgDB.get(data.value, cb)
    })
    // ,
    // utils.filterStream(function (data) {
    //   var errs = utils.getErrors(data, 'send')
    //   return !(errs && errs.length >= Errors.MAX_SEND)
    // })
  )

  return Q.nfcall(collect, stream)
}

Driver.prototype.getChainQueue = function () {
  var msgDB = this.msgDB
  var stream = pump(
    msgDB.getToChainStream({
      old: true,
      tail: false
    }),
    map(function (data, cb) {
      msgDB.get(data.value, cb)
    })
    // ,
    // utils.filterStream(function (data) {
    //   var errs = utils.getErrors(data, 'chain')
    //   return !(errs && errs.length >= Errors.MAX_CHAIN)
    // })
  )

  return Q.nfcall(collect, stream)
}

Driver.prototype._sendTheUnsent = function () {
  var getStream = this.msgDB.getToSendStream.bind(this.msgDB, {
    old: true,
    tail: true
  })

  this._processQueue({
    name: 'sendTheUnsent',
    getStream: getStream,
    maxErrors: Errors.MAX_RESEND,
    errorsGroup: Errors.group.send,
    processItem: this._processSendQueueItem,
    retryDelay: this.sendThrottle,
    successType: EventType.msg.sendSuccess,
    giveUpType: EventType.msg.giveUpSend
  })
}

Driver.prototype._processQueue = function (opts) {
  var self = this

  typeforce({
    name: 'String',
    successType: 'Number',
    getStream: 'Function',
    processItem: 'Function',
    maxErrors: '?Number',
    shouldSkipQueue: '?Function',
    shouldTryLater: '?Function',
    retryDelay: '?Number',
    retryOnFail: '?Boolean' // default: true
  }, opts)

  var name = opts.name
  this._processingQueue = this._processingQueue || {}
  if (this._processingQueue[name]) return

  this._processingQueue[name] = true

  // queues by recipient root hash
  var queues = {}
  var lastProcessTime = 0
  var shouldSkipQueue = opts.shouldSkipQueue || alwaysFalse
  var shouldTryLater = opts.shouldTryLater || alwaysFalse
  var maxErrors = typeof opts.maxErrors === 'number' ? opts.maxErrors : Infinity
  var errorsGroup = opts.errorsGroup
  var processItem = opts.processItem
  if (opts.throttle) {
    processItem = utils.rateLimitPromiseFn(processItem, opts.throttle)
  }

  var retryDelay = opts.retryDelay
  var retryOnFail = opts.retryOnFail !== false
  var successType = opts.successType
  var giveUpType = opts.giveUpType
  var stream = opts.getStream()
  var sync
  stream.once('sync', function () {
    sync = true
  })

  stream.once('end', function () {
    debugger
  })

  this._streams[name] = stream
  pump(
    stream,
    map(function (data, cb) {
      if (data.type === 'del') {
        remove(data)
        return cb()
      }

      self.msgDB.get(data.value, function (err, state) {
        if (err) {
          self._debug('error on "get" from msgDB', err)
          remove(data)
          return throwIfDev(err)
        }

        // HACK for bug fixed in 3.5.1
        // to kill orphaned timestamps that were already introduced
        if (state.timestamp !== Number(data.key.split('!')[1])) {
          self._debug('ignoring orphaned timestamp', state.uuid)
          return cb()
        }

        if (shouldSkipQueue(state)) {
          runASAP(state)
        } else {
          insert(state)
          processQueue(getQueueID(state))
        }

        cb()
      })
    })
  )

  this._pauseStreamIfPaused(stream)

  function getQueueID (data) {
    // should public go in one queue?
    // return data.public ? data[ROOT_HASH] : data.to[ROOT_HASH]
    return data.to[ROOT_HASH] || data.to.fingerprint
  }

  function runASAP (state) {
    if (self._destroyed) return
    if (shouldTryLater(state)) {
      setTimeout(runASAP.bind(null, state), retryDelay)
    } else {
      processItem(state)
    }
  }

  function processQueue (rid) {
    if (self._destroyed) return

    var q = queues[rid] = queues[rid] || []
    if (!q.length || q.processing || q.waiting) return

    self._debug('processing queue', name)
    q.processing = true
    var rawNext = q[0]
    if (shouldTryLater(rawNext)) {
      return utils.promiseDelay(retryDelay)
        .done(processQueue.bind(null, rid))
    }

    rawNext.errors = rawNext.errors || {}
    var errors = rawNext.errors[errorsGroup] = rawNext.errors[errorsGroup] || []
    var next = omit(rawNext, ['errors']) // defensive copy
    return processItem(next)
      .catch(function (err) {
        debug(name + ' processItem crashed (this should not happen)', err)
        throw err
      })
      .then(function (entry) {
        if (self._destroyed) return

        self._debug('processed item from queue', name)
        q.processing = false
        var eType = utils.getEntryProp(entry, 'type')
        var isFinished = eType === successType
          || eType === giveUpType
          || !retryOnFail

        if (!isFinished) {
          errors.push.apply(errors, utils.getErrors(entry, errorsGroup))
          isFinished = errors.length >= maxErrors
        }

        if (isFinished) {
          q.shift()
          processQueue(rid)
        } else if (retryOnFail) {
          self._debug('throttling queue', name)
          q.waiting = true
          setTimeout(keepGoing, retryDelay)
        }
      })

    function keepGoing () {
      if (self._destroyed) return
      q.waiting = false
      // probably not needed
      self.msgDB.onLive(function () {
        processQueue(rid)
      })
    }
  }

  function insert (data) {
    var rid = getQueueID(data)
    var q = queues[rid] = queues[rid] || []
    if (!sync) return q.push(data)

    var exists = q.some(function (item) {
      return item.uid === data.uid
    })

    if (!exists) q.push(data)
  }

  function remove (data) {
    if (!data.value) {
      debugger
      return
    }

    var idx = -1
    var uid = data.value
    var rid = utils.parseUID(uid).to
    var q = queues[rid]
    if (!q || !q.length) return

    q.some(function (item, i) {
      if (item.uid === uid) {
        idx = i
      }
    })

    if (idx !== -1) q.splice(idx, 1)
  }
}

Driver.prototype._processSendQueueItem = function (entry) {
  // this._markSending(entry)
  var self = this
  var nextEntry = new Entry()
    .set(utils.pick(entry, 'uid', ROOT_HASH))

  // return Q.ninvoke(this.msgDB, 'onLive')
  //   .then(function () {
      return self._trySend(entry)
    // })
    .then(function () {
      self._debug('msg sent successfully')
      return nextEntry.set('type', EventType.msg.sendSuccess)
    })
    .catch(function (err) {
      self._debug('msg send failed', err.message)
      var eType = err.type === 'fileNotFound'
        ? EventType.msg.giveUpSend
        : EventType.msg.sendError

      nextEntry.set({
        type: eType
      })

      utils.addError({
        entry: nextEntry,
        group: Errors.group.send,
        error: err
      })

      // self._markNotSending(nextEntry)
      return nextEntry
    })
    .then(this.log)
}

Driver.prototype.name = function () {
  if (this._name) return this._name

  return this._name = this.identityJSON.pubkeys[0].fingerprint
}

Driver.prototype._markSending = function (entry) {
  this._currentlySending.push(entry.uid)
}

Driver.prototype._markNotSending = function (entry) {
  var idx = this._currentlySending.indexOf(entry.uid)
  this._currentlySending.splice(idx, 1)
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
  //     self._runFetchTxsLoop(lastBlock, lastBlockTxIds)
  //     defer.resolve()
  //   })
  //
  // return defer.promise

  this._runTxProcessingLoop(0, [])
  return Q.resolve()
}

Driver.prototype._runTxProcessingLoop = function (fromHeight, skipIds) {
  var self = this
  if (this._destroyed) return

  // TODO: fetch based on height
  if (!fromHeight) fromHeight = 0

  this._fetchAndReschedule()
}

Driver.prototype.sync = function (force) {
  if (force) {
    delete this._pendingFetch
  }

  this._fetchAndReschedule()
}

Driver.prototype._fetchAndReschedule = function () {
  var self = this
  var waitTillUnpaused
  if (this._destroyed) return

  if (this._pendingFetch) return this._pendingFetch

  clearTimeout(this._fetchTxsTimeout)
  if (this._paused) {
    waitTillUnpaused = Q.Promise(function (resolve) {
      if (!self._paused) resolve()
      else self.once('resume', resolve)
    })
  }

  return this._pendingFetch = Q(waitTillUnpaused)
    .then(this._fetchTxs)
    .then(this._processTxs)
    .catch(function (err) {
      debug('failed to fetch/process txs', err)
    })
    .then(function () {
      delete self._pendingFetch
      if (!self._destroyed) {
        waitAndLoop()
      }
    })

  function waitAndLoop() {
    var defer = Q.defer()
    self._fetchTxsTimeout = setTimeout(defer.resolve, self.syncInterval)
    defer.promise.then(self._fetchAndReschedule)
  }
}

Driver.prototype._fetchTxs = function () {
  var self = this
  return this._loadWatchesPromise
    .then(function () {
      var watchedTxs = self._getWatchedTxs()
      return Q.all([
        Q.ninvoke(self.blockchain.addresses, 'transactions', self._getWatchedAddresses()),
        watchedTxs.length && Q.ninvoke(self.blockchain.transactions, 'get', watchedTxs)
      ])
    })
    .spread(function (part1, part2) {
      var txInfos = part2 ? part1.concat(part2) : part1
      // TODO: get rid of this parse everywhere
      txInfos = utils.parseCommonBlockchainTxs(txInfos)
      self.emit('txs', txInfos)
      return txInfos
    })
}

Driver.prototype._processTxs = function (txInfos) {
  var self = this
  if (this._destroyed) return

  var idsToLookup = []
  txInfos = this._sortParsedTxs(txInfos)

  var filtered = txInfos.filter(function (txInfo) {
      var id = txInfo.txId
      if (self._watchedTxs.indexOf(id) === -1 &&
          !txInfo.watch &&
          txInfo.blockTimestamp &&
          txInfo.blockTimestamp <= self.afterBlockTimestamp) {

        // TODO: remove this when we have a blockchain API
        // that supports height-based queries
        self.log(new Entry({
          type: EventType.tx.ignore,
          txId: id
        }))

        return
      }

      if (self._pendingTxs.indexOf(id) !== -1) {
        return
      }

      // TODO: filter shouldn't have side effects
      self._pendingTxs.push(id)
      idsToLookup.push(id)
      return true
    })

  if (!idsToLookup.length) return

  return this._multiGetFromDB({
      db: this.txDB,
      keys: idsToLookup
    })
    .then(function (results) {
      var logPromises = results.map(function (result, i) {
        var txInfo = filtered[i]
        var id = txInfo.txId
        var entry = result.value
        var err = result.reason
        var unexpectedErr = err && !err.notFound
        if (unexpectedErr) {
          self._debug('unexpected txDB error', err)
        }

        var handled
        if (entry && 'confirmations' in entry) {
          if (entry.confirmations > CONFIRMATIONS_BEFORE_CONFIRMED
              || entry.confirmations === txInfo.confirmations) {
            handled = true
          }
        }

        if (unexpectedErr || handled) {
          return self._rmPending(id)
        }

        var type

        // console.log(TxInfo.parse(txInfo.tx))
        var parsedTx = TxInfo.parse(txInfo.tx, self.networkName, self.opReturnPrefix)
        if (entry) {
          if (entry.tx && (entry.dateDetected || entry.ignore)) {
            // already got this from chain
            // at least once
            return
          }

          // we put this tx on chain
          // this is the first time we're getting it FROM the chain
          type = EventType.tx.confirmation

          if (!entry.dateChained) {
            self._debug('uh oh, this should be a confirmation for a tx chained by us')
          }
        } else if (!parsedTx.txData) {
          type = EventType.tx.ignore
        } else {
          type = EventType.tx.new
        }

        if (type !== EventType.tx.ignore) {
          if (!(entry && entry.ignore === false) && !self._willLoadTx(parsedTx)) {
            type = EventType.tx.ignore
          }
        }

        var isOutbound = parsedTx.addressesFrom.some(function (addr) {
          return addr === self.wallet.addressString
        })

        var nextEntry = new Entry(extend(
          entry || {},
          txInfo,
          parsedTx,
          {
            type: type,
            txId: id,
            tx: utils.toBuffer(txInfo.tx),
            // ignore can be revoked
            ignore: type === EventType.tx.ignore,
            dir: isOutbound ? Dir.outbound : Dir.inbound
          }
        ))

        // clear errors
        nextEntry.set('errors', {})
        return self.log(nextEntry)
      })

    return Q.all(logPromises)
  })
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
    'sent',
    'forgot'
  ]

  reemit(this.msgDB, this, msgDBEvents)
  this._enableObjectCaching()

  ;['message', 'unchained'].forEach(function (event) {
    self.msgDB.on(event, function (entry) {
      if (entry.tx && entry.dateReceived) {
        self.emit('resolved', entry)
      }
    })
  })

  this.on('unchained', function (entry) {
    if (entry[TYPE] === TYPES.IDENTITY && entry[CUR_HASH] === self.myCurrentHash()) {
      self.emit('unchained-self', entry)
    }
  })

  this.txDB = createTxDB(this._prefix('txs.db'), {
    leveldown: this.leveldown,
    log: this._log,
    timeout: false,
    autostart: false
  })

  this.txDB.name = this.name()

  this.miscDB = createMiscDB(this._prefix('misc.db'), {
    leveldown: this.leveldown,
    log: this._log,
    timeout: false,
    autostart: false
  })

  this.miscDB.on('watchedAddresses', function (addrs) {
    self._setWatchedAddresses(addrs)
  })

  this.miscDB.on('watchedTxs', function (txIds) {
    self._setWatchedTxs(txIds)
  })

  this.miscDB.name = this.name()

  this.typeDB = createTypeDB(this._prefix('type.db'), {
    leveldown: this.leveldown,
    log: this._log,
    timeout: false,
    autostart: false,
    keeper: this.keeper
  })

  this.typeDB.name = this.name()
}

Driver.prototype._trySend = function (entry) {
  // TODO:
  //   do we log that we sent it?
  //   do we log when we delivered it? How do we know it was delivered?
  var self = this
  return this.lookupObject(entry)
    .then(function (obj) {
      var to = clone(obj.to)
      to.identity = to.identity.toJSON()
      self._debug('sending msg to peer', obj.parsed.data[TYPE])
      var msg = utils.msgToBuffer(utils.getMsgProps(obj))
      var toRootHash = obj.to[ROOT_HASH]
      return self._send(toRootHash, msg, to)
    })
}

/**
 * Override this
 * @return {Promise} send a message to a particular recipient
 */
Driver.prototype._send = function (recipientInfo, msg) {
  throw new Error('override this method')
}

Driver.prototype.lookupObjectsByRootHash = function (rootHash) {
  var self = this
  return Q.ninvoke(this.messages(), 'byRootHash', rootHash)
    .then(function (msgs) {
      return Q.all(msgs.map(function (m) {
        return self.lookupObject(m)
      }))
    })
}

Driver.prototype.lookupObjectByCurHash = function (curHash) {
  return Q.ninvoke(this.messages(), 'byCurHash', curHash)
    .then(this.lookupObject)
}

Driver.prototype.lookupObject = function (info, verify) {
  var self = this

  if (this._destroyed) {
    return Q.reject(new Error('already destroyed'))
  }

  // TODO: this unfortunately duplicates part of unchainer.js
  var canLookup = info.txData || info.tx
  if (!canLookup) {
    if (info.txType === TxData.types.public && info[CUR_HASH]) {
      info.txData = new Buffer(info[CUR_HASH], 'hex')
      canLookup = true
    }

    if (!canLookup) {
      throw new Error('missing required info to lookup chained obj')
    }
  }

  if (!verify) {
    var cached = this._getCachedObject(info)
    if (cached) return Q(cached)
  }

  return this.unchainer.unchain(clone(info), {
      verify: !!verify
    })
    .then(function (obj) {
      self._cacheObject(obj)
      return obj
    })
}

Driver.prototype.byType = function (type) {
  var self = this
  return this.typeDB.createReadStream(type).pipe(map(function (txData, cb) {
    self.lookupObject({ txData: txData })
      .then(function (obj) {
        cb(null, obj)
      }, function (err) {
        cb(err)
      })
  }))
}

Driver.prototype._enableObjectCaching = function () {
  var self = this
  if (this._objectCache) return

  this._objectCache = new Cache({
    max: 100
  })

  ;['message', 'sent', 'chained', 'unchained'].forEach(function (event) {
    self.msgDB.on(event, function (entry) {
      self._debug('event', event, entry.uid)
      var cached = self._getCachedObject(entry)
      if (!cached) return

      // cached object is frozen
      cached = clone(cached)
      for (var p in entry) {
        if (!(p in cached)) {
          cached[p] = entry[p]
        }
      }

      self._cacheObject(cached)
    })
  })
}

Driver.prototype._getCachedObject = function (info) {
  if (!this._objectCache) return

  var keys = this._getCachedObjectKeys(info)
  for (var i = 0; i < keys.length; i++) {
    var cached = this._objectCache.get(keys[i])
    if (cached) return cached
  }
}

Driver.prototype._cacheObject = function (obj) {
  if (!this._objectCache) return

  obj = Object.freeze(obj)

  this._getCachedObjectKeys(obj).forEach(function (key) {
    this._objectCache.set(key, obj)
  }, this)
}

Driver.prototype._uncacheObject = function (obj) {
  if (!this._objectCache) return

  this._getCachedObjectKeys(obj).forEach(function (key) {
    this._objectCache.del(key)
  }, this)
}

Driver.prototype._getCachedObjectKeys = function (obj) {
  var keys = []
  // if (obj[CUR_HASH]) keys.push(obj[CUR_HASH])
  if (obj.txData) keys.push(obj.txData.toString('hex'))

  return keys
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
  return utils.firstKey(this.keys, where)
}

Driver.prototype.getBlockchainKey = function () {
  return toKey(this.getPrivateKey({
    networkName: this.networkName,
    type: 'bitcoin',
    purpose: Driver.BLOCKCHAIN_KEY_PURPOSE
  }))
}

Driver.prototype.getCachedIdentity = function (query) {
  if (this._destroyed) throw new Error('already destroyed')

  for (var p in query) {
    var cached = this._identityCache[query[p]]
    if (cached) return cached
  }
}

Driver.prototype._cacheIdentity = function (identityInfo, query) {
  var queries = []
  var identity = identityInfo.identity
  identity.pubkeys.forEach(function (k) {
    queries.push(k.fingerprint, k.value)
  })

  // each fingerprint/pubkey/hash is enough
  // to uniquely identify an identity
  var rh = identityInfo[ROOT_HASH]
  var ch = identityInfo[CUR_HASH]
  if (rh) queries.push(rh)
  if (ch) queries.push(ch)
  if (query) {
    for (p in query) {
      queries.push(p)
    }
  }

  queries.forEach(function (q) {
    this._identityCache[q] = identityInfo
  }, this)

  var size = Object.keys(this._identityCache)
  if (size > 1000) {
    for (var p in this._identityCache) {
      delete this._identityCache[p]
      if (--size < 1000) break
    }
  }
}

Driver.prototype.lookupIdentity = function (query) {
  var self = this
  var me = this.identityJSON
  var isMe = query[ROOT_HASH] === this.myRootHash() ||
    query[CUR_HASH] === this.myCurrentHash() ||
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
    .then(function (result) {
      self._cacheIdentity(result, query)
      return result
    })
}

Driver.prototype.log = function (entry) {
  utils.jsonifyErrors(entry)
  var defer = Q.defer()
  // faster than ninvoke
  this._log.append(entry, function (err) {
    if (err) defer.reject(err)
    // pass through for convenience
    else defer.resolve(entry)
  })

  return defer.promise
}

Driver.prototype.createReadStream = function (options) {
  return this._log.createReadStream(options)
}

Driver.prototype._prefix = function (path) {
  return this.pathPrefix + '-' + path
}

/**
 * Manually add a known identity
 * @param {Object|Buffer} identity
 */
Driver.prototype.addContactIdentity = function (identity) {
  var self = this
  var identityJSON = typeof identity === 'string' || Buffer.isBuffer(identity)
    ? JSON.parse(identity)
    : identity

  var curHash
  var buf = utils.toBuffer(identity)
  return Q.ninvoke(tradleUtils, 'getStorageKeyFor', buf)
    .then(function (_curHash) {
      curHash = _curHash.toString('hex')
      return Q.ninvoke(self.addressBook, 'byCurHash', curHash)
    })
    .catch(add)

  function add () {
    return self.keeper.put(curHash, buf)
      .then(function () {
        rootHash = identityJSON[ROOT_HASH] || curHash
        var fromAddress = identityJSON.pubkeys.filter(function (key) {
          return key.purpose === Driver.BLOCKCHAIN_KEY_PURPOSE && key.networkName === self.networkName
        })[0].fingerprint

        var entry = new Entry({
            type: EventType.misc.addIdentity,
            txType: TxData.types.public,
            parsed: identityJSON,
            from: utils.toObj(ROOT_HASH, rootHash),
            to: utils.toObj(ROOT_HASH, self.myRootHash()),
            addressesFrom: [fromAddress],
            addressesTo: [self.wallet.addressString]
          })
          .set(TYPE, TYPES.IDENTITY)
          .set(ROOT_HASH, rootHash)
          .set(CUR_HASH, curHash)

        return self.log(entry)
      })
  }
}

// Driver.prototype._receivePublicMsg = function (msg, senderInfo) {
//   var self = this

//   try {
//     var parsed = JSON.parse(msg.data)
//   } catch (err) {
//     valid = false
//   }

//   var putPromise
//   if (parsed[TYPE] === 'tradle.IdentityPublishRequest') {
//     putPromise = this.keeper.put(parsed)
//   }

//   return Q(putPromise)
//     .then(function () {
//       var txInfo = {
//         addressesFrom: [senderInfo.fingerprint],
//         addressesTo: [self.wallet.addressString],
//         txData: msg.txData,
//         txType: msg.txType
//       }

//       return self.chainloader._processTxInfo(txInfo)
//     })
//     .then(this.lookupObject)
//     .then(function (obj) {
//       debugger
//     })
//     .catch(function (error) {
//       debugger
//     })
// }

Driver.prototype.receiveMsg = function (buf, senderInfo) {
  var self = this
  if (this._destroyed) {
    return Q.reject(new Error('already destroyed'))
  }

  var msg

  validateRecipients(senderInfo)

  try {
    msg = utils.bufferToMsg(buf)
  } catch (err) {
    return Q.reject(new Error('expected JSON'))
  }

  this._debug('received msg')

  var timestamp = hrtime()

  // this thing repeats work all over the place
  var chainedObj
  var promiseValid
  var valid = utils.validateMsg(msg)
  if (valid) {
    // if (msg.txType === TxData.types.public) {
    //   return this._receivePublicMsg(msg, senderInfo)
    // }

    promiseValid = Q.resolve()
  } else {
    promiseValid = Q.reject(new Error('received invalid msg'))
  }

  var from
  return this._receivingPromise = promiseValid
    .then(function () {
      return self.lookupIdentity(senderInfo)
    })
    .then(function (result) {
      from = result
      var fromKey = utils.firstKey(result.identity.pubkeys, {
        type: 'bitcoin',
        networkName: self.networkName,
        purpose: 'messaging'
      })

      chainedObj = {
        timestamp: timestamp,
        addressesFrom: [fromKey.fingerprint],
        addressesTo: [self.wallet.addressString],
        // txData: msg.txData,
        txType: msg.txType,
        data: msg.data,
        encryptedData: msg.encryptedData,
        encryptedPermission: msg.encryptedPermission
      }

      // TODO: rethink how chainloader should work
      // this doesn't look right

      // this will verify that the message
      // comes from the sender in senderInfo
      return self.chainloader._processTxInfo(chainedObj)
    })
    .then(function (info) {
      extend(chainedObj, info)
      if (info.txType === TxData.types.public) {
        return loadPublicMessage()
      } else {
        return loadPrivateMessage()
      }
    })
    .then(function () {
      return self.lookupObject(chainedObj, true)
    })
    .then(function (obj) {
      return self.unchainResultToEntry(obj)
    })
    .then(function (entry) {
      return entry.set({
        type: EventType.msg.receivedValid,
        dir: Dir.inbound
      })
    })
    .catch(function (err) {
      // TODO: retry
      self._debug('failed to process inbound msg', err.message, err.stack)
      return new Entry({
        type: EventType.msg.receivedInvalid,
        msg: msg,
        from: utils.toObj(ROOT_HASH, from && from[ROOT_HASH]),
        to: utils.toObj(ROOT_HASH, self.myRootHash()),
        dir: Dir.inbound,
        errors: {
          receive: [err]
        }
      })
    })
    .then(function (entry) {
      self._debug('processed received msg')
      return self.log(entry)
    })

  function loadPublicMessage () {
    // nothing to do here
    return self.keeper.putOne({
      key: chainedObj.key,
      value: chainedObj.data
    })
  }

  function loadPrivateMessage () {
    return Q.ninvoke(Permission, 'recover', msg.encryptedPermission, chainedObj.sharedKey)
      .then(function (permission) {
        chainedObj.permission = permission
        if (!chainedObj.permissionKey) {
          return derivePermissionKey(chainedObj)
        }
      })
      .then(function () {
        return self.keeper.putMany([
          {
            key: chainedObj.permissionKey.toString('hex'),
            value: msg.encryptedPermission
          },
          {
            key: chainedObj.permission.fileKeyString(),
            value: msg.encryptedData
          }
        ])
      })
  }
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
  assert(entry[ROOT_HASH] && entry[CUR_HASH], 'missing required fields')

  var type = entry.txType
  var data = entry.txData
  var nextEntry = new Entry()
    .set(utils.pick(entry, ROOT_HASH, CUR_HASH, TYPE, 'uid', 'txType'))

  var addr = entry.dir === Dir.outbound
    ? entry.addressesTo[0]
    : entry.addressesFrom[0]

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

      utils.addError({
        entry: nextEntry,
        group: 'chain',
        error: err
      })
    })
    .then(function () {
      self._updateBalance()
      return self.log(nextEntry)
    })
}

Driver.prototype._signee = function () {
  return this.myRootHash() + ':' + this.myCurrentHash()
}

Driver.prototype.sign = function (msg) {
  typeforce('Object', msg, true) // strict
  var signee = msg[SIGNEE] || this._signee()
  return new Builder()
    .data(msg)
    .signWith(this._signingKey, signee)
    .build()
}

/**
 * chain an existing message
 * @param  {String} uid - uid by which to find this message in msgDB
 * @return {Promise}
 */
Driver.prototype.chainExisting = function (uid) {
  var self = this

  if (typeof uid !== 'string') {
    uid = utils.getUID(uid)
  }

  typeforce('String', uid)

  return Q.ninvoke(this.msgDB, 'byUID', uid)
    .then(function (info) {
      if (info.chain || info.dateUnchained) {
        throw new Error('already chained')
      }

      var entry = new Entry({
        uid: uid,
        type: EventType.msg.edit,
        chain: true
      })

      return self.log(entry)
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
    to: typeforce.oneOf('Array', 'Object'),
    chain: '?Boolean',
    deliver: '?Boolean'
  }, options)

  assert(CUR_HASH in options, 'expected current hash of object being shared')

  var to = [].concat(options.to)
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
        .set(TYPE, obj[TYPE])

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
          txData: utils.toBuffer(share.encryptedKey, 'hex'),
          v: Driver.PROTOCOL_VERSION
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
  var msg = Buffer.isBuffer(options.msg) ? JSON.parse(options.msg) : options.msg
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
    from: utils.toObj(ROOT_HASH, this.myRootHash()),
    [PREV_HASH]: msg[PREV_HASH],
    [ROOT_HASH]: msg[ROOT_HASH]
  })

  var recipients
  var blockchainAddrs
  var blockchainKeys
  // var parsed
  return this._readyPromise
    // validate
    .then(function () {
      return Parser.parse(data)
    })
    .then(function (parsed) {
      entry.set(TYPE, parsed.data[TYPE])
      return isPublic && to.every(function (r) { return r.fingerprint })
        ? Q.resolve(to)
        : Q.all(to.map(self.lookupIdentity, self))
    })
    .then(function (_recipients) {
      recipients = _recipients
      if (recipients === to) {
        blockchainAddrs = to
      } else {
        var rawKeys = utils.pluck(recipients, 'identity')
          .map(self._getBTCKey, self)

        blockchainKeys = rawKeys
          .map(function (k) {
            return k.value
          })

        blockchainAddrs = rawKeys
          .map(function (k) {
            return { fingerprint: k.fingerprint }
          })
      }

      return self.chainwriter.create()
        .data(data)
        .setPublic(isPublic)
        .recipients(blockchainKeys || blockchainAddrs)
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
            addressesTo: [blockchainAddrs[i].fingerprint],
            txType: TxData.types.public,
            txData: utils.toBuffer(resp.key, 'hex'),
            v: Driver.PROTOCOL_VERSION
          })
        })
      } else {
        entries = resp.shares.map(function (share, i) {
          return entry.clone().set({
            to: utils.toObj(ROOT_HASH, recipients[i][ROOT_HASH]),
            addressesTo: [share.address],
            addressesFrom: [self.wallet.addressString],
            txType: TxData.types.permission,
            txData: utils.toBuffer(share.encryptedKey, 'hex'),
            v: Driver.PROTOCOL_VERSION
          })

          // side effect that can be cached in objectCache
          // var json = shareEntry.toJSON()
          // json.key = resp.key
          // json.data = data
          // json.parsed = parsed
          // json.permission = share.permission
          // json.permissionKey = share.permission.key().toString('hex')
          // json.sharedKey = share.ecdhKey
          // json.encryptedKey = json.txData
          // json.encryptedPermission = share.value
          // json.encryptedData = resp.value
          // json.from = { identity: Identity.fromJSON(self.identityJSON) }
          // json.from[ROOT_HASH] = self.myRootHash()
          // json.to = { identity: Identity.fromJSON(recipients[i].identity) }
          // json.to[ROOT_HASH] = recipients[i][ROOT_HASH]
          // utils.setUID(json)
          // // console.log('share Entry', shareEntry.toJSON())
          // self._cacheObject(json)
          // return shareEntry
        })
      }

      entries.forEach(utils.setUID)
      return Q.all(entries.map(self.log, self))
    })
}

// Driver.prototype._cache = function (chainedObj) {
//   this._cachedObjs = this._cachedObjs || {}
//   this._cachedObjs[utils.getUID(chainedObj)] = chainedObj
// }

Driver.prototype.forget = function (rootHash) {
  var self = this
  typeforce('String', rootHash)
  var forgotten
  return this.getConversation(rootHash)
    .then(function (msgs) {
      forgotten = msgs
      return getOnlyChildren(msgs)
    })
    .then(function (curHashes) {
      forgotten.forEach(function (info) {
        info.deleted = curHashes.indexOf(info[CUR_HASH]) !== -1
        self._uncacheObject(info)
      })

      return self.keeper.removeMany(curHashes)
    })
    .then(function () {
      return self.log(new Entry({
        type: EventType.misc.forget,
        who: rootHash
      }))
    })
    .then(function () {
      return forgotten
    })

  function getOnlyChildren (msgs) {
    var byCurHash = {}
    var curHashes = utils.pluck(msgs, CUR_HASH)
    var stream = self.msgDB.createValueStream()
      .on('data', function (data) {
        var curHash = data[CUR_HASH]
        if (curHashes.indexOf(curHash) !== -1) {
          byCurHash[curHash] = (byCurHash[curHash] || 0) + 1
        }
      })
      .on('end', function () {
        var onlyChildren = Object.keys(byCurHash).filter(function (curHash) {
          return byCurHash[curHash] === 1
        })

        defer.resolve(onlyChildren)
      })

    var defer = Q.defer()
    return defer.promise
  }
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
    purpose: Driver.BLOCKCHAIN_KEY_PURPOSE
  })
}

Driver.prototype.lookupBTCKey = function (recipient) {
  var self = this
  return this.lookupIdentity(recipient)
    .then(function (result) {
      return utils.firstKey(result.identity.pubkeys, {
        type: 'bitcoin',
        networkName: self.networkName,
        purpose: Driver.BLOCKCHAIN_KEY_PURPOSE
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
  clearTimeout(this._fetchTxsTimeout)

  for (var name in this._streams) {
    this._streams[name].destroy()
  }

  var tasks = [
    self.keeper.destroy(),
    Q.ninvoke(self.addressBook, 'close'),
    Q.ninvoke(self.msgDB, 'close'),
    Q.ninvoke(self.txDB, 'close'),
    Q.ninvoke(self.miscDB, 'close'),
    Q.ninvoke(self._log, 'close')
  ]

  delete this._ignoreTxs
  delete this._identityCache
  // async
  return Q.all(tasks)
    .then(function () {
      self._debug('destroyed!')
    })

    // .then(function () {
    //   self.removeAllListeners()
    // })
// .done(console.log.bind(console, this.pathPrefix + ' is dead'))
}

Driver.prototype._debug = function () {
  if (!debug.enabled) return

  var args = [].slice.call(arguments)
  args.unshift(this.name())
  return debug.apply(null, args)
}

Driver.prototype.options = function () {
  return clone(this._options)
}

Driver.prototype.history = function (otherPartyRootHash) {
  var self = this
  var getMsgs = otherPartyRootHash
    ? this.getConversation(otherPartyRootHash)
    : Q.nfcall(collect, this.msgDB.createValueStream())

  return getMsgs.then(function (msgs) {
    return Q.all(msgs.map(function (msg) {
      return self.lookupObject(msg)
    }))
  })
}

Driver.prototype.getConversation = function (rootHash) {
  return Q.ninvoke(this.msgDB, 'getConversation', rootHash)
}

Driver.prototype._getWatchedAddresses = function () {
  return this._watchedAddresses.slice()
}

Driver.prototype._getWatchedTxs = function () {
  return this._watchedTxs.slice()
}

/**
 * TODO: record why we're watching a particular address
 * so we can stop watching it later
 */
Driver.prototype.watchAddresses = function (/* addrs */) {
  var self = this
  var addrs = utils.argsToArray(arguments)
  typeforce(typeforce.arrayOf('String'), addrs)

  this._loadWatchesPromise.then(function () {
    addrs = addrs.filter(function (addr) {
      return self._watchedAddresses.indexOf(addr) === -1
    })

    if (!addrs.length) return

    self.log(new Entry({
      type: EventType.misc.watchAddresses,
      addresses: addrs
    }))
  })

  return this
}

Driver.prototype.watchTxs = function (/* txIds */) {
  var self = this
  var txIds = utils.argsToArray(arguments)
  typeforce(typeforce.arrayOf('String'), txIds)

  return this._loadWatchesPromise
    .then(function () {
      txIds = txIds.filter(function (txId) {
        return self._watchedTxs.indexOf(txId) === -1
      })

      if (!txIds.length) return

      return Q.all(txIds.map(function (txId) {
        return self.log(new Entry({
          type: EventType.tx.watch,
          txId: txId
        }))
      }))
    })
}

Driver.prototype.unwatchAddresses = function (/* addrs */) {
  var self = this
  var addrs = utils.argsToArray(arguments)
  typeforce(typeforce.arrayOf('String'), addrs)

  return this.ready()
    .then(function () {
      addrs = addrs.filter(function (addr) {
        return self._watchedAddresses.indexOf(addr) !== -1
      })

      if (!addrs.length) return

      self.log(new Entry({
        type: EventType.misc.unwatchAddresses,
        addresses: addrs
      }))
    })

  return this
}

Driver.prototype._loadWatches = function () {
  var self = this
  if (this._loadWatchesPromise) return this._loadWatchesPromise

  return this._loadWatchesPromise = Q.all([
    this._loadWatchedAddrs(),
    this._loadWatchedTxs()
  ])
}

Driver.prototype._loadWatchedAddrs = function () {
  var self = this
  return Q.ninvoke(this.miscDB, 'getWatchedAddresses')
    .then(this._setWatchedAddresses)
    .then(function () {
      var addrs = self.identityJSON.pubkeys
        .filter(function (k) {
          return k.type === 'bitcoin' &&
            k.networkName === self.networkName
        })
        .map(function (k) {
          return k.fingerprint
        })

      return self.watchAddresses(addrs)
    })
}

Driver.prototype._setWatchedAddresses = function (addrs) {
  var hasNew = addrs.some(function (addr) {
    return this._watchedAddresses.indexOf(addr) === -1
  }, this)

  this._watchedAddresses = addrs
  if (hasNew) this.sync(true)
}

Driver.prototype._addresses = function () {
  return this._watchedAddresses.slice()
}

Driver.prototype._setWatchedTxs = function (txIds) {
  var hasNew = txIds.some(function (txId) {
    return this._watchedTxs.indexOf(txId) === -1
  }, this)

  this._watchedTxs = txIds
  this._ignoreTxs = this._ignoreTxs.filter(function (txId) {
    return this._watchedTxs.indexOf(txId) !== -1
  }, this)

  if (hasNew) this.sync(true)
}

Driver.prototype._addresses = function () {
  return this._watchedAddresses.slice()
}

Driver.prototype._loadWatchedTxs = function () {
  return Q.ninvoke(this.miscDB, 'getWatchedTxs')
    .then(this._setWatchedTxs)
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
    cb()
    return
  }

  var lastErr = errors[errors.length - 1]
  if (!lastErr.timestamp) {
    debug('bad error: ', lastErr)
    throw new Error('error is missing timestamp')
  }

  var now = utils.now()
  // exponential back-off
  var wait = lastErr.timestamp + Math.pow(2, errors.length) * throttle - now
  if (wait < 0) {
    cb()
    return
  }

  // just in case the device clock time-traveled
  // max wait an hour
  wait = Math.min(wait, throttle, 3600000) | 0
  setTimeout(cb, wait)
  return wait
}

function alwaysFalse () {
  return false
}

function derivePermissionKey (chainedObj) {
  var defer = Q.defer()
  tradleUtils.getStorageKeyFor(chainedObj.encryptedPermission, function (err, key) {
    if (err) return defer.reject(err)

    chainedObj.permissionKey = key
    tradleUtils.encryptAsync({
      data: key,
      key: chainedObj.sharedKey
    }, function (err, encryptedPermissionKey) {
      if (err) return defer.reject(err)

      chainedObj.txData = chainedObj.encryptedKey = encryptedPermissionKey
      defer.resolve()
    })
  })

  return defer.promise
}

function throwIfDev (err) {
  if (DEV) throw err
}

// function promiseDebounce (fn, ctx) {
//   var pending = null
//   function clear() { pending = null }

//   return function() {
//     if (pending) return pending
//     pending = fn.apply(ctx, arguments)
//     pending.finally(clear, clear)
//     return pending
//   }
// }
