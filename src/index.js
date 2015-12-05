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
var ChainedObj = require('@tradle/chained-obj')
var TxData = require('@tradle/tx-data').TxData
var TxInfo = require('@tradle/tx-data').TxInfo
var ChainWriter = require('@tradle/bitjoe-js')
var ChainLoader = require('@tradle/chainloader')
var Permission = require('@tradle/permission')
var Wallet = require('@tradle/simple-wallet')
// var cbstreams = require('@tradle/cb-streams')
var Zlorp = require('zlorp')
var Messengers = require('./messengers')
var hrtime = require('monotonic-timestamp')
var mi = require('@tradle/identity')
var Identity = mi.Identity
var kiki = require('@tradle/kiki')
var toKey = kiki.toKey
var Builder = ChainedObj.Builder
var Parser = ChainedObj.Parser
var lb = require('logbase')
var Entry = lb.Entry
var unchainer = require('./unchainer')
var constants = require('@tradle/constants')
var EventType = require('./eventType')
var Dir = require('./dir')
var createIdentityDB = require('./identityDB')
var createMsgDB = require('./msgDB')
var createTxDB = require('./txDB')
var Errors = require('./errors')
var utils = require('./utils')
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
var KEY_PURPOSE = 'messaging'
Driver.MIN_BALANCE = 10000
Driver.CHAIN_WRITE_THROTTLE = 60000
Driver.CHAIN_READ_THROTTLE = 60000
Driver.SEND_THROTTLE = 10000
Driver.CATCH_UP_INTERVAL = 2000
Driver.Zlorp = Zlorp
Driver.Kiki = kiki
Driver.Identity = Identity
Driver.Wallet = Wallet
Driver.Messengers = Messengers
Driver.EventType = EventType
// TODO: export other deps

var noop = function () {}

module.exports = Driver
util.inherits(Driver, EventEmitter)

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
    leveldown: 'Function',
    port: 'Number',
    pathPrefix: 'String',
    dht: '?Object',
    messenger: '?Object',
    syncInterval: '?Number',
    chainThrottle: '?Number',
    readOnly: '?Boolean',
    relay: '?Object',
    afterBlockTimestamp: '?Number'
  }, options)

  EventEmitter.call(this)
  tradleUtils.bindPrototypeFunctions(this)
  this._options = options
  extend(this, options)

  this.chainThrottle = this.chainThrottle || Driver.CHAIN_WRITE_THROTTLE
  this.syncInterval = this.syncInterval || Driver.CHAIN_READ_THROTTLE

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

  this._paused = false

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

  this._streams = {}

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

Driver.prototype.setHttpClient = function (client) {
  this.httpClient = client
}

Driver.prototype.setHttpServer = function (server) {
  this.httpServer = server
}

Driver.prototype.addSender = function (messenger, rootHash) {
  typeforce('String', rootHash)
  this._senders[rootHash] = messenger
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
    toObjectStream(),
    map(function (entry, cb) {
      // was read from chain and hasn't been processed yet
      // self._debug('unchaining tx', entry.txId)
      if (!entry.dateDetected || entry.dateUnchained || !entry.txData) {
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

      var txId = entry.txId
      if (errs.length >= Errors.MAX_UNCHAIN) {
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

      if (!utils.countErrors(chainedObj) &&
        chainedObj.txType === TxData.types.public) {
        self._debug('saving to keeper')
        self.keeper.put(chainedObj.key, chainedObj.data)
      }


      self.unchainResultToEntry(chainedObj)
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

Driver.prototype._catchUpWithBlockchain = async function () {
  var self = this
  var txIds
  while (true) {
    if (this._destroyed) return

    if (!txIds) {
      try {
        var txInfos = await this._doFetchTxs()
      } catch (err) {
        this._debug('failed to fetch txs', err.stack)
        await utils.delay(Driver.CATCH_UP_INTERVAL)
        continue
      }

      txInfos = utils.parseCommonBlockchainTxs(txInfos)
      txIds = txInfos
        .filter(function (txInfo) {
          return txInfo.blockTimestamp > self.afterBlockTimestamp
        })
        .map(function (txInfo) {
          return txInfo.tx.getId()
        })
    }

    try {
      await checkDBs()
    } catch (err) {
      this._debug('waiting to for txs to get processed...', err)
      await utils.delay(Driver.CATCH_UP_INTERVAL)
      continue
    }

    break
  }

  async function checkDBs () {
    await Q.all(txIds.map(async function (txId) {
      var entry = await Q.ninvoke(self.txDB, 'get', txId)
      var hasErrors = !!utils.countErrors(entry)
      var processed = entry.dateUnchained || entry.dateChained || hasErrors
      if (!processed) throw new Error('not ready')
    }))
  }
}

Driver.prototype.identityPublishStatus = async function () {
  // check if we've already chained it

  await this._readyPromise

  var rh = this.myRootHash()
  var me = this.identityJSON
  var status = {
    ever: false,
    current: false,
    queued: false
  }

  await this._catchUpWithBlockchain()
  var entriesPromise = Q.ninvoke(this.msgDB, 'byRootHash', rh)
  var curHashPromise = Q.ninvoke(tradleUtils, 'getStorageKeyFor', utils.toBuffer(me))
  var promises = [
    entriesPromise,
    curHashPromise
  ]

  try {
    var results = await Q.all(promises)
  } catch (err) {
    if (!err.notFound) throw err

    return status
  }

  var entries = results[0]
  var curHash = results[1].toString('hex')

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

  return status
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

Driver.prototype.publishMyIdentity = async function () {
  var self = this

  if (this._publishingIdentity) {
    throw new Error('wait till current publishing process ends')
  }

  this._publishingIdentity = true

  try {
    await this._readyPromise
    var status = await this.identityPublishStatus()
    if (!status.ever) return this.publishIdentity()
    if (status.queued) {
      throw new Error('already publishing this version')
    }

    if (status.current) {
      throw new Error('already published this version')
    }

    var priv = this.getPrivateKey({ purpose: 'update' })
    var update = extend({}, this.identityJSON)
    var prevHash = this.myCurrentHash() || this.myRootHash()
    utils.updateChainedObj(update, prevHash)

    var nonce = await Q.ninvoke(Builder, 'addNonce', update)
    var builder = Builder()
      .data(update)
      .signWith(toKey(priv))

    var build = await Q.ninvoke(builder, 'build')
    this.setIdentity(update)
    extend(this.identityMeta, utils.pick(update, PREV_HASH, ROOT_HASH))
    await Q.all([
      this._prepIdentity(),
      this.publishIdentity(build.form)
    ])

    delete this._publishingIdentity
  } finally {
    this._publishingIdentity = false
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

  if ('key' in chainedObj) {
    entry.set(CUR_HASH, chainedObj.key)
  }

  if ('parsed' in chainedObj) {
    entry
      .set(ROOT_HASH, chainedObj.parsed.data[ROOT_HASH] || chainedObj.key)
      .set(TYPE, chainedObj.parsed.data[TYPE])
      .set('public', chainedObj.txType === TxData.types.public)
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
    shouldSkipQueue: function (state) {
      return state.public
    },
    shouldTryLater: function (state) {
      if (self._balance < Driver.MIN_BALANCE) {
        self._updateBalance()
        return true
      }
    }
  })
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
    processItem: this._trySend,
    retryDelay: Driver.SEND_THROTTLE,
    successType: EventType.msg.sendSuccess
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
  var stream = opts.getStream()
  var sync
  stream.once('sync', function () {
    sync = true
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
          throw err
        }

        if (shouldSkipQueue(state)) {
          runASAP(state)
        } else {
          insert(state)
          processQueue(state.to[ROOT_HASH])
        }

        cb()
      })
    })
  )

  this._pauseStreamIfPaused(stream)

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
      .done(function (entry) {
        if (self._destroyed) return

        self._debug('processed item from queue', name)
        q.processing = false
        var isFinished = utils.getEntryProp(entry, 'type') === successType
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
    var rid = data.to[ROOT_HASH]
    var q = queues[rid] = queues[rid] || []
    if (!sync) return q.push(data)

    var exists = q.some(function (item) {
      return item.uid === data.uid
    })

    if (!exists) q.push(data)
  }

  function remove (data) {
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

Driver.prototype._trySend = function (entry) {
  // this._markSending(entry)
  var self = this
  var nextEntry = new Entry()
    .set(utils.pick(entry, 'uid', ROOT_HASH))

  // return Q.ninvoke(this.msgDB, 'onLive')
  //   .then(function () {
      return self._doSend(entry)
    // })
    .then(function () {
      self._debug('msg sent successfully')
      return nextEntry.set('type', EventType.msg.sendSuccess)
    })
    .catch(function (err) {
      self._debug('msg send failed', err.message)
      nextEntry.set({
        type: EventType.msg.sendError
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

  this._runFetchTxsLoop(0, [])
  return Q.resolve()
}

Driver.prototype._runFetchTxsLoop = function (fromHeight, skipIds) {
  var self = this
  if (this._destroyed) return

  if (!fromHeight) fromHeight = 0

  // var stream = this._streams.rawTxs = new PassThrough({
  //   objectMode: true
  // })

  // stream.destroy = stream.end

  this._scheduleFetch()
  this._fetchTxs()
}

Driver.prototype._scheduleFetch = function () {
  if (this._destroyed) return
  if (this._paused) {
    return this.once('resume', this._scheduleFetch)
  }

  this._fetchTxsTimeout = setTimeout(this._fetchTxs, this.syncInterval)
}

Driver.prototype._doFetchTxs = function () {
  return Q.ninvoke(this.blockchain.addresses, 'transactions', this._addresses(), null)
}

Driver.prototype._fetchTxs = function () {
  var self = this
  this.blockchain.addresses.transactions(this._addresses(), function (err, txInfos) {
    if (!err && txInfos) {
      self._processTxs(txInfos)
    }
  })
}

Driver.prototype._processTxs = async function (txInfos) {
  var self = this

  this._scheduleFetch()

  var lookups = []
  var filtered = utils.parseCommonBlockchainTxs(txInfos)
    .filter(function (txInfo) {
      if (txInfo.blockTimestamp &&
          txInfo.blockTimestamp <= self.afterBlockTimestamp) {
        return
      }

      var id = txInfo.tx.getId()
      if (self._pendingTxs.indexOf(id) !== -1) {
        return
      }

      // TODO: filter shouldn't have side effects
      self._pendingTxs.push(id)
      txInfo.id = id

      // run in parallel
      lookups.push(Q.ninvoke(self.txDB, 'get', id))
      return true
    })

  return await Q.all(lookups.map(async function (lookup, i) {
    var err
    try {
      var entry = await lookup
    } catch (e) {
      err = e
    }

    var txInfo = filtered[i]
    var id = txInfo.id
    var unexpectedErr = err && !err.notFound
    if (unexpectedErr) {
      self._debug('unexpected txDB error: ' + JSON.stringify(err))
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
    if (entry) {
      if (entry.dateDetected) {
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
    } else {
      type = EventType.tx.new
    }

    // console.log(TxInfo.parse(txInfo.tx))
    var parsedTx = TxInfo.parse(txInfo.tx, self.networkName, PREFIX)
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
        dir: isOutbound ? Dir.outbound : Dir.inbound
      }
    ))

    // clear errors
    nextEntry.set('errors', {})
    return self.log(utils.jsonifyErrors(nextEntry))
  }))
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
  return this.lookupObject(entry)
    .then(function (obj) {
      obj.to.identity = obj.to.identity.toJSON()
      self._debug('sending msg to peer', obj.parsed.data[TYPE])
      var msg = utils.msgToBuffer(utils.getMsgProps(obj))
      var toRootHash = obj.to[ROOT_HASH]
      var messenger = self.messenger
      if (self.httpClient && self.httpClient.hasEndpointFor(toRootHash)) {
        messenger = self.httpClient
      }

      return messenger.send(toRootHash, msg, obj.to)
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

  if (this._destroyed) {
    return Q.reject(new Error('already destroyed'))
  }

  // TODO: this unfortunately duplicates part of unchainer.js
  if (!info.txData) {
    if (!info.tx) {
      throw new Error('missing required info to lookup chained obj')
    }
  }

  var chainedObj
  return this.chainloader.load(clone(info))
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
  if (this._destroyed) throw new Error('already destroyed')

  return this._identityCache[tradleUtils.stringify(query)]
}

Driver.prototype._cacheIdentity = function (query, value) {
  this._identityCache[tradleUtils.stringify(query)] = value
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
  utils.jsonifyErrors(entry)
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
//   return (this._receiving || Q())
//     .finally(this._receiveMsg.bind(this, buf, senderInfo))
// }

// Driver.prototype._receiveMsg = function (buf, senderInfo) {
  var self = this
  if (this._destroyed) {
    return Q.reject(new Error('already destroyed'))
  }

  var msg

  validateRecipients(senderInfo)

  try {
    msg = utils.bufferToMsg(buf)
  } catch (err) {
    return this.emit('warn', 'received message not in JSON format', buf)
  }

  this._debug('received msg', msg)

  var timestamp = hrtime()

  // this thing repeats work all over the place
  var txInfo
  var promiseValid
  var valid = utils.validateMsg(msg)
  if (valid) promiseValid = Q.resolve()
  else promiseValid = Q.reject(new Error('received invalid msg'))

  var from
  return this._receiving = promiseValid
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
      self._debug('failed to process inbound msg', err.message, err.stack)
      return new Entry({
        type: EventType.msg.receivedInvalid,
        msg: msg,
        from: utils.toObj(ROOT_HASH, from && from[ROOT_HASH]),
        to: utils.toObj(ROOT_HASH, self.myRootHash()),
        dir: Dir.inbound,
        errors: {
          receive: err
        }
      })
    })
    .then(function (entry) {
      entry.set('timestamp', timestamp)
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
  assert(entry[ROOT_HASH] && entry[CUR_HASH], 'missing required fields')

  var type = entry.txType
  var data = entry.txData
  var nextEntry = new Entry()
    .set(utils.pick(entry, ROOT_HASH, CUR_HASH, TYPE, 'uid', 'txType'))
    .set({
      chain: true,
      dir: Dir.outbound
    })

//   return this.lookupBTCAddress(to)
//     .then(shareWith)
  var addr = entry.addressesTo[0]
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

Driver.prototype.share = async function (options) {
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

  var recipients = await Q.all(to.map(this.lookupIdentity, this))
  var objInfo = await Q.ninvoke(this.msgDB, 'byCurHash', curHash)
  var obj = await this.lookupObject(objInfo)
  entry.set(CUR_HASH, curHash)
    .set(ROOT_HASH, obj[ROOT_HASH])
    .set(TYPE, obj[TYPE])

  var symmetricKey = obj.permission.body().decryptionKey
  var shares = await Q.all(recipients.map(function (r) {
    var pubkey = self._getBTCKey(r.identity)
    return self.chainwriter.share()
      .shareAccessTo(curHash, symmetricKey)
      .shareAccessWith(pubkey.value)
      .execute()
  }))

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
  await Q.all(entries.map(this.log, this))
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
Driver.prototype.send = async function (options) {
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
  await this._readyPromise
  // validate
  var parsed = await Q.ninvoke(Parser, 'parse', data)
  entry.set(TYPE, parsed.data[TYPE])
  if (isPublic) {
    recipients = btcKeys = to
  } else {
    recipients = await Q.all(to.map(this.lookupIdentity, this))
    btcKeys = utils.pluck(recipients, 'identity')
      .map(this._getBTCKey, this)
      .map(function (k) {
        return k.value
      })
  }

  var resp = await this.chainwriter.create()
    .data(data)
    .setPublic(isPublic)
    .recipients(btcKeys)
    .execute()

  copyDHTKeys(entry, resp.key)
  this._debug('stored (write)', entry.get(ROOT_HASH))

  var entries
  if (isPublic) {
    this._push(resp)
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
  await Q.all(entries.map(this.log, this))
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
  return this.lookupBTCKey(recipient)
    .then((k) => k.value)
}

Driver.prototype.lookupBTCAddress = function (recipient) {
  return this.lookupBTCKey(recipient)
    .then((k) => k.fingerprint)
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
  // if (this._rawTxStream) {
  //   this._rawTxStream.close() // custom close method
  // }

  for (var name in this._streams) {
    this._streams[name].destroy()
  }

  var tasks = [
    self.keeper.destroy(),
    this.messenger.destroy(),
    Q.ninvoke(self.addressBook, 'close'),
    Q.ninvoke(self.msgDB, 'close'),
    Q.ninvoke(self.txDB, 'close'),
    Q.ninvoke(self._log, 'close')
  ]

  if (this.httpClient) {
    tasks.push(this.httpClient.destroy())
  }

  if (this.httpServer) {
    tasks.push(this.httpServer.destroy())
  }

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
  var args = [].slice.call(arguments)
  args.unshift(this.name())
  return debug.apply(null, args)
}

Driver.prototype.options = function () {
  return clone(this._options)
}

Driver.prototype.history = function (identityInfo) {
  var self = this
  var otherPartyRootHash
  var lookupOtherParty
  if (identityInfo) {
    if (ROOT_HASH in identityInfo) {
      lookupOtherParty = Q(identityInfo)
    } else {
      lookupOtherParty = this.lookupIdentity(identityInfo)
    }
  } else {
    lookupOtherParty = Q()
  }

  // console.log(this.myRootHash())
  return lookupOtherParty
    .then(function (other) {
      var stream = self.messages().createValueStream()
      if (other) {
        otherPartyRootHash = other[ROOT_HASH]
        stream = pump(stream, utils.filterStream(function (data) {
          if (data[TYPE] === constants.TYPES.IDENTITY) return

          return (data.from && data.from[ROOT_HASH] === otherPartyRootHash)
            || (data.to && data.to[ROOT_HASH] === otherPartyRootHash)
        }))
      }

      return Q.nfcall(collect, stream)
    })
    .then(function (msgs) {
      return Q.all(msgs.map(self.lookupObject))
    })
}

Driver.prototype._addresses = function () {
  return [
    this.wallet.addressString,
    constants.IDENTITY_PUBLISH_ADDRESS
  ]
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

function alwaysFalse () {
  return false
}