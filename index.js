var assert = require('assert')
var util = require('util')
var EventEmitter = require('events').EventEmitter
var omit = require('object.omit')
var Q = require('q')
var typeforce = require('typeforce')
var pick = require('object.pick')
var debug = require('debug')('tim')
var reemit = require('re-emitter')
var bitcoin = require('bitcoinjs-lib')
var extend = require('xtend/mutable')
var utils = require('tradle-utils')
var map = require('map-stream')
var collect = require('stream-collector')
var levelErrs = require('level-errors')
var pump = require('pump')
var find = require('array-find')
var ChainedObj = require('chained-obj')
var TxData = require('tradle-tx-data').TxData
var ChainWriter = require('bitjoe-js')
var ChainLoader = require('chainloader')
var Permission = require('tradle-permission')
var Wallet = require('simple-wallet')
var cbstreams = require('cb-streams')
var safe = require('safecb')
var Zlorp = require('zlorp')
var mi = require('midentity')
var Identity = mi.Identity
var kiki = require('kiki')
var toKey = kiki.toKey
var Builder = ChainedObj.Builder
var Parser = ChainedObj.Parser
var lb = require('logbase')
var Entry = lb.Entry
var unchainer = require('./lib/unchainer')
var filter = require('./lib/filterStream')
var getDHTKey = require('./lib/getDHTKey')
var constants = require('tradle-constants')
var EventType = require('./lib/eventType')
var Dir = require('./lib/dir')
var updateChainedObj = require('./lib/updateChainedObj')
var createIdentityDB = require('./lib/identityDB')
var createMsgDB = require('./lib/msgDB')
var createTxDB = require('./lib/txDB')
var toObj = require('./lib/toObj')
var errors = require('./lib/errors')
var currentTime = require('./lib/now')
var TYPE = constants.TYPE
var ROOT_HASH = constants.ROOT_HASH
var PREV_HASH = constants.PREV_HASH
var CUR_HASH = constants.CUR_HASH
var PREFIX = constants.OP_RETURN_PREFIX
var CONFIRMATIONS_BEFORE_CONFIRMED = 1000000 // for now
var MAX_CHAIN_RETRIES = 3
var MAX_RESEND_RETRIES = 10
var CHAIN_WRITE_THROTTLE = 60000
var KEY_PURPOSE = 'messaging'
var MSG_SCHEMA = {
  txData: 'Buffer',
  txType: 'Number',
  encryptedPermission: 'Buffer',
  encryptedData: 'Buffer'
}

// var MessageType = Driver.MessageType = {
//   plaintext: 1 << 1,
//   chained: 1 << 2
// }

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
    dht: 'Object',
    leveldown: 'Function',
    port: 'Number',
    pathPrefix: 'String',
    syncInterval: '?Number',
    chainThrottle: '?Number'
  }, options)

  EventEmitter.call(this)
  utils.bindPrototypeFunctions(this)
  extend(this, options)

  this.otrKey = toKey(
    this.getPrivateKey({
      type: 'dsa',
      purpose: 'sign'
    })
  )

  this.signingKey = toKey(
    this.getPrivateKey({
      type: 'ec',
      purpose: 'sign'
    })
  )

  // copy
  this.identityJSON = options.identity.toJSON()
  this.identityMeta = {}
  var networkName = this.networkName
  var keeper = this.keeper
  var dht = this.dht
  var identity = this.identity = Identity.fromJSON(this.identityJSON)
  var blockchain = this.blockchain
  var leveldown = this.leveldown
  var wallet = this.wallet = this.wallet || new Wallet({
    networkName: networkName,
    blockchain: blockchain,
    priv: this.getBlockchainKey().priv
  })

  this.p2p = new Zlorp({
    name: identity.name(),
    available: true,
    leveldown: leveldown,
    port: this.port,
    dht: dht,
    key: this.otrKey.priv()
  })

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
  this._fingerprintToIdentity = {}
  this._pendingTxs = []
  this._setupP2P()

  this._setupDBs()
  var keeperReadyDfd = Q.defer()
  var keeperReady = keeperReadyDfd.promise
  if (!keeper.isReady || keeper.isReady()) keeperReadyDfd.resolve()
  else keeper.once('ready', keeperReadyDfd.resolve)

  Q.all([
      self._prepIdentity(),
      self._setupTxStream(),
      keeperReady
    ])
    .done(function () {
      if (self._destroyed) return

      self._ready = true
      self.msgDB.start()
      self.txDB.start()
      self.addressBook.start()

      self._debug('ready')
      self.emit('ready')
      // self.publishMyIdentity()
      self._writeToChain()
      self._readFromChain()
      self._sendTheUnsent()
      // self._watchMsgStatuses()
    })
}

Driver.prototype.isReady = function () {
  return this._ready
}

Driver.prototype._onLogReadError = function (err) {
  throw err
}

Driver.prototype._onLogWriteError = function (err) {
  throw err
}

Driver.prototype._prepIdentity = function () {
  var self = this

  return Q.nfcall(getDHTKey, this.identityJSON)
    .then(function (key) {
      copyDHTKeys(self.identityMeta, key)
    })
}

Driver.prototype._setupP2P = function () {
  var self = this

  this.p2p.on('data', this._onmessage)
  this.p2p.on('connect', function (fingerprint, addr) {
    self.lookupByFingerprint(fingerprint)
      .then(function (identity) {
        self._debug('connecting to', identity.name.formatted)
        self.emit('connect', identity, addr)
      })
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

  if (this._unchaining) return

  this._unchaining = true

  pump(
    this.txDB.liveStream({
      old: true,
      tail: true
    }),
    toObjectStream(),
    filter(function (entry) {
      var idx = self._pendingTxs.indexOf(entry.txId)
      if (idx !== -1) {
        self._pendingTxs.splice(idx, 1)
      }

      // was read from chain and hasn't been processed yet
      // self._debug('unchaining tx', entry.txId)
      return entry.dateDetected && !entry.dateUnchained
    }),
    this.unchainer,
    map(function (chainedObj, cb) {
      if (chainedObj.parsed) {
        self._debug('unchained (read)', chainedObj.key, chainedObj.errors)
      }

      var entry = self.unchainResultToEntry(chainedObj)
      cb(null, entry)
    }),
    this._log,
    this._rethrow
  )
}

Driver.prototype._rethrow = function (err) {
  if (err) {
    this._debug('experienced an error', err)
    if (!this._destroyed && err) throw err
  }
}

Driver.prototype.identityPublishStatus = function (cb) {
  // check if we've already chained it
  // var self = this
  var rh = this._myRootHash()
  var me = this.identityJSON
  var status = {
    ever: false,
    current: false
  }

  // Q.ninvoke(this.msgDB, 'onLive')
  //   .then(function () {
  //     return Q.ninvoke(self, '_lookupMsgs', toObj(ROOT_HASH, rh))
  //   })
  //   .catch(function (err) {
  //     if (!err.notFound) throw err
  //   })
  //   .then(function (entries) {

  this._lookupMsgs(toObj(ROOT_HASH, rh), function (err, entries) {
    if (err) {
      if (!err.notFound) return cb(err)

      return cb(null, status)
    }

    status.ever = true

    var entry = entries.pop()
    var buf = toBuffer(me)
    utils.getStorageKeyFor(buf, function (err, key) {
      if (err) return cb(err)

      key = key.toString('hex')
      // already queued or already chained
      if (entry.dir === Dir.outbound || entry.tx) {
        if (key === entry[CUR_HASH]) {
          status.current = true
        }
      }

      cb(null, status)
    })
  })
}

Driver.prototype._publishIdentity = function (identity, cb) {
  if (typeof identity === 'function') {
    cb = identity
    identity = this.identityJSON
  }

  cb = safe(cb)
  this.publish({
      msg: toBuffer(identity),
      to: [{ fingerprint: constants.IDENTITY_PUBLISH_ADDRESS }]
    })
    .nodeify(cb)
}

Driver.prototype.publishMyIdentity = function () {
  var self = this
  // var origCB = safe(cb)

  var cb = function (err, result) {
    // origCB(err, result)
    if (err) throw err

    finish()
  }

  if (this._publishingIdentity) {
    this._publishIdentityQueued = true
    return cb()
  }

  delete this._publishIdentityQueued

  this.identityPublishStatus(function (err, status) {
    if (err) return cb(err)
    if (!status.ever) return self._publishIdentity(cb)
    if (status.current) return cb()

    var priv = self.getPrivateKey({ purpose: 'update' })
    var update = extend({}, self.identityJSON)
    var prevHash = self._myCurrentHash() || self._myRootHash()
    updateChainedObj(update, prevHash)

    Builder()
      .data(update)
      .signWith(toKey(priv))
      .build(function (err, result) {
        if (err) return cb(err)

        self._publishIdentity(result.form, cb)

        // TODO: better have this as a response to an event
        // so it can be handled the same at startup and here
        self.identityJSON = update
        extend(self.identityMeta, pick(update, [CUR_HASH, PREV_HASH, ROOT_HASH]))
      })
  })

  function finish () {
    delete self._publishingIdentity
    if (self._publishIdentityQueued) {
      self.publishMyIdentity()
    }
  }
}

Driver.prototype.identities = function () {
  return this.addressBook
}

Driver.prototype.messages = function () {
  return this.msgDB
}

Driver.prototype.decryptedMessagesStream = function (opts) {
  var self = this
  opts = opts || {}
  return this.msgDB.createValueStream()
    .pipe(map(function (info, cb) {
      self.lookupObject(info)
        .nodeify(cb)
    }))
}

Driver.prototype.transactions = function () {
  return this.txDB
}

Driver.prototype.unchainResultToEntry = function (chainedObj) {
  var type = chainedObj.errors && chainedObj.errors.length ?
    EventType.chain.readError :
    EventType.chain.readSuccess

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

  var from = chainedObj.from && chainedObj.from.getOriginalJSON()
  if (from) {
    entry.set('from', from[ROOT_HASH])
  }

  var to = chainedObj.to && chainedObj.to.getOriginalJSON()
  if (to) {
    entry.set('to', toObj(ROOT_HASH, to[ROOT_HASH]))
  }

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
    entry.set('tx', toBuffer(chainedObj.tx))
  }

  if ('id' in chainedObj) {
    entry.prev(chainedObj.id)
  }

  return entry
}

Driver.prototype._getToChainStream = function () {
  return pump(
    this.msgDB.liveStream({
      old: true,
      tail: true
    }),
    toObjectStream(),
    filter(function (entry) {
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
  var throttle = this.chainThrottle || CHAIN_WRITE_THROTTLE

  pump(
    this._getToChainStream(),
    map(function (entry, cb) {
      var errors = entry.errors
      if (!errors || !errors.length) {
        return tryAgain()
      }

      var lastErr = errors[errors.length - 1]
      var now = currentTime()
      var wait = lastErr.timestamp + throttle - now
      if (wait < 0) {
        return tryAgain()
      }

      self._debug('throttling chaining', wait)
      // just in case the device clock time-traveled
      wait = Math.min(wait, throttle)
      setTimeout(tryAgain, wait)

      function tryAgain () {
        self.putOnChain(new Entry(entry))
          .done(function (nextEntry) {
            cb(null, nextEntry)
          })
      }
    }),
    // filter(function (data) {
    //   console.log('after chain write', data.toJSON())
    //   return true
    // }),
    this._log,
    this._rethrow
  )
}

Driver.prototype._getUnsentStream = function () {
  var self = this
  return pump(
    this.msgDB.liveStream({
      tail: true,
      old: true
    }),
    toObjectStream(),
    filter(function (entry) {
      if (entry.dateSent ||
          entry.txType === TxData.types.public ||
          !entry.to ||
          !entry.deliver ||
          entry.dir !== Dir.outbound ||
          self._currentlySending.indexOf(entry.id) !== -1) {
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
  return this.identityJSON.name.formatted
}

Driver.prototype._sendTheUnsent = function () {
  var self = this
  if (this._destroyed) return

  var db = this.msgDB

  if (!db.isLive()) return db.once('live', this._sendTheUnsent)

  if (this._sending) return

  this._sending = true
  this._currentlySending = []
  this.msgDB.on('sent', function (entry) {
    var id = entry.prev[0]
    var idx = self._currentlySending.indexOf(id)
    self._currentlySending.splice(idx, 1)
  })

  pump(
    this._getUnsentStream(),
    map(function (entry, cb) {
      self._currentlySending.push(entry.id)
      self._sendP2P(entry)
        .then(function () {
          return new Entry({
            type: EventType.msg.sendSuccess
          })
        })
        .catch(function (err) {
          var errEntry = new Entry({
            type: EventType.msg.sendError,
            errors: entry.errors || []
          })

          addError(errEntry, err)
          return errEntry
        })
        .done(function (nextEntry) {
          var prev = entry.prev.concat(entry.id)
          nextEntry.prev(prev)
          cb(null, nextEntry)
        })
    }),
    // filter(function (data) {
    //   console.log('after sendTheUnsent', data.toJSON())
    //   return true
    // }),
    this._log,
    this._rethrow
  )
}

Driver.prototype._setupTxStream = function () {
  // TODO: use txDB for this instead
  var self = this
  var defer = Q.defer()
  var lastBlock
  var lastBlockTxIds = []
  var chainTypes = EventType.chain
  var rs = this._log.createReadStream({ reverse: true })
    .pipe(filter(function (entry, cb) {
      var eType = entry.get('type')
      return eType === chainTypes.readSuccess || eType === chainTypes.readError
    }))
    .on('data', function (entry) {
      var txId = bitcoin.Transaction.fromBuffer(entry.get('tx')).getId()
      lastBlockTxIds.unshift(txId)
      if (typeof lastBlock === 'undefined') {
        lastBlock = entry.get('height')
      } else {
        if (entry.get('height') < lastBlock) {
          rs.destroy()
        }
      }
    })
    .on('error', this._rethrow)
    .once('close', function () {
      // start CONFIRMATIONS_BEFORE_CONFIRMED blocks back
      lastBlock = lastBlock || 0
      lastBlock = Math.max(0, lastBlock - CONFIRMATIONS_BEFORE_CONFIRMED)
      self._streamTxs(lastBlock, lastBlockTxIds)
      defer.resolve()
    })

  return defer.promise
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
      var id = txInfo.tx.getId()
      if (self._pendingTxs.indexOf(id) !== -1) {
        return cb() // already handled this one
      }

      self.txDB.get(id, function (err, entry) {
        if (err && !err.notFound) return cb(err)

        save(entry)
      })

      function save (entry) {
        self._pendingTxs.push(id)
        var type = entry ?
          EventType.tx.confirmation :
          EventType.tx.new
          // we may have chained this tx
          // but this is the first time we're
          // getting it FROM the chain

        var nextEntry = new Entry(extend(entry || {}, txInfo, {
          type: type,
          txId: id,
          tx: toBuffer(txInfo.tx),
          dir: self._getTxDir(txInfo.tx)
        }))

        if (entry) nextEntry.prev(new Entry(entry))
        cb(null, nextEntry)
      }
    }),
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
    autostart: false
  })

  var readWrite = this.addressBook
  var readOnly = this._readOnlyAddressBook = {}
  ;[
    'get',
    'createReadStream',
    'createKeyStream',
    'createValueStream',
    'query',
    'liveStream',
    'byFingerprint',
    'byRootHash'
  ].forEach(function (method) {
    if (readWrite[method]) {
      readOnly[method] = readWrite[method].bind(readWrite)
    }
  })

  this.msgDB = createMsgDB(this._prefix('messages.db'), {
    leveldown: this.leveldown,
    log: this._log,
    autostart: false
  })

  this.msgDB.name = this.identityJSON.name.formatted + ' msgDB'

  var msgDBEvents = [
    'chained',
    'unchained',
    'message',
    'sent'
  ]

  reemit(this.msgDB, this, msgDBEvents)
  this.msgDB.on('live', function () {
    self._debug('msgDB is live')
  })
  // msgDBEvents.forEach(function (e) {
  //   self.msgDB.on(e, function (entry) {
  //     self._debug(e, entry)
  //   })
  // })

  ;['message', 'unchained'].forEach(function (event) {
    self.msgDB.on(event, function (entry) {
      if (entry.tx && entry.dateReceived) {
        self.emit('resolved', entry)
      }
    })
  })

  // announce that own identity got published
  // this.msgDB.on('unchained', function (entry) {
  //   if (entry.txType === TxData.types.public) {
  //     if ()
  //     self.identityKeys.map(function (k) {
  //       return k.fingerprint
  //     }).some(function )
  //     if (entry.from && entry.from[ROOT_HASH] === self._myRootHash()) {
  //     }
  //   }
  // })

  this.txDB = createTxDB(this._prefix('txs.db'), {
    leveldown: this.leveldown,
    log: this._log,
    autostart: false
  })

  this.txDB.name = this.identityJSON.name.formatted + ' txDB'
}

Driver.prototype._sendP2P = function (entry) {
  // TODO:
  //   do we log that we sent it?
  //   do we log when we delivered it? How do we know it was delivered?
  var self = this
  return Q.all([
      this.lookupIdentity(entry.to),
      this.lookupObject(entry)
    ])
    .spread(function (identity, chainedObj) {
      var fingerprint = getFingerprint(identity)
      self._debug(KEY_PURPOSE, fingerprint)
      var msg = msgToBuffer(getMsgProps(chainedObj))
      return Q.ninvoke(self.p2p, 'send', msg, fingerprint)
    })
}

Driver.prototype.lookupObject = function (info) {
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
}

Driver.prototype.lookupByFingerprint = function (fingerprint) {
//   var cached = this._fingerprintToIdentity[fingerprint]
//   if (cached) return Q.resolve(cached)

  return this.lookupIdentity({
    fingerprint: fingerprint
  })
//     .then(function (identity) {
//       identity.pubkeys.forEach(function (p) {
//         self._fingerprintToIdentity[p.fingerprint] = identity
//       })

//       return identity
    // })
}

Driver.prototype.getKeyAndIdentity = function (fingerprint, returnPrivate) {
  var self = this
  return this.lookupByFingerprint(fingerprint)
    .then(function (identity) {
      var key = returnPrivate && self.getPrivateKey(fingerprint)
      key = key || keyForFingerprint(identity, fingerprint)
      return {
        key: key,
        identity: identity
      }
    })
}

Driver.prototype.getKeyAndIdentity2 = function (fingerprint, returnPrivate) {
  return this.getKeyAndIdentity.apply(this, arguments)
    .then(function (result) {
      result.identity = Identity.fromJSON(result.identity)
      return result
    })
}

Driver.prototype._lookupMsgs = function (query, cb) {
  var self = this
  this.msgDB.onLive(function () {
    collect(self.msgDB.query(query), function (err, results) {
      if (err) return cb(err)
      if (!results.length) {
        return cb(new levelErrs.NotFoundError())
      }

  //     if (results.length !== 1) {
  //       return self.emit('error',
  //         new Error('invalid state: should have single result for query'))
  //     }

      return cb(null, results)
    })
  })
}

Driver.prototype.lookupByDHTKey = function (key, cb) {
  var self = this
  this._lookupMsgs(toObj(CUR_HASH, key), function (err, entries) {
    if (err) return cb(err)

    self.lookupObject(entries.pop())
      .nodeify(cb)
  })
}

Driver.prototype.getPublicKey = function (fingerprint, identity) {
  identity = identity || this.identityJSON
  return find(identity.pubkeys, function (k) {
    return k.fingerprint === fingerprint
  })
}

Driver.prototype.getPrivateKey = function (where) {
  return firstKey(this.identityKeys, where)
}

Driver.prototype.getBlockchainKey = function () {
  return this.getPrivateKey({
    networkName: this.networkName,
    type: 'bitcoin',
    purpose: KEY_PURPOSE
  })
}

Driver.prototype.lookupIdentity = function (query) {
  var me = this.identityJSON
  var valid = !!query.fingerprint ^ !!query[ROOT_HASH]
  if (!valid) {
    return Q.reject(new Error('query by "fingerprint" OR "' + ROOT_HASH + '" (root hash)'))
  }

  if (query[ROOT_HASH]) {
    if (query[ROOT_HASH] === this._myRootHash()) {
      return Q.resolve(me)
    } else {
      return Q.ninvoke(this.addressBook, 'byRootHash', query[ROOT_HASH])
    }
  } else if (query.fingerprint) {
    var pub = this.getPublicKey(query.fingerprint)
    if (pub) {
      return Q.resolve(me)
    } else {
      return Q.ninvoke(this.addressBook, 'byFingerprint', query.fingerprint)
    }
  }
}

Driver.prototype.log = function (entry) {
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

Driver.prototype._onmessage = function (buf, fingerprint) {
  var self = this
  var msg

  try {
    msg = bufferToMsg(buf)
  } catch (err) {
    return this.emit('warn', 'received message not in JSON format', buf)
  }

  this._debug('received msg', msg)

  // this thing repeats work all over the place
  var txInfo
  var valid = validateMsg(msg)
  Q[valid ? 'resolve' : 'reject']()
    .then(this.lookupIdentity.bind(this, { fingerprint: fingerprint }))
    .then(function (identity) {
      var fromKey = firstKey(identity.pubkeys, {
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
    .then(function (chainedObj) {
      chainedObj.from = chainedObj.from && chainedObj.from.identity
      chainedObj.to = chainedObj.to && chainedObj.to.identity
      return self.unchainResultToEntry(chainedObj)
        .set({
          type: EventType.msg.receivedValid,
          dir: Dir.inbound
        })
    })
    .catch(function (err) {
      self._debug('failed to process inbound msg', err)
      return new Entry({
        type: EventType.msg.receivedInvalid,
        msg: msg,
        from: {
          fingerprint: fingerprint
        },
        to: [toObj(ROOT_HASH, self._myRootHash())],
        dir: Dir.inbound,
        errors: [err]
      })
    })
    .done(function (entry) {
      return self._log.append(entry)
    })
}

Driver.prototype._myRootHash = function () {
  return this.identityMeta[ROOT_HASH]
}

Driver.prototype._myCurrentHash = function () {
  return this.identityMeta[CUR_HASH]
}

// Driver.prototype._myPrevHash = function () {
//   return this.identityMeta[PREV_HASH]
// }

// TODO: enforce order
Driver.prototype.putOnChain = function (entry) {
  var self = this
  assert(entry.get(ROOT_HASH) && entry.get(CUR_HASH), 'missing required fields')

  var type = entry.get('txType')
  var data = entry.get('txData')
  var nextEntry = new Entry()
    .prev(entry)
    .set(ROOT_HASH, entry.get(ROOT_HASH))
    .set(CUR_HASH, entry.get(CUR_HASH))
    .set({
      txType: type,
      chain: true,
      dir: Dir.outbound
    })

//   return this.lookupBTCAddress(to)
//     .then(shareWith)
  var addr = entry.get('addressesTo')[0]
  return self.chainwriter.chain()
    .type(type)
    .data(data)
    .address(addr)
    .execute()
    .then(function (tx) {
      // ugly!
      nextEntry
        .set({
          type: EventType.chain.writeSuccess,
          tx: toBuffer(tx),
          txId: tx.getId()
        })

      self._debug('chained (write)', nextEntry.get(CUR_HASH), 'tx: ' + nextEntry.get('txId'))
      return nextEntry
    })
    .catch(function (err) {
      err = toErrInstance(err)
      err = errors.ChainWriteError(err, {
        timestamp: currentTime()
      })

      self._debug('chaining failed', err)
      self.emit('error', err)

      var errEntry = new Entry({
          type: EventType.chain.writeError
        })
        .prev(entry)

      addError(errEntry, err)
      return errEntry
    })
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
    from: toObj(ROOT_HASH, this._myRootHash()),
    to: to
  })

  var recipients
  return Q.all(to.map(this.lookupBTCPubKey))
    .then(function (_recipients) {
      recipients = _recipients
      return Q.ninvoke(self, '_lookupMsgs', toObj(CUR_HASH, curHash))
    })
    .then(function (results) {
      return self.lookupObject(results[0])
    })
    .then(function (obj) {
      entry.set(CUR_HASH, curHash)
        .set(ROOT_HASH, obj[ROOT_HASH])

      var symmetricKey = obj.permission.body().decryptionKey
      return Q.all(recipients.map(function (r) {
        return self.chainwriter.share()
          .shareAccessTo(curHash, symmetricKey)
          .shareAccessWith(r)
          .execute()
      }))
    })
    .then(function (shares) {
      // TODO: rethink this repeated code from send()
      var entries = shares.map(function (share, i) {
        return entry.clone().set({
          to: to[i],
          addressesTo: [share.address],
          addressesFrom: [self.wallet.addressString],
          txType: TxData.types.permission,
          txData: toBuffer(share.encryptedKey, 'hex')
        })
      })

      return Q.all(entries.map(self.log, self))
    })
}

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

  var data = toBuffer(options.msg)
  // assert(TYPE in data, 'structured messages must specify type property: ' + TYPE)

  // either "public" or it has recipients
  var isPublic = !!options.public
  // assert(isPublic ^ !!options.to, 'private msgs must have recipients, public msgs cannot')

  var to = options.to
  if (!to && isPublic) {
    var me = toObj(ROOT_HASH, this._myRootHash())
    to = [me]
  }

  validateRecipients(to)

  var entry = new Entry({
    type: EventType.msg.new,
    dir: Dir.outbound,
    public: isPublic,
    chain: !!options.chain,
    deliver: !!options.deliver,
    from: toObj(ROOT_HASH, this._myRootHash()),
    to: to
  })

  var recipients
  var lookup = isPublic ? this.lookupBTCAddress : this.lookupBTCPubKey
  var validate = options.chain ? Q.ninvoke(Parser, 'parse', data) : Q.resolve()

  return validate
    .then(function () {
      return Q.all(to.map(lookup))
    })
    .catch(function (err) {
      if (isPublic) {
        return to.map(function (recipient) {
          return recipient.fingerprint
        })
      } else {
        throw err
      }
    })
    .then(function (_recipients) {
      recipients = _recipients
      return self.chainwriter.create()
        .data(data)
        .setPublic(isPublic)
        .recipients(recipients)
        .execute()
    })
    .then(function (resp) {
      copyDHTKeys(entry, resp.key)
      self._debug('stored (write)', entry.get(ROOT_HASH))
      if (isPublic && self.keeper.push) {
        self.keeper.push(resp)
      }

      var entries
      if (isPublic) {
        entries = to.map(function (contact, i) {
          return entry.clone().set({
            to: contact,
            addressesFrom: [self.wallet.addressString],
            addressesTo: [recipients[i]],
            txType: TxData.types.public,
            txData: toBuffer(resp.key, 'hex')
          })
        })
      } else {
        entries = resp.shares.map(function (share, i) {
          return entry.clone().set({
            to: to[i],
            addressesTo: [share.address],
            addressesFrom: [self.wallet.addressString],
            txType: TxData.types.permission,
            txData: toBuffer(share.encryptedKey, 'hex')
          })
        })
      }

      return Q.all(entries.map(self.log, self))
    })
}

Driver.prototype.lookupBTCKey = function (recipient) {
  var self = this
  return this.lookupIdentity(recipient)
    .then(function (identity) {
      return firstKey(identity.pubkeys, {
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

  // async
  return Q.all([
      self.keeper.destroy(),
      Q.ninvoke(self.p2p, 'destroy'),
      Q.ninvoke(self.addressBook, 'close'),
      Q.ninvoke(self.msgDB, 'close'),
      Q.ninvoke(self.txDB, 'close'),
      Q.ninvoke(self._log, 'close')
    ])
// .done(console.log.bind(console, this.pathPrefix + ' is dead'))
}

Driver.prototype._debug = function () {
  var args = [].slice.call(arguments)
  args.unshift(this.identityJSON.name.formatted)
  return debug.apply(null, args)
}

Driver.prototype._getTxDir = function (tx) {
  var self = this
  var isOutbound = tx.ins.some(function (input) {
    var addr = utils.getAddressFromInput(input, self.networkName)
    return addr === self.wallet.addressString
  })

  return isOutbound ? Dir.outbound : Dir.inbound
}

function validateMsg (msg) {
  try {
    typeforce(MSG_SCHEMA, msg)
    return true
  } catch (err) {
    return false
  }
}

function getMsgProps (info) {
  return {
    txData: info.encryptedKey,
    txType: info.txType, // no need to send this really
    encryptedPermission: info.encryptedPermission,
    encryptedData: info.encryptedData
  }
}

function msgToBuffer (msg) {
  if (!validateMsg(msg)) throw new Error('invalid msg')

  msg = extend({}, msg)
  for (var p in MSG_SCHEMA) {
    var type = MSG_SCHEMA[p]
    if (type === 'Buffer') {
      msg[p] = msg[p].toString('base64')
    }
  }

  return toBuffer(msg, 'binary')
}

function bufferToMsg (buf) {
  var msg = JSON.parse(buf.toString('binary'))
  for (var p in MSG_SCHEMA) {
    var type = MSG_SCHEMA[p]
    if (type === 'Buffer') {
      msg[p] = new Buffer(msg[p], 'base64')
    }
  }

  return msg
}

function toBuffer (data, enc) {
  if (typeof data.toBuffer === 'function') return data.toBuffer()
  if (Buffer.isBuffer(data)) return data
  if (typeof data === 'object') data = utils.stringify(data)

  return new Buffer(data, enc || 'binary')
}

function getFingerprint (identity) {
  return find(identity.pubkeys, function (k) {
    return k.type === 'dsa'
  }).fingerprint
}

function keyForFingerprint (identityJSON, fingerprint) {
  return find(identityJSON.pubkeys, function (k) {
    return k.fingerprint === fingerprint
  })
}

function copyDHTKeys (dest, src, curHash) {
  if (typeof curHash === 'undefined') {
    if (typeof src === 'string') {
      curHash = src
    } else {
      curHash = getEntryProp(src, CUR_HASH) || getEntryProp(src, ROOT_HASH)
    }

    src = dest
  }

  var rh = getEntryProp(src, ROOT_HASH)
  var ph = getEntryProp(src, PREV_HASH)
  setEntryProp(dest, ROOT_HASH, rh || curHash)
  setEntryProp(dest, PREV_HASH, ph)
  setEntryProp(dest, CUR_HASH, curHash)
}

function getEntryProp (obj, name) {
  return obj instanceof Entry ? obj.get(name) : obj[name]
}

function setEntryProp (obj, name, val) {
  if (obj instanceof Entry) obj.set(name, val)
  else obj[name] = val
}

function validateRecipients (recipients) {
  if (!Array.isArray(recipients)) recipients = [recipients]

  recipients.every(function (r) {
    assert(r.fingerprint || r.pubKey || r[ROOT_HASH],
      '"recipient" must specify "fingerprint", "pubKey" or identity.' + ROOT_HASH +
      ' (root hash of recipient identity)')
  })
}

function toErrJSON (err) {
  var json = {}

  Object.getOwnPropertyNames(err).forEach(function (key) {
    json[key] = err[key]
  })

  delete json.stack
  return json
}

function addError (entry, err) {
  var errs = entry.get('errors') || []
  errs.push(toErrJSON(err))
  entry.set('errors', errs)
}

function firstKey (keys, where) {
  if (typeof where === 'string') where = { fingerprint: where }

  return find(keys, function (k) {
    for (var p in where) {
      if (where[p] !== k[p]) return
    }

    return true
  })
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

function toErrInstance (err) {
  var n = new Error(err.message)
  for (var p in err) {
    n[p] = err[p]
  }

  return n
}
