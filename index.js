var assert = require('assert')
var util = require('util')
var EventEmitter = require('events').EventEmitter
var collect = require('stream-collector')
var omit = require('object.omit')
var pick = require('object.pick')
var Q = require('q')
var typeforce = require('typeforce')
var debug = require('debug')('tim')
var reemit = require('re-emitter')
var bitcoin = require('bitcoinjs-lib')
var extend = require('extend')
var utils = require('tradle-utils')
var map = require('map-stream')
var pump = require('pump')
var typeforce = require('typeforce')
var find = require('array-find')
var ChainedObj = require('chained-obj')
var TxData = require('tradle-tx-data').TxData
var ChainWriter = require('bitjoe-js')
var ChainLoader = require('chainloader')
var Permission = require('tradle-permission')
var Wallet = require('simple-wallet')
var cbstreams = require('cb-streams')
var Zlorp = require('zlorp')
var mi = require('midentity')
var Identity = mi.Identity
var toKey = mi.toKey
var Parser = ChainedObj.Parser
var lb = require('logbase')
var rebuf = lb.rebuf
var Entry = lb.Entry
var unchainer = require('./unchainer')
var filter = require('./filterStream')
var getDHTKey = require('./getDHTKey')
var constants = require('tradle-constants')
var EventType = require('./eventType')
var Dir = require('./dir')
var createIdentityDB = require('./identityDB')
var createMsgDB = require('./msgDB')
var createTxDB = require('./txDB')
var toObj = require('./toObj')
var TYPE = constants.TYPE
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH
var PREFIX = constants.OP_RETURN_PREFIX
var MAX_CHAIN_RETRIES = 3
var MAX_RESEND_RETRIES = 10
var ArrayProto = Array.prototype
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
    identityKeys: 'Array', // maybe allow read-only mode if this is missing
    identityJSON: 'Object',
    blockchain: 'Object',
    networkName: 'String',
    keeper: 'Object',
    dht: 'Object',
    leveldown: 'Function',
    port: 'Number',
    pathPrefix: 'String',
    syncInterval: 'Number'
  }, options)

  EventEmitter.call(this)
  utils.bindPrototypeFunctions(this)
  extend(this, options)

  this.otrKey = toKey(
    find(this.identityKeys, function (k) {
      return k.type === 'dsa' && k.purpose === 'sign'
    })
  )

  this.signingKey = toKey(
    find(this.identityKeys, function (k) {
      return k.type === 'ec' && k.purpose === 'sign'
    })
  )

  // copy
  this.identityJSON = extend(true, {}, this.identityJSON)
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
  this._setupP2P()

  this._setupDBs()
  Q.all([
      self._prepIdentity(),
      self._setupTxStream()
    ])
    .done(function () {
      self._ready = true
      self.emit('ready')
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

  if (ROOT_HASH in this.identityJSON && CUR_HASH in this.identityJSON) {
    return Q()
  }

  return Q.nfcall(getDHTKey, this.identityJSON)
    .then(function (key) {
      copyDHTKeys(self.identityJSON, key)
      self._myRootHash = self.identityJSON[ROOT_HASH]
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

  if (!this.txDB.isLive()) {
    return this.txDB.once('live', this._readFromChain)
  }

  if (this._unchaining) return

  this._unchaining = true

  pump(
    this._getFromChainStream(),
    this.unchainer,
    map(function (chainedObj, cb) {
      self._debug('unchained (read)', chainedObj.key)
      // var query = {}
      // query[CUR_HASH] = chainedObj.key
      // self.msgDB.query(query)
      var type = chainedObj.errors && chainedObj.errors.length ?
        EventType.chain.readError :
        EventType.chain.readSuccess

      var entry = self.chainedObjToEntry(chainedObj)
        .set('type', type)

      cb(null, entry)
    }),
    // filter(function (data) {
    //   console.log('after chain read')
    //   return true
    // }),
    this._log,
    rethrow
  )
}

Driver.prototype.chainedObjToEntry = function (chainedObj) {
  var from = chainedObj.from.getOriginalJSON()
  var to = chainedObj.to && chainedObj.to.getOriginalJSON()

  var entry = new Entry(omit(chainedObj, ['parsed', 'key', 'data', 'type'])) // data is stored in keeper
    .set(CUR_HASH, chainedObj.key)
    .set(ROOT_HASH, chainedObj.parsed.data[ROOT_HASH] || chainedObj.key)
    .set(TYPE, chainedObj.parsed.data[TYPE])
    .set({
      public: chainedObj.type === TxData.types.public,
      from: from[ROOT_HASH],
      to: to && toObj(ROOT_HASH, to[ROOT_HASH])
    })

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
             !entry.chained &&
              entry.dir === Dir.outbound &&
              (!entry.errors || entry.errors.length < MAX_CHAIN_RETRIES)
    }),
    rethrow
  )
}

/**
 * write to chain
 */
Driver.prototype._writeToChain = function () {
  var self = this
  var db = this.msgDB

  if (!db.isLive()) return db.once('live', this._writeToChain)
  if (this._chaining) return

  this._chaining = true

  pump(
    this._getToChainStream(),
    map(function (entry, cb) {
      self.putOnChain(new Entry(entry))
        .done(function (nextEntry) {
          cb(null, nextEntry)
        })
    }),
    // filter(function (data) {
    //   console.log('after chain write', data.toJSON())
    //   return true
    // }),
    this._log,
    rethrow
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
      if (entry.sent ||
          entry.txType === TxData.types.public ||
          !entry.to ||
          entry.dir !== Dir.outbound ||
          self._currentlySending.indexOf(entry.id) !== -1) return

      if (entry.errors && entry.errors.length > MAX_RESEND_RETRIES) {
        self._debug('giving up on sending message', entry)
        return
      }

      return true
    }),
    rethrow
  )
}

Driver.prototype._getFromChainStream = function () {
  return pump(
    this.txDB.liveStream({
      old: true,
      tail: true
    }),
    toObjectStream(),
    filter(function (entry) {
      if (entry[CUR_HASH]) return
      if (entry.errors && entry.errors.length) return

      return true
    }),
    rethrow
  )
}

// Driver.prototype._watchMsgStatuses = function () {
//   var self = this

//   if (this._watching) return

//   this._watching = true

//   pump(
//     this.msgDB.liveStream({
//       old: false, // only new
//       tail: true
//     }),
//     toObjectStream(),
//     map(function (entry, cb) {
//       if (entry.received) {
//         if (entry[CUR_HASH]) { // actually loaded
//           self.emit('received', entry)
//           if (entry.chained) {

//           }
//         }
//       }

//       self.emit('received', entry)
//       if (entry.get('chained')) {
//         self.emit('resolved', entry)
//       }
//     }),
//     rethrow
//   )
// }

Driver.prototype._sendTheUnsent = function () {
  var self = this
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
    filter(function (data) {
      console.log('after sendTheUnsent', data.toJSON())
      return true
    }),
    this._log,
    rethrow
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
    .on('error', rethrow)
    .once('close', function () {
      self._streamTxs(lastBlock, lastBlockTxIds)
      defer.resolve()
    })

  return defer.promise
}

Driver.prototype._streamTxs = function (fromHeight, skipIds) {
  var self = this
  if (!fromHeight) fromHeight = 0

  this._rawTxStream = cbstreams.stream.txs({
    live: true,
    interval: this.syncInterval || 60000,
    api: this.blockchain,
    height: fromHeight,
    addresses: [
      this.wallet.addressString,
      constants.IDENTITY_PUBLISH_ADDRESS
    ]
  })

  pump(
    this._rawTxStream,
    map(function (txInfo, cb) {
      var id = txInfo.tx.getId()
      if (txInfo.height < fromHeight ||
        (txInfo.height === fromHeight && skipIds.indexOf(id) !== -1)) {
        return cb()
      }

      var props = extend({}, txInfo, {
        type: EventType.tx,
        tx: txInfo.tx.toBuffer(),
        txId: id,
        dir: self._getTxDir(txInfo.tx)
      })

      cb(null, new Entry(props))
    }),
    this._log,
    rethrow
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
    keeper: this.keeper
  })

  this.msgDB = createMsgDB(this._prefix('messages.db'), {
    leveldown: this.leveldown,
    log: this._log
  })

  this.msgDB.name = this.identityJSON.name.formatted + ' msgDB'

  reemit(this.msgDB, this, [
    'chained',
    'unchained'
  ])

  this.msgDB.on('received', function (msg) {
    self.emit('message', msg)
  })

  ;['received', 'unchained'].forEach(function (event) {
    self.msgDB.on(event, function (entry) {
      if (entry.tx && entry.received) {
        self.emit('resolved', entry)
      }
    })
  })

  this.txDB = createTxDB(this._prefix('txs.db'), {
    leveldown: this.leveldown,
    log: this._log
  })

  this.txDB.name = this.identityJSON.name.formatted + ' txDB'
}

Driver.prototype._sendP2P = function (info) {
  // TODO:
  //   do we log that we sent it?
  //   do we log when we delivered it? How do we know it was delivered?
  var self = this

  return Q.all([
      this.lookupIdentity(info.to),
      this.lookupChainedObj(info)
    ])
    .spread(function (identity, chainedObj) {
      var fingerprint = getFingerprint(identity)
      self._debug('messaging', fingerprint)
      var msg = msgToBuffer(getMsgProps(chainedObj))
      self.p2p.send(msg, fingerprint)
    })
}

Driver.prototype.lookupChainedObj = function (info) {
  // this duplicates part of unchainer.js
  if (!info.txData) {
    if (info.tx) {
      info = bitcoin.Transaction.fromBuffer(info.tx)
    } else {
      throw new Error('missing required info to lookup chained obj')
    }
  }

  var chainedObj
  return this.chainloader.load(info)
    .then(function (objs) {
      chainedObj = objs[0]
      return Q.ninvoke(Parser, 'parse', chainedObj.data)
    })
    .then(function (parsed) {
      chainedObj.parsed = parsed
      return chainedObj
    })
}

Driver.prototype.lookupByFingerprint = function (fingerprint) {
  var self = this

  var cached = this._fingerprintToIdentity[fingerprint]
  if (cached) return Q.resolve(cached)

  return this.lookupIdentity({
      fingerprint: fingerprint
    })
    .then(function (identity) {
      identity.pubkeys.forEach(function (p) {
        self._fingerprintToIdentity[p.fingerprint] = identity
      })

      return identity
    })
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

Driver.prototype.lookupByDHTKey = function (key, cb) {
  var defer = Q.defer()
  this._db.query({ key: key })
    .once('data', defer.resolve)
    .once('end', defer.reject.bind(defer, new Error('not found')))
    .once('error', defer.reject)

  return defer.promise
}

Driver.prototype.getPublicKey = function (fingerprint, identity) {
  identity = identity || this.identityJSON
  return find(identity.pubkeys, function (k) {
    return k.fingerprint === fingerprint
  })
}

Driver.prototype.getPrivateKey = function (where) {
  if (typeof where === 'string') where = { fingerprint: where }

  return find(this.identityKeys, function (k) {
    for (var p in where) {
      if (where[p] !== k[p]) return
    }

    return true
  })
}

Driver.prototype.getBlockchainKey = function () {
  return this.getPrivateKey({
    networkName: this.networkName,
    type: 'bitcoin',
    purpose: 'messaging'
  })
}

Driver.prototype.lookupIdentity = function (query) {
  var me = this.identityJSON
  var valid = !!query.fingerprint ^ !!query[ROOT_HASH]
  if (!valid) {
    return Q.reject(new Error('query by "fingerprint" OR "' + ROOT_HASH + '" (root hash)'))
  }

  if (query[ROOT_HASH]) {
    if (query[ROOT_HASH] === this._myRootHash) {
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

  if (!validateMsg(msg)) {
    var badMsgEntry = new Entry({
      type: EventType.msg.receivedInvalid,
      msg: msg,
      from: {
        fingerprint: fingerprint
      },
      to: [toObj(ROOT_HASH, this._myRootHash)],
      dir: Dir.inbound
    })

    return this.log(badMsgEntry)
  }

  // this thing repeats work all over the place
  var txInfo
  this.lookupIdentity({ fingerprint: fingerprint })
    .then(function (identity) {
      var fromKey = find(identity.pubkeys, function (k) {
        return k.type === 'bitcoin' && k.purpose === 'messaging'
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
      return self.lookupChainedObj(txInfo)
    })
    .then(function (chainedObj) {
      chainedObj.from = chainedObj.from && chainedObj.from.identity
      chainedObj.to = chainedObj.to && chainedObj.to.identity
      var entry = self.chainedObjToEntry(chainedObj)
        .set({
          type: EventType.msg.receivedValid,
          dir: Dir.inbound
        })

      return self.log(entry)
    })
    .catch(function (err) {
      self._debug('failed to find identity by fingerprint', fingerprint, err)
      debugger
    })
    // .finally(function () {
    //   entry.set('from', from)
    //   return self.log(entry)
    // })
    .done()
}

Driver.prototype.putOnChain = function (entry) {
  var self = this
  assert(entry.get(ROOT_HASH) && entry.get(CUR_HASH), 'missing required fields')

  var curHash = entry.get(CUR_HASH)
  var isPublic = entry.get('public')
  var type = entry.get('txType')
  var data = entry.get('txData')
  var to = entry.get('to')
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
          tx: tx.toBuffer(),
          txId: tx.getId()
        })

      self._debug('chained (write)', nextEntry.get(CUR_HASH), 'tx: ' + nextEntry.get('txId'))
      return nextEntry
    })
    .catch(function (err) {
      self._debug('chaining failed', err)
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

/**
 * send an object (and optionally chain it)
 * @param {Object} options
 * @param {Buffer} options.msg - message to send (to be chainable, it should pass Parser.parse())
 * @param {Array} options.to (optional) - recipients
 * @param {Boolean} options.public (optional) - whether this message should be publicly visible
 * @param {Boolean} options.chain (optional) - whether to put this message on chain
 */
Driver.prototype.send = function (options) {
  var self = this

  typeforce({
    msg: 'Buffer',
    to: 'Array',
    public: '?Boolean',
    chain: '?Boolean'
  }, options)

  var data = options.msg
  // assert(TYPE in data, 'structured messages must specify type property: ' + TYPE)

  // either "public" or it has recipients
  var isPublic = !!options.public
  // assert(isPublic ^ !!options.to, 'private msgs must have recipients, public msgs cannot')

  var type = data[TYPE]
  var to = options.to
  if (!to && isPublic) {
    var me = toObj(ROOT_HASH, this._myRootHash)
    to = [me]
  }

  validateRecipients(to)

  var entry = new Entry({
    type: EventType.msg.new,
    dir: Dir.outbound,
    public: isPublic,
    chain: !!options.chain,
    from: toObj(ROOT_HASH, this._myRootHash),
    to: to
  })

  var recipients
  var lookup = isPublic ? this.lookupBTCAddress : this.lookupBTCPubKey
  var validate = options.chain ? Q.ninvoke(Parser, 'parse', data) : Q.resolve()

  return validate
    .then(function () {
      return Q.all(to.map(lookup))
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
      var entries
      if (isPublic) {
        entries = to.map(function (addr, i) {
          return entry.clone().set({
            to: {
              fingerprint: addr
            },
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
      return find(identity.pubkeys, function (k) {
        return k.type === 'bitcoin' && k.networkName === self.networkName
      })
    })
}

Driver.prototype.lookupBTCPubKey = function (recipient) {
  return this.lookupBTCKey(recipient).then(function (k) {
    return k.value
  })
}

Driver.prototype.lookupBTCAddress = function (recipient) {
  if (recipient.fingerprint === constants.IDENTITY_PUBLISH_ADDRESS) {
    return Q.resolve(recipient.fingerprint)
  }

  return this.lookupBTCKey(recipient).then(function (k) {
    return k.fingerprint
  })
}

Driver.prototype.destroy = function () {
  delete this._fingerprintToIdentity

  // sync
  this.chainwriter.destroy()
  this._rawTxStream.close() // custom close method

  // async
  return Q.all([
    this.keeper.destroy(),
    Q.ninvoke(this.p2p, 'destroy'),
    Q.ninvoke(this.addressBook, 'close'),
    Q.ninvoke(this.msgDB, 'close'),
    Q.ninvoke(this.txDB, 'close'),
    Q.ninvoke(this._log, 'close')
  ])
    .done()
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
  if (Buffer.isBuffer(data)) return data
  if (typeof data === 'object')

  data = utils.stringify(data)
  return new Buffer(data, enc || 'binary')
}

function rethrow (err) {
  if (err) throw err
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

  var rh = getEntryProp(src, ROOT_HASH) || curHash
  setEntryProp(dest, ROOT_HASH, rh)
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

var toObjectStream = map.bind(null, function (data, cb) {
  if (data.type !== 'put' || typeof data.value !== 'object') {
    return cb()
  }

  cb(null, rebuf(data.value))
})
