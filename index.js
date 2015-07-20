
var assert = require('assert')
var util = require('util')
var EventEmitter = require('events').EventEmitter
var Writable = require('readable-stream').Writable
var omit = require('object.omit')
var pick = require('object.pick')
var Q = require('q')
var typeforce = require('typeforce')
var debug = require('debug')('tim')
var levelup = require('levelup')
var reemit = require('re-emitter')
var bitcoin = require('bitcoinjs-lib')
var extend = require('extend')
var utils = require('tradle-utils')
var concat = require('concat-stream')
var map = require('map-stream')
var pump = require('pump')
var typeforce = require('typeforce')
var find = require('array-find')
var ChainedObj = require('chained-obj')
var TxData = require('tradle-tx-data').TxData
var ChainWriter = require('bitjoe-js')
var ChainLoader = require('chainloader')
var Wallet = require('simple-wallet')
var cbstreams = require('cb-streams')
var Zlorp = require('zlorp')
var mi = require('midentity')
var Identity = mi.Identity
var toKey = mi.toKey
var Parser = ChainedObj.Parser
var Builder = ChainedObj.Builder
var lb = require('logbase')
var Entry = lb.Entry
var unchainer = require('./unchainer')
// var OneBlock = require('./oneblock')
// var identityStore = require('./identityStore')
var filter = require('./filterStream')
var getDHTKey = require('./getDHTKey')
// var filterByTag = require('./filterByTag')
var constants = require('tradle-constants')
var EventType = require('./eventType')
var Dir = require('./dir')
var createIdentityDB = require('./identityDB')
var createMsgDB = require('./msgDB')
var createTxDB = require('./txDB')
var noop = function () {}
var TYPE = constants.TYPE
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH
var PREFIX = constants.OP_RETURN_PREFIX
var ArrayProto = Array.prototype

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
    priv: this.getPrivateKey({
      networkName: networkName,
      type: 'bitcoin'
    }).priv
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

  this._setupLog()
  Q.all([
      self._prepIdentity(),
      self._setupTxStream()
    ])
    .done(function () {
      self._ready = true
      self.emit('ready')
      self._chainTheUnchained()
      self._unchainTheChained()
      self._sendTheUnsent()
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
Driver.prototype._unchainTheChained = function () {
  var self = this
  var db = this.txDB

  if (!db.isLive()) return db.once('live', this._unchainTheChained)
  if (this._unchaining) return

  this._unchaining = true

  pump(
    db.liveStream({
      old: true,
      tail: true
    }),
    toObjectStream(),
    filter(function (entry) {
      return !entry[CUR_HASH]
    }),
    this.unchainer,
    map(function (chainedObj, cb) {
      self._debug('unchained (read)', chainedObj.key)
      // var query = {}
      // query[CUR_HASH] = chainedObj.key
      // self.msgDB.query(query)
      var type = chainedObj.errors && chainedObj.errors.length ?
        EventType.chain.readError :
        EventType.chain.readSuccess

      var tx = chainedObj.tx
      tx.body = tx.body.toBuffer()
      var from = chainedObj.from.getOriginalJSON()
      var to = chainedObj.to && chainedObj.to.getOriginalJSON()
      var entry = new Entry(omit(chainedObj, ['parsed', 'key', 'data', 'type'])) // data is stored in keeper
        .set(CUR_HASH, chainedObj.key)
        .set(ROOT_HASH, chainedObj.parsed.data[ROOT_HASH] || chainedObj.key)
        .set(TYPE, chainedObj.parsed.data[TYPE])
        .set({
          type: type,
          public: chainedObj.type === 'public',
          from: from[ROOT_HASH],
          to: to && to[ROOT_HASH]
        })
        .prev(chainedObj.id)

      cb(null, entry)
    }),
    this._log
  ).on('error', rethrow)
}

/**
 * write to chain
 */
Driver.prototype._chainTheUnchained = function () {
  var self = this
  var db = this.msgDB

  if (!db.isLive()) return db.once('live', this._chainTheUnchained)
  if (this._chaining) return

  this._chaining = true

  pump(
    db.liveStream({
      old: true,
      tail: true
    }),
    toObjectStream(),
    map(function (entry, cb) {
      if (!entry.chain ||
          entry.chained ||
          entry.dir !== Dir.outbound) {
        return
      }

      self.putOnChain(new Entry(entry))
        .done(function (nextEntry) {
          cb(null, nextEntry)
        })
    }),
    this._log
  ).on('error', rethrow)
}

Driver.prototype._sendTheUnsent = function () {
  var self = this
  var db = this.msgDB

  if (!db.isLive()) return db.once('live', this._chainTheUnchained)

  pump(
    db.liveStream({
      tail: true,
      old: true
    }),
    toObjectStream(),
    map(function (entry, cb) {
      if (entry.sent || entry.public) return cb()

      if (entry.errors && entry.errors.length > MAX_CHAIN_RETRIES) {
        self._debug('giving up on chaining', entry)
        return cb()
      }

      debugger
      self._sendP2P(entry)
        .then(function () {
          return new Entry({
            type: EventType.msg.sendSuccess
          })
        })
        .catch(function (err) {
          var errEntry = new Entry({
              type: EventType.msg.sendError
            })

          addError(errEntry, err)
          return errEntry
        })
        .done(function (nextEntry) {
          nextEntry.prev(entry)
          cb(null, nextEntry)
        })
    }),
    this._log
  ).on('error', rethrow)
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
    this._log
  ).on('error', rethrow)
}

Driver.prototype._setupLog = function () {
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

  return this.lookupIdentity(info.to)
    .then(function (identity) {
      var fingerprint = getFingerprint(identity)
      self._debug('messaging', fingerprint)
      debugger
      var msg = toBuffer(pick(info, [
        'data',
        'type'
      ]))

      self.p2p.send(msg, fingerprint)
    })
}

Driver.prototype.lookupChainedObj = function (info) {
  typeforce({
    tx: 'Object'
  }, info)

  var tx = bitcoin.Transaction.fromBuffer(info.tx.body)
  var chainedObj
  return this.chainloader.load(tx)
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

Driver.prototype.lookupIdentity = function (query) {
  var self = this
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

Driver.prototype._onmessage = function (msg, fingerprint) {
  var self = this

  debugger
  try {
    msg = JSON.parse(msg)
  } catch (err) {
    return this.emit('warn', 'received message not in JSON format', msg)
  }

  this._debug('received msg', msg)

  var entry = new Entry({
    type: EventType.msg.received,
    data: msg.data,
    to: this._myRootHash,
    dir: Dir.inbound
    // public: false
  })

  var from = {}
  this.lookupIdentity({ fingerprint: fingerprint })
    .then(function (identity) {
      from[ROOT_HASH] = identity[ROOT_HASH]
    })
    .catch(function (err) {
      from.fingerprint = fingerprint
      self._debug('failed to find identity by fingerprint', fingerprint, err)
    })
    .finally(function () {
      entry.set('from', from)
      return self.log(entry)
    })
    .done()
}

Driver.prototype.putOnChain = function (entry) {
  var self = this
  assert(entry.get(ROOT_HASH) && entry.get(CUR_HASH))

  var curHash = entry.get(CUR_HASH)
  var isPublic = entry.get('public')
  var type = isPublic ? TxData.types.public : TxData.types.permission
  var share = entry.get('share')
  var data = isPublic ? curHash : share.data
  var to = entry.get('to')
  var nextEntry = new Entry()
    .prev(entry)
    .set(ROOT_HASH, entry.get(ROOT_HASH))
    .set(CUR_HASH, entry.get(CUR_HASH))
    .set({
      public: isPublic,
      chain: true,
      dir: Dir.outbound
    })

  return this.lookupBTCAddress(to)
    .then(shareWith)
    .catch(function (err) {
      self._debug('chaining failed', err)
      var errEntry = new Entry({
          type: EventType.chain.writeError
        })
        .prev(entry)

      addError(errEntry, err)
      return errEntry
    })

  function shareWith (addr) {
    nextEntry.set('address', addr)
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
  }
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
    to: '?Array',
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
    var me = {}
    me[ROOT_HASH] = this._myRootHash
    to = [me]
  }

  validateRecipients(to)

  var entry = new Entry({
    type: EventType.msg.new,
    dir: Dir.outbound,
    public: isPublic,
    chain: !!options.chain,
    from: this._myRootHash,
    to: to
  })

  var recipients
  var lookup = isPublic ? this.lookupBTCAddress : this.lookupBTCPubKey
  var validate = options.chain ? Q.ninvoke(Parser, 'parse', data) : Q.resolve()

  return validate
    .catch(function (err) {
      debugger
      throw err
    })
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
      if (isPublic) {
        entry.set('to', to[0])
        return self.log(entry)
      }

      return Q.all(resp.shares.map(function (share, i) {
        entry = entry.clone().set({
          to: to[i],
          share: {
            fingerprint: share.address,
            data: share.encryptedKey,
            permission: share.value
          }
        })

        return self.log(entry)
      }))
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

function toBuffer (data) {
  if (Buffer.isBuffer(data)) return data
  return new Buffer(utils.stringify(data), 'binary')
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
    if (typeof src === 'string') curHash = src
    else {
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

function notNull (o) {
  return !!o
}

function toErrJSON (err) {
  var json = {}

  Object.getOwnPropertyNames(err).forEach(function (key) {
    json[key] = err[key];
  });

  delete json.stack
  return json
}

function addError (entry, err) {
  var errs = entry.get('errors') || []
  errs.push(toErrJSON(err))
  entry.set('errors', errs)
}

function filterByEventTypes (types) {
  types = ArrayProto.concat.apply([], arguments)
  return function (entry, cb) {
    return types.indexOf(entry.get('type')) !== -1
  }
}

var toObjectStream = map.bind(null, function (data, cb) {
  if (data.type !== 'put' || typeof data.value !== 'object') {
    return cb()
  }

  cb(null, data.value)
})
