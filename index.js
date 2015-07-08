
var assert = require('assert')
var util = require('util')
var EventEmitter = require('events').EventEmitter
var Writable = require('readable-stream').Writable
var Q = require('q')
var typeforce = require('typeforce')
var debug = require('debug')('chained-chat')
var levelup = require('levelup')
var changesFeed = require('changes-feed')
var bitcoin = require('bitcoinjs-lib')
var Queue = require('level-jobs')
var extend = require('extend')
var utils = require('tradle-utils')
var concat = require('concat-stream')
var mapStream = require('map-stream')
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
var normalizeJSON = require('./normalizeJSON')
var chainstream = require('./chainstream')
// var OneBlock = require('./oneblock')
var identityStore = require('./identityStore')
var getDHTKey = require('./getDHTKey')
var LogEntry = require('./logEntry')
var constants = require('tradle-constants')
var Tags = require('./tags')
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
Driver.LogEntry = LogEntry

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

  this.signingKey = toKey(
    find(this.identityKeys, function (k) {
      return k.type === 'dsa' && k.purpose === 'sign'
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
    key: this.signingKey.priv()
  })

  this.chainwriter = new ChainWriter({
    wallet: wallet,
    keeper: keeper,
    networkName: networkName,
    minConf: 0,
    prefix: PREFIX
  })

  this.queueDB = levelup(this._prefix('txs.db'), {
    db: this.leveldown,
    valueEncoding: 'json'
  })

  this.chainwriterq = Queue(this.queueDB, this._chainWriterWorker, 1)
  this.chainwriterq.on('error', function (err) {
    self.emit('error', err)
  })

  this.chainloader = new ChainLoader({
    keeper: keeper,
    networkName: networkName,
    prefix: PREFIX,
    lookup: this.getKeyAndIdentity2
  })

  this.addressBook = identityStore({
    path: this._prefix('identities.db'),
    leveldown: this.leveldown
  })

  // in-memory cache of recent conversants
  this._fingerprintToIdentity = {}
  this._setupP2P()

  this._setupLog()
    .then(function () {
      return Q.all([
        self._prepIdentity(),
        self._setupTxStream()
      ])
    })
    .done(function () {
      self._ready = true
      self.emit('ready')
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

Driver.prototype._setupTxStream = function () {
  var self = this
  var defer = Q.defer()
  var lastBlock
  var lastBlockTxIds = []

  var rs = this._log.createReadStream({ reverse: true })
    .pipe(logFilter(Tags.fromChain))
    .on('data', function (data) {
      var txId = bitcoin.Transaction.fromBuffer(data.tx).getId()
      lastBlockTxIds.unshift(txId)
      if (typeof lastBlock === 'undefined') {
        lastBlock = data.height
      } else {
        if (data.height < lastBlock) {
          rs.destroy()
        }
      }
    })
    .once('close', function () {
      self._streamTxs(lastBlock, lastBlockTxIds)
      defer.resolve()
    })

  return defer.promise
}

Driver.prototype._streamTxs = function (fromHeight, skipIds) {
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
    mapStream(function (txInfo, cb) {
      var id = txInfo.tx.getId()
      if (txInfo.height < fromHeight ||
        (txInfo.height === fromHeight && skipIds.indexOf(id) === -1)) {
        return cb()
      }

      var copy = extend({}, txInfo)
      var entry = new LogEntry()
        .copy(copy)
        .set('tx', copy.tx.toBuffer())
        .tag(Tags.tx)

      cb(null, entry)
    }),
    this._logWS
  )
}

Driver.prototype._setupLog = function () {
  var self = this

  this._logDB = levelup(this._prefix('msg-log.db'), {
    db: this.leveldown,
    valueEncoding: 'json'
  })

  this._log = changesFeed(this._logDB)

  var createReadStream = this._log.createReadStream
  this._log.createReadStream = function (options) {
    return pump(
      createReadStream.apply(self._log, arguments),
      mapStream(function (data, cb) {
        var hasKeys = !options || options.keys !== false
        var hasValues = !options || options.values !== false
        if (hasValues) {
          if (hasKeys) data.value = normalizeJSON(data.value)
          else data = normalizeJSON(data)
        }

        cb(null, data)
      })
    )
  }

  var append = this._log.append
  this._log.append = function (entry, cb) {
    typeforce('LogEntry', entry)
    entry.validate()
    return append.call(self._log, entry.toJSON(), cb || noop)
  }

  this._logWS = new Writable({ objectMode: true })
  this._logWS.on('error', this._onLogWriteError)
  this._logWS._write = function (chunk, enc, next) {
    self._log.append(chunk)
    next()
  }

  var defer = Q.defer()
  this._log
    .createReadStream({ limit: 1, reverse: true, values: false })
    .pipe(concat(function (results) {
      self._readLog(results[0])
      defer.resolve()
    }))

  return defer.promise
}

Driver.prototype._readLog = function (startId) {
  this._logRS = this._log
    .createReadStream({
      live: true,
      since: startId || 0
    })
    .on('error', this._onLogReadError)
    .pipe(mapStream(function (data, cb) {
      var id = data.change
      data = data.value
      // so whoever processes this can refer to this entry
      var entry = LogEntry.fromJSON(data)
        .id(id)

      cb(null, entry)
    }))

  this._logRS.setMaxListeners(0)

  // this._log
  //   .createReadStream({ live: true })
  //   .pipe(mapStream(function (data, cb) {
  //     cb(null, utils.stringify(data))
  //   }))
  //   .pipe(process.stdout)

  // this._logStream = duplexify(ws, rs)

  this._txStream = this._logRS.pipe(logFilter(Tags.tx))
    .pipe(mapStream(function (entry, cb) {
      var tx = bitcoin.Transaction.fromBuffer(entry.get('tx'))
      entry.set('tx', tx)
      cb(null, entry)
    }))

  // var inbound = this._logRS.pipe(logFilter(Tags.inbound))
  // inbound.on('data', this._onInboundMessage)

  var inboundPlain = this._logRS.pipe(logFilter(Tags.inbound, Tags.plain))
  inboundPlain.on('data', this._onInboundPlaintext)

  // this._newInboundPlain = this._logRS.pipe(logFilter(Tags.plain, Tags.inbound))
  // this._newPublicStruct = this._logRS.pipe(logFilter(Tags.struct, Tags.public))
  // this._

  var inboundStruct = this._logRS.pipe(logFilter(Tags.struct, Tags.inbound, Tags.private))
  inboundStruct.on('data', this._onInboundStruct)

  var newOutboundPlain = this._logRS.pipe(logFilter(Tags.new, Tags.plain, Tags.outbound))
  newOutboundPlain.on('data', this._onNewOutboundPlainMessage)

  var newOutboundStruct = this._logRS.pipe(logFilter(Tags.new, Tags.struct, Tags.outbound))
  newOutboundStruct.on('data', this._onNewOutboundStructMessage)

  var storedOutboundStruct = this._logRS.pipe(logFilter(Tags.struct, Tags.outbound, Tags.stored))
  storedOutboundStruct.on('data', this._onStoredOutboundStructMessage)

  // this._newOutbound.on('data', this._onNewOutboundMessage)

  // this._newPlain = this._logRS.pipe(logFilter(Tags.new, Tags.plain))

  // this._newPlainInbound = this._newPlain.pipe(logFilter(Tags.inbound))
  // this._newPlainInbound.on('data', this._onInboundPlain)

  // this._newStructured = this._logRS.pipe(logFilter(Tags.new, Tags.struct))
  // var structStored = this._logRS.pipe(logFilter(Tags.struct, Tags.stored))
  var structChained = this._logRS.pipe(logFilter(Tags.struct, Tags.fromChain))
  structChained.on('data', this._onStructChained)

  var objStream = chainstream({
    chainloader: this.chainloader,
    lookup: this.lookupByDHTKey
  })

  pump(
    this._txStream,
    objStream,
    mapStream(this._postProcessChainedObj.bind(this)),
    this._logWS
  )
}

Driver.prototype._postProcessChainedObj = function (chainedObj, cb) {
  var entry
  if (!chainedObj.parsed || chainedObj.errors.length) {
    entry = new LogEntry()
      .copy(chainedObj)
      .set('tx', chainedObj.tx.toBuffer())
      .tag(
        Tags.fromChain,
        Tags.nonData
      )
      .prev(chainedObj)

    return cb(null, entry)
  }

  // how do we know prev?
  // necessary? maybe just write this straight to log
  var from = chainedObj.from.getOriginalJSON()
  var to = chainedObj.to && chainedObj.to.getOriginalJSON()

  this._debug('picked up object from chain', chainedObj)
  entry = new LogEntry()
    .copy(chainedObj, 'height', 'data')
    .set({
      from: from[ROOT_HASH],
      tx: chainedObj.tx.toBuffer()
    })
    .set(CUR_HASH, chainedObj.key)
    .set(ROOT_HASH, chainedObj.parsed.data[ROOT_HASH] || chainedObj.key)
    .set(TYPE, chainedObj.parsed.data[TYPE])
    .tag(
      Tags.struct,
      Tags.fromChain,
      from[ROOT_HASH] === this._myRootHash ? Tags.outbound : Tags.inbound,
      chainedObj.type === 'public' ? Tags.public : Tags.private
    )
    .prev(chainedObj)

  if (to) entry.set('to', to[ROOT_HASH])

  cb(null, entry)
}

Driver.prototype._onInboundPlaintext = function (entry) {
  this.emit('message', entry.toJSON())
}

Driver.prototype._onInboundStruct = function (entry) {
  var self = this
  var destroyed = false
  var other
  var stream = this._log.createReadStream({ reverse: true })
    .on('data', function (d) {
      if (destroyed) return

      var old = LogEntry.fromJSON(d.value)
      if (old.get(CUR_HASH) === entry.get(CUR_HASH)) {
        other = old
        destroyed = true
        stream.destroy()
      }
    })
    .once('close', function () {
      if (entry.hasTag(Tags.fromChain)) {
        if (other) {
          self.emit('resolved', entry.toJSON())
        }
      } else {
        self.emit('message', entry.toJSON())
      }
    })
}

Driver.prototype._onStructChained = function (entry) {
  var self = this
  this._debug('chained (read)', entry)
  this.emit('chained', entry.toJSON())
  Parser.parse(entry.get('data'), function (err, parsed) {
    if (err) return self.emit('error', 'stored invalid struct', err)

    if (parsed.data[TYPE] === Identity.TYPE) {
      // what about attachments?
      copyDHTKeys(parsed.data, entry)
      self.addressBook.update(entry.id(), parsed.data)
    }
  })
}

Driver.prototype._onNewOutboundStructMessage = function (entry) {
  var self = this
  var isPublic = entry.hasTag(Tags.public)
  var builder = new Builder()
    .data(entry.get('data'))

  var atts = entry.get('attachments')
  if (atts) {
    atts.forEach(builder.attach, builder)
  }

  if (entry.get('sign')) {
    builder.signWith(this.signingKey)
  }

  var lookup = isPublic ? this.lookupBTCAddress : this.lookupBTCPubKey
  var tasks = [
    Q.ninvoke(builder, 'build')
  ].concat(entry.get('to').map(lookup))

  var ssEntry = new LogEntry()
    .tag(Tags.struct, Tags.stored, Tags.outbound, getPrivacyTag(entry))
    .prev(entry)
    .copy(entry, 'chain', 'to', 'sign')

  return Q.all(tasks)
    .then(function (results) {
      var build = results[0]
      var buf = build.form
      var recipients = results.slice(1)
      ssEntry.set('data', buf)
      return self.chainwriter.create()
        .data(buf)
        .setPublic(isPublic)
        .recipients(recipients)
        .execute()
    })
    .then(function (resp) {
      copyDHTKeys(ssEntry, resp.key)
      if (!isPublic) {
        ssEntry.set('shares', resp.shares.map(function (share) {
          return {
            fingerprint: share.address,
            data: share.encryptedKey,
            permission: share.value
          }
        }))
      }

      self._debug('stored (write)', ssEntry.get(ROOT_HASH))
      return self.log(ssEntry)
    })
    .done()
}

Driver.prototype._sendP2P = function (entry) {
  // TODO:
  //   do we log that we sent it?
  //   do we log when we delivered it? How do we know it was delivered?
  var self = this
  var msg = toBuffer({
    data: entry.get('data'),
    type: getMsgTypeTag(entry)
  })

  var to = entry.get('to')
  var lookups = to.map(this.lookupIdentity)

  return Q.allSettled(lookups)
    .then(function (results) {
      var found = results.filter(function (r) {
          return r.state === 'fulfilled'
        })
        .map(function (r) {
          return r.value
        })

      if (found.length !== results.length) {
        var missing = results.map(function (r, i) {
          return r.state === 'fulfilled' ? null : to[i]
        })
        .filter(notNull)
        .join(', ')

        self.emit('warn', 'no identities found for: ' + missing)
      }

      found.forEach(function (i) {
        var fingerprint = getFingerprint(i)
        self._debug('messaging', fingerprint)
        self.p2p.send(msg, fingerprint)
      })
    })
}

Driver.prototype._onStoredOutboundStructMessage = function (entry) {
  if (entry.get('chain')) this.putOnChain(entry)
  if (entry.hasTag(Tags.public)) return

  this._sendP2P(entry)
}

Driver.prototype._onNewOutboundPlainMessage = function (entry) {
  this._sendP2P(entry)
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
    }
  } else if (query.fingerprint) {
    var pub = this.getPublicKey(query.fingerprint)
    if (pub) {
      return Q.resolve(me)
    }
  }

  return Q.ninvoke(this.addressBook, 'query', query)
    .then(function (results) {
      return results[0]
    })
    .catch(function (err) {
      self._debug('unable to find identity', query, err)
      throw err
    })
}

Driver.prototype.log = function (entry) {
  return Q.ninvoke(this._log, 'append', entry)
}

Driver.prototype._chainWriterWorker = function (task, cb) {
  var self = this

  var chainEntry = LogEntry.fromJSON(task)
  return this.chainwriter.chain()
    .type(task.type)
    .data(task.data)
    .address(task.address)
    .execute()
    .then(function (tx) {
      // ugly!
      chainEntry.set('tx', tx.toBuffer())
      copyDHTKeys(chainEntry, task)
      self._debug('chained (write)', chainEntry)
      return self.log(chainEntry)
    })
    .then(function () {
      cb()
//       self.emit('chained', task, update)
    })
    .catch(cb)
    .done()
}

Driver.prototype.createReadStream = function (options) {
  return this._log.createReadStream(options)
}

Driver.prototype._prefix = function (path) {
  return this.pathPrefix + '-' + path
}

Driver.prototype._onmessage = function (msg, fingerprint) {
  var self = this

  try {
    msg = JSON.parse(msg)
  } catch (err) {
    return this.emit('warn', 'received message not in JSON format', msg)
  }

  this._debug('received msg', msg)
  var tags = [Tags.inbound, Tags.private]
  if (msg.type !== Tags.plain && msg.type !== Tags.struct) {
    this.emit('warn', 'unexpected message type: ' + msg.type)
  } else {
    tags.push(msg.type)
  }

  var entry = new LogEntry()
    .set({
      data: msg.data,
      to: this._myRootHash
    })
    .tag(tags)

  this.lookupIdentity({ fingerprint: fingerprint })
    .then(function (from) {
      entry.set('from', from[ROOT_HASH])
    })
    .catch(function (err) {
      self._debug('failed to find identity by fingerprint', fingerprint, err)
    })
    .finally(function () {
      return self.log(entry)
    })
    .done()
}

Driver.prototype.putOnChain = function (entry) {
  var self = this
  assert(entry.get(ROOT_HASH) && entry.get(CUR_HASH))

  var isPublic = entry.hasTag(Tags.public)
  var type = isPublic ? TxData.types.public : TxData.types.permission
  var recipients = entry.get(isPublic ? 'to' : 'shares')
  if (!recipients) {
    throw new Error('no recipients!')
    // var pubKey = to.keys({
    //   type: 'bitcoin',
    //   networkName: this.networkName
    // })[0].pubKeyString()

    // recipients = [pubKey]
  }

  // TODO: do we really need a separate queue/log for this?
  var curHash = entry.get(CUR_HASH)
  recipients.forEach(function (r) {
    // queue up this entry
    var chainEntry = new LogEntry()
      .prev(entry)
      .tag(Tags.struct, 'to-chain', Tags.outbound)
      .set('type', type)
      .set('data', isPublic ? curHash : r.data)

    copyDHTKeys(chainEntry, entry, curHash)
    self.lookupBTCAddress(r)
      .then(function (addr) {
        chainEntry.set('address', addr)
        self.chainwriterq.push(chainEntry.toJSON())
      })
      .catch(function (err) {
        self._debug('unable to find on-chain recipient\'s address', err)
      })
      .done()
  })
}

Driver.prototype.sendPlaintext = function (options) {
  var msg = options.msg
  var to = options.to
  if (typeof to === 'string') to = [to]

  assert(to && msg, 'missing required arguments')

  validateRecipients(to)
  var entry = new LogEntry()
    .set({
      data: msg,
      from: this._myRootHash,
      to: to
    })
    .tag(Tags.new, Tags.plain, Tags.outbound)

  this.log(entry)
}

Driver.prototype.publish = function (options) {
  return this.sendStructured(extend({
    public: true,
    chain: true
  }, options))
}

Driver.prototype.sendStructured = function (options) {
  typeforce({
    msg: 'Object',
    attachments: '?Array',
    to: '?Array',
    public: '?Boolean',
    sign: '?Boolean',
    chain: '?Boolean'
  }, options)

  var obj = options.msg
  assert(TYPE in obj, 'structured messages must specify type property: ' + TYPE)

  // either "public" or it has recipients
  var isPublic = !!options.public
  assert(isPublic ^ !!options.to, 'private msgs must have recipients, public msgs cannot')

  var entry = new LogEntry()
    .set({
      type: obj[TYPE],
      data: obj,
      from: this._myRootHash
    })
    .copy(options, 'to', 'attachments', 'sign', 'chain')
    .tag(Tags.new, Tags.struct, Tags.outbound, isPublic ? Tags.public : Tags.private)

  var to
  if (isPublic) {
    if (entry.get('type') === Identity.TYPE) {
      to = [{
        fingerprint: constants.IDENTITY_PUBLISH_ADDRESS
      }]
    } else {
      var me = {}
      me[ROOT_HASH] = this._myRootHash
      to = [me]
    }
  } else {
    to = options.to
  }

  validateRecipients(to)
  entry.set('to', to)
  this.log(entry)
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
    Q.ninvoke(this.queueDB, 'close'),
    Q.ninvoke(this.addressBook, 'close'),
    Q.ninvoke(this._logDB, 'close')
  ])
  .done()
  // .done(console.log.bind(console, this.pathPrefix + ' is dead'))
}

Driver.prototype._debug = function () {
  var args = [].slice.call(arguments)
  args.unshift(this.identityJSON.name.formatted)
  return debug.apply(null, args)
}

// function call (method) {
//   return function (obj) {
//     return obj[method]()
//   }
// }

// function caller (method) {
//   return function (obj) {
//     return obj[method].bind(obj)
//   }
// }

function toBuffer (data) {
  if (Buffer.isBuffer(data)) return data
  return new Buffer(utils.stringify(data), 'binary')
}

// function msgToBuf (msg) {
//   var contents = msg.data
//   var buf = new Buffer(1 + contents.length)
//   buf[0] = msg.type
//   contents.copy(buf, 1, 0, contents.length)
//   return buf
// }

// function bufToMsg (buf) {
//   return {
//     type: buf[0],
//     data: buf.slice(1)
//   }
// }

// function rethrow (err) {
//   if (err) throw err
// }

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

// function isIdentityTx (txInfo) {
//   return txInfo.tx.outs.some(function (out) {
//     return utils.getAddressFromOutput(out, networkName) === constants.IDENTITY_PUBLISH_ADDRESS
//   })
// }

function copyDHTKeys (dest, src, curHash) {
  if (typeof curHash === 'undefined') {
    if (typeof src === 'string') curHash = src
    else {
      curHash = getProp(src, CUR_HASH) || getProp(src, ROOT_HASH)
    }

    src = dest
  }

  var rh = getProp(src, ROOT_HASH) || curHash
  setProp(dest, ROOT_HASH, rh)
  setProp(dest, CUR_HASH, curHash)
}

function getProp (obj, name) {
  return obj instanceof LogEntry ? obj.get(name) : obj[name]
}

function setProp (obj, name, val) {
  if (obj instanceof LogEntry) obj.set(name, val)
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

function logFilter (/* tags */) {
  var tags = [].concat.apply([], arguments)

  return mapStream(function (entry, cb) {
    var matches = tags.every(entry.hasTag, entry)
    if (matches) return cb(null, entry)
    else cb()
  })
}

function getPrivacyTag (entry) {
  return getTaggedWith(entry, Tags.public, Tags.private)
}

function getMsgTypeTag (entry) {
  return getTaggedWith(entry, Tags.plain, Tags.struct)
}

function getTaggedWith (entry /*, tags */) {
  var tags = ArrayProto.concat.apply([], ArrayProto.slice.call(arguments, 1))
  var tag = find(tags, entry.hasTag, entry)
  if (tag) return tag

  throw new Error('tag not found')
}

function notNull (o) {
  return !!o
}
