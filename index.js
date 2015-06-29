
var assert = require('assert')
var crypto = require('crypto')
var util = require('util')
var EventEmitter = require('events').EventEmitter
var Q = require('q')
var typeforce = require('typeforce')
var debug = require('debug')('chained-chat')
var sublevel = require('level-sublevel')
var levelup = require('levelup')
var levelQuery = require('level-queryengine')
var jsonqueryEngine = require('jsonquery-engine')
var changesFeed = require('changes-feed')
var bitcoin = require('bitcoinjs-lib')
var Queue = require('level-jobs')
var bufferEqual = require('buffer-equal')
var extend = require('extend')
var omit = require('object.omit')
var pick = require('object.pick')
var utils = require('tradle-utils')
var concat = require('concat-stream')
var mapStream = require('map-stream')
var safe = require('safecb')
var typeforce = require('typeforce')
var find = require('array-find')
var ChainedObj = require('chained-obj')
var TxData = require('tradle-tx-data').TxData
var ChainWriter = require('bitjoe-js')
var ChainLoader = require('chainloader')
var Keeper = require('bitkeeper-js')
var Wallet = require('simple-wallet')
var cbstreams = require('cb-streams')
var Zlorp = require('zlorp')
var mi = require('midentity')
var Identity = mi.Identity
var toKey = mi.toKey
var Parser = ChainedObj.Parser
var Builder = ChainedObj.Builder
var normalizeJSON = require('./normalizeJSON')
var normalizeMsg = require('./normalizeMsg')
var chainstream = require('./chainstream')
var OneBlock = require('./oneblock')
var identityStore = require('./identityStore')
var getDHTKey = require('./getDHTKey')
var constants = require('./constants')
var ROOT_HASH = constants.rootHash
var CUR_HASH = constants.currentHash
var PREFIX = 'tradle'
// var INDICES = [
//   'key',
//   'tx',
//   'them',
//   'me',
//   'incoming',
//   'status',
//   'type'
// ]

var normalizeStream = mapStream.bind(null, function (data, cb) {
  cb(null, normalizeJSON(data))
})

var MessageType = Driver.MessageType = {
  plaintext: 1 << 1,
  chained: 1 << 2
}

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
    priv: identity.keys({
      networkName: networkName,
      type: 'bitcoin'
    })[0].priv()
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
    debugger
    self.emit('error', err)
  })

  this.chainloader = new ChainLoader({
    keeper: keeper,
    networkName: networkName,
    prefix: PREFIX,
    lookup: this._lookupByFingerprint
  })

  this._fingerprintToIdentity = {} // in-memory cache of recent conversants
  this._setupP2P()
  this._setupStorage()

  var tasks = [
    this._prepIdentity(),
    this._setupTxStream()
  ]

  Q.all(tasks)
    .then(function () {
      self._setupInputStreams()
      self._logStream.on('data', self._onLogged)
      self._logStream.on('error', self._onLogError)
      self.emit('ready')
    })
    .done()
}

Driver.prototype._onLogError = function (err) {
  debugger
  throw err
}

Driver.prototype._onLogged = function (data) {
  if (data.tx) return
  if (data.type === Identity.TYPE) {
    this._addressBook.update(JSON.parse(data.data.toString('binary')))
  }

  if (data.from === this._myRootHash) {
    this._processOutgoing(data)
  } else {
    this._processIncoming(data)
  }
}

Driver.prototype._processOutgoing = function (obj) {
  var self = this
  // TODO: optimize into batch
  // var query = {}
  // query[ROOT_HASH] = { $in: obj.to }

  // return Q.all(obj.to.map(function (hash) {
  //     var query = {}
  //     query[ROOT_HASH] = hash
  //     return self.lookupIdentity({
  //       query: query
  //     })
  //   }))

  if (obj.mode === 'public') {
    return this.putOnChain(obj)

    // return this.chainwriter.create()
    //   .data(obj.data)
    //   .setPublic(true)
    //   .execute()
    //   .then(function (resp) {

    //   })
  }

  debugger
  return Q.all(obj.to.map(this.lookupIdentity))
    .then(function (identities) {
      if (err) throw err

      identities.forEach(function (i) {
        if (obj.type === 'text') {
          var msg = toBuffer(pick(obj, ['type', 'data']))
          self.p2p.send(msg, getFingerprint(i))
        } else {
          debugger
        }
      })
    })

  // this.lookupIdentity({
  //   query: query
  // }, function (err, identities) {
  //   var msg = msgToBuf({
  //     type: obj.type,
  //     data: obj.data
  //   })

  //   identities.forEach(function (i) {
  //     self.p2p.send(msg, getFingerprint(i))
  //   })
  // })
}

Driver.prototype._processIncoming = function (obj) {
  debugger
  var self = this

  assert(ROOT_HASH in obj, 'expected ' + ROOT_HASH)
  var query = pick(obj, ROOT_HASH)

  this._logDB.query(query)
    .on('error', rethrow) // TODO: handle
    .pipe(concat(function(results) {
      debugger
      var saved = results.pop()
      if (!bufferEqual(saved.data, obj.data)) return

      self.emit('resolved', chainedObj)
      saved.status = 'resolved'

      var update = pick(obj, ['type', ROOT_HASH, CUR_HASH])
      update.status = 'resolved'
      self._log.append(update)
    }))
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
    // var identity = self._fingerprintToIdentity[fingerprint]
    // if (identity) return self.emit('connect', identity, addr)

    // self.identityByFingerprint
  })
}

Driver.prototype._setupTxStream = function () {
  var self = this
  var defer = Q.defer()

  this._lastBlock = new OneBlock({
    path: this._prefix('lastBlock.db'),
    leveldown: this.leveldown
  })

  this._lastBlock.once('ready', function () {
    self.rawtxstream = cbstreams.stream.txs({
      live: true,
      interval: self.syncInterval || 60000,
      api: self.blockchain,
      height: self._lastBlock.height(),
      addresses: [
        self.wallet.addressString,
        constants.IDENTITY_PUBLISH_ADDRESS
      ]
    })

    defer.resolve()
  })

  return defer.promise
}

Driver.prototype._setupStorage = function () {
  this._logDB = levelQuery(levelup(this._prefix('msg-log.db'), {
    db: this.leveldown,
    valueEncoding: 'json'
  }))

  // this._logDB.use(jsonqueryEngine())
  // this._logDB.ensureIndex(ROOT_HASH)
  // this._logDB.ensureIndex(CUR_HASH)
  this._log = changesFeed(this._logDB)
  this._logStream = this._log
    .createReadStream({ live: true, keys: false })
    .pipe(normalizeStream())

  this._addressBook = identityStore({
    path: this._prefix('identities.db'),
    leveldown: this.leveldown
  })
}

// Driver.prototype._setupStorage = function (cb) {
  // this._db = levelQuery(levelup(this._prefix('msgs.db'), {
  //   db: this.leveldown,
  //   valueEncoding: 'json'
  // }))

  // this._db.query.use(jsonqueryEngine())
  // INDICES.forEach(function (i) {
  //   this._db.ensureIndex(i)
  // }, this)

  // TODO: record last block height
  // this.chainstream.public.on('data', function (chainedObj) {
  //   var height = chainedObj.tx.height
  // })

  // this.chaindb.on('saved', function (chainedObj) {
  //   console.log('saved', chainedObj.key)

  //   if (!chainedObj.from) return

  //   var from = Identity.fromJSON(chainedObj.from)
  //   var fromKeys = from.keys()
  //   var isFromMe = self.identity.keys().some(function (k1) {
  //     return fromKeys.some(function (k2) {
  //       return k2.equals(k1)
  //     })
  //   })

  //   // var from = Identity.fromJSON(chainedObj.from)
  //   // var isFromMe = self.identity.keys({ type: 'bitcoin' })
  //   //   .some(function (k) {
  //   //     var f = k.fingerprint()
  //   //     return from.keys({ fingerprint: f }).length
  //   //   })

  //   var fromFingerprint = getFingerprint(from)
  //   var to = chainedObj.to && Identity.fromJSON(chainedObj.to)
  //   var toFingerPrint = to && getFingerprint(to)
  //   var tasks = [
  //     self.chaindb.keyForFingerprint(fromFingerprint)
  //   ]

  //   if (toFingerprint) {
  //     tasks.push(self.chaindb.keyForFingerprint(toFingerprint))
  //   }

  //   Q.allSettled(tasks)
  //     .spread(function (fromIdKey, toIdKey) {

  //     })

  //   self.chaindb.keyForFingerprint(fromFingerprint)
  //     .then(function (theirDHTKey) {
  //       var key = getKey({
  //         msg: chainedObj.data,
  //         // type: MessageType.chained,
  //         them: theirDHTKey,
  //         incoming: theirFingerprint !== myFingerprint
  //       })
  //     })
  //     .done()

  //   // var myFingerprint = self.p2p.fingerprint

  //   self._db.get(key, function (err, saved) {
  //     if (err) return

  //     var savedObj = normalizeJSON(saved.msg)
  //     if (!bufferEqual(savedObj, chainedObj.data)) return

  //     self.emit('resolved', chainedObj)
  //     saved.status = 'resolved'

  //     self._log.append({
  //       msgKey: key,
  //       status: saved.status
  //     })

  //     self._msgs.put(key, saved)
  //   })
  // })
// }


Driver.prototype._setupInputStreams = function () {
  // var identityTxStream = txstream.pipe(mapStream(function (txInfo, callback) {
  //   if (isIdentityTx(txInfo)) {
  //     callback(null, txInfo)
  //   }
  // }))

  // var otherTxStream = txstream.pipe(mapStream(function (txInfo, callback) {
  //   if (!isIdentityTx(txInfo)) {
  //     callback(null, txInfo)
  //   }
  // }))

  var publicObjStream = chainstream({
    chainloader: this.chainloader,
    lookup: this.lookupByDHTKey
  })

  var identityStream = publicObjStream.pipe(mapStream(function (data, cb) {
    if (data.parsed.data._type === Identity.TYPE) {
      cb(null, data)
    } else {
      cb()
    }
  }))

  var privateObjStream = chainstream({
    chainloader: this.chainloader,
    lookup: this.lookupByDHTKey
  })

  this.rawtxstream.pipe(publicObjStream)
  this.rawtxstream.pipe(privateObjStream)

  this.chainstreams = {
    public: publicObjStream,
    private: privateObjStream,
    identity: identityStream
  }

  // this.chainstreams.identity.on('data', this._updateIdentity)
  this.chainstreams.public.on('data', function (data) {
    debugger
    self._logIt(data)
  })
  this.chainstreams.private.on('data', this._logIt)
  this._logStream.pipe(mapStream(function (data) {
      if (data.tx && data.height) {
        cb(null, pick(data, ['tx', 'height']))
      }
    }))
    .pipe(this._lastBlock)

  // this._logStream.pipe(mapStream(function (data) {
  //   if (data.type === 'text') {
  //     var msg = msgToBuf({
  //       type: MessageType.plaintext,
  //       data: data.data
  //     })

  //     self.p2p.send(msg, getFingerprint(identity))
  //   }
  // }))
}

Driver.prototype._lookupByFingerprint = function (fingerprint, private) {
  var self = this
  return this.lookupIdentity({
      fingerprint: fingerprint
    })
    .then(function (identity) {
      var key = private && self.getPrivateKey(fingerprint)
      key = key || keyForFingerprint(identity)
      return {
        key: key,
        identity: identity
      }
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

Driver.prototype.getPrivateKey = function (fingerprint) {
  return find(this.identityKeys, function (k) {
    return k.fingerprint === fingerprint
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
    }
  }
  else if (query.fingerprint) {
    var pub = this.getPublicKey(query.fingerprint)
    if (pub) {
      return Q.resolve(me)
    }
  }

  return this._addressBook.query(query)
    .then(function (results) {
      return results[0]
    })

  // var stream = this._identityDB.query(query)
  //   .on('data', function (data) {
  //     if (found(data.value)) {
  //       stream.destroy()
  //     }
  //   })
  //   .once('error', cb)
  //   .once('end', function () {
  //     cb() // no results
  //   })

  // function found (identity) {
  //   if (done) return

  //   var isMe = me[ROOT_HASH] === identity[ROOT_HASH]
  //   var key
  //   if (isMe && options.private) {
  //     key = self.getPrivateKey(query.fingerprint)
  //   }

  //   var key = key || keyForFingerprint(identity, fingerprint)
  //   if (key) {
  //     cb(null, {
  //       key: key,
  //       identity: identity
  //     })

  //     done = true
  //     return true
  //   }
  // }
}

Driver.prototype._logIt = function (obj) {
  if (obj.tx) {
    typeforce({
      txId: 'String'
    }, obj)
  } else {
    typeforce({
      from: 'String',
      to: 'Array'
    }, obj)
  }

  var parsed = obj.parsed
  var type = obj.type || (parsed && parsed.data._type)
  if (type !== 'text') {
    assert(obj[ROOT_HASH], 'expected root hash')
    assert(obj[CUR_HASH], 'expected current hash')
  }

  var copy = omit(obj, 'parsed')
  if (type) copy.type = type

  return Q.ninvoke(this._log, 'append', copy)
}

Driver.prototype._chainWriterWorker = function (task, cb) {
  var self = this

  task = normalizeJSON(task)
  // var promise

  var update
  return this.chainwriter.chain()
    .type(task.type)
    .data(task.data)
    .address(task.address)
    .execute()
    .then(function (tx) {
      update = {
        tx: tx,
        txId: tx.getId()
      }

      copyDHTKeys(update, task)
      return self._logIt(update)
    })
    .then(function () {
      cb()
      self.emit('chained', task, update)
    })
    .catch(cb)
    .done()

  // if (task.data) {
  //   // create
  //   typeforce({
  //     data: 'Object',
  //     public: '?Boolean',
  //     recipients: '?Array'
  //   }, task)

  //   promise = this.chainwriter.create()
  //     .data(task.data)
  //     .recipients(task.recipients || [])
  //     .setPublic(!!task.public)
  //     .execute()
  // } else {
  //   // share
  //   typeforce({
  //     key: 'Buffer',
  //     encryptionKey: 'Buffer',
  //     shareWith: 'Buffer',
  //     public: '?Boolean'
  //   }, task)

  //   promise = this.chainwriter.share()
  //     .shareAccessTo(task.key, task.encryptionKey)
  //     .shareAccessWith(task.shareWith)
  //     .setPublic(!!task.public)
  //     .execute()
  // }

  // promise.then(function (resp) {
  //   // do we need to append to log,
  //   // or can we just wait till it's incoming on a chainstream?
  //   self.emit('chained', task, resp)
  //   cb()
  // })
  // .catch(cb)
  // .done()
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
    msg = normalizeJSON(JSON.parse(msg))
  } catch (err) {
    return this.emit('warn', 'received message not in JSON format', msg)
  }

  debugger
  this.emit('message', msg, fingerprint)

  this._lookupByFingerprint(fingerprint)
    .then(function (from) {
      debugger
      var identity = results[0]
      var normalized = results[1]
      // normalized.key = results[2]
      normalized.from = identity[ROOT_HASH]
      normalized.to = self._myRootHash
      delete normalized.parsed
      self._logIt(normalized)
    })
    // ,
    // normalizeMsg.bind(null, msg)
    // ,
    // getDHTKey.bind(null, msg)
    // don't know DHT key because we don't know if chained object is encrypted or not
  // ], function (err, results) {
  //   debugger
  //   if (err) {
  //     if (/not found/i.test(err.message)) {
  //       return self.emit('warn', 'ignoring message from unchained identity', fingerprint)
  //     }

  //     throw err // TODO: not this
  //   }

  //   var identity = results[0]
  //   var normalized = results[1]
  //   // normalized.key = results[2]
  //   normalized.from = identity[ROOT_HASH]
  //   normalized.to = self._myRootHash
  //   delete normalized.parsed
  //   self._logIt(normalized)
  // })

  // this.chaindb.keyForFingerprint(fingerprint)
  //   .catch(function (err) {
  //     self.emit('warn', 'ignoring message from unchained identity', err)
  //   })
  //   .done(function (dhtKey) {
  //     if (dhtKey) return

  //     msg.from = dhtKey

  //     parseAndSave()
  //   })

  // function save () {
  //   self.emit('saved', msg, fingerprint)
  //   self._log.append(msg)
  //   self._db.put(getKey(msg), msg)
  // }

  // function parseAndSave (onsuccess) {
  //   if (msg.type !== MessageType.chained) return save()

  //   Parser.parse(msg.data, function (err, parsed) {
  //     if (err) return debug('failed to parse message', err)

  //     utils.getStorageKeyFor(msg.msg, function (err, key) {
  //       if (err) return self.emit('error', err)

  //       msg.key = key
  //       save()
  //     })
  //   })
  // }
}

// Driver.prototype.getMessages = function (theirFingerprint, results) {
//   this._db.createReadStream({
//       start: theirFingerprint,
//       end: theirFingerprint + '\xff'
//     })
//     .pipe(concat(function(results) {
//       cb(null, results)
//     }))
// }

// Driver.prototype._onChainMessage = function (msg, fingerprint) {
//   // var self = this
//   // typeforce({
//   //   txId: 'String',
//   //   key: 'String',
//   //   data: 'Object|Buffer'
//   // })

//   // var txId = json.txId
//   // var key = json.key // dht key
//   // var value = json.value

//   utils.getStorageKeyFor(msg, function (err, key) {
//     if (err) {
//       return self._debug('invalid msg', msg)
//     }

//     var wrapper = {
//       msg: msg,
//       type: MessageType.chained,
//       status: 'pending'
//     }

//     self._msgDB.put(key.toString('hex'), wrapper, rethrow)
//   })


//   // this.chaindb._processChainedObj({
//   //   key: json.key,
//   //   addresses:
//   // })

//   // this.chaindb.byFingerprint(fingerprint)
//   //   .then(function (identity) {
//   //     identity = Identity.fromJSON(identity)
//   //     var parser = new Parser()
//   //     parser.verifyWith(identity.keys({
//   //       type: 'dsa'
//   //     }))
//   //   })

//   // var buf = toBuffer(data) // wasteful
//   // Parser.parse(buf, function (err, obj) {
//   //   if (err) {
//   //     return self.emit('error', 'Failed to parse data msg from peer: ' + err.message)
//   //   }

//   //   self.chaindb.
//   //   // obj.data
//   //   // obj.attachments
//   // })
// }

Driver.prototype.putOnChain = function (obj) {
  var self = this

  typeforce({
    data: 'Buffer'
  }, obj)

  var type = obj.mode === 'public' ? TxData.types.public : TxData.types.permission
  var options = {
    data: obj.data,
    type: type
  }

  var recipients = obj.to
  if (!recipients && obj.to) {
    var to = obj.to
    var pubKey = to.keys({
      type: 'bitcoin',
      networkName: this.networkName
    })[0].pubKeyString()

    recipients = [pubKey]
  }

  copyDHTKeys(options, obj, obj[CUR_HASH])
  recipients.forEach(function (r) {
    var chainOpts = extend({}, options)
    self._getAddress(r)
      .then(function (addr) {
        chainOpts.address = addr
        self.chainwriterq.push(chainOpts)
      })
      .catch(function (err) {
        debugger
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

  this._logIt({
    type: 'text',
    mode: 'private',
    data: msg,
    from: this._myRootHash,
    to: to
  })
}

Driver.prototype.publish = function (obj) {
  var self = this
  var wrapper = {
    type: obj._type,
    mode: 'public',
    from: this._myRootHash
  }

  if (wrapper.type === Identity.TYPE) {
    wrapper.to = [{
      fingerprint: constants.IDENTITY_PUBLISH_ADDRESS
    }]
  }
  else {
    wrapper.to = [{}]
    wrapper.to[0][ROOT_HASH] = this._myRootHash
  }

  var builder = new Builder()
    .data(obj)
    .signWith(this.signingKey)

  Q.ninvoke(builder, 'build')
    .then(function (result) {
      wrapper.data = result.form
      return self.chainwriter.create()
        .data(wrapper.data)
        .setPublic(true)
        .execute()
    })
    .then(function (resp) {
      copyDHTKeys(wrapper, resp.key)
      return self._logIt(wrapper)
    })
    .done()
}

Driver.prototype._lookupBTCKey = function (recipient) {
  var self = this
  return this.lookupIdentity(recipient)
    .then(function (identity) {
      return find(identity.pubkeys, function (k) {
        return k.type === 'bitcoin' && k.networkName === self.networkName
      }).value
    })
}

Driver.prototype.sendStructured = function (options) {
  debugger
  var self = this

  typeforce({
    msg: 'Object',
    to: 'Array'
  }, options)

  var builder = new Builder()
    .data(options.msg)
    .signWith(this.identityKeys)

  var tasks = options.to.map(function (r) {
    return Q.ninvoke(this, '_lookupBTCKey')
  })

  tasks.unshift(Q.ninvoke(builder, 'build'))
  Q.all(tasks)
    .then(function (results) {
      debugger
      var build = results[0]
      var pubKeys = results.slice(1)
      wrapper.data = build.form
      return self.chainwriter.create()
        .data(wrapper.data)
        .recipients(pubKeys)
        .execute()
    })
    .then(function (result) {
      debugger
      var wrapper = {
        type: msg._type,
        mode: 'private',
        to: options.to,
        toPubKeys: result.shares
      }

      copyDHTKeys(wrapper, msg, result.key)
      self._logIt(wrapper)
    })


  // normalizeMsg(options.msg, function (err, normalized) {
  //   if (err) throw err

  //   normalized.from = self._myRootHash
  //   normalized.to = options.to
  //   normalized.mode = 'private'
  //   delete normalized.parsed
  //   self._logIt(normalized)
  // })
}

Driver.prototype.destroy = function (cb) {
  var self = this

  // sync
  this.chainwriter.destroy()

  // async
  // this.keeper.destroy().done(console.log.bind(console, 'keeper dead'))
  // this.p2p.destroy(console.log.bind(console, 'p2p dead')),
  // this.chaindb.destroy(console.log.bind(console, 'chaindb dead')),
  // this.queueDB.close(console.log.bind(console, 'queue dead'))

  parallel([
    function killKeeper(cb) {
      self.keeper.destroy().finally(function () {
        if (cb) cb()
      })
    },
    this.p2p.destroy.bind(this.p2p),
    this.queueDB.close.bind(this.queueDB),
    this._identityDB.close.bind(this.queueDB),
    this._logDB.close.bind(this.queueDB)
  ], cb)
}

Driver.prototype._debug = function () {
  var args = [].slice.call(arguments)
  args.unshift(this.identity.name())
  return debug.apply(null, args)
}

Driver.prototype._getAddress = function (recipient) {
  if (recipient.fingerprint) return Q(recipient.fingerprint)

  if (recipient.pubKey) {
    var addr = bitcoin.ECPubKey.fromHex(recipient.pubKey)
      .getAddress(bitcoin.networks[this.networkName])

    return Q(addr)
  }

  if (recipient[ROOT_HASH]) {
    return this._lookupBTCKey(recipient)
  }

  return Q.reject(new Error('unable to deduce address'))
}

function call (method) {
  return function (obj) {
    return obj[method]()
  }
}

function caller (method) {
  return function (obj) {
    return obj[method].bind(obj)
  }
}

function toBuffer (data) {
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

function rethrow (err) {
  if (err) throw err
}

function validateMsgType (type) {
  for (var name in MessageType) {
    if (MessageType[name] === type) return true
  }

  return false
}

// function toBuffer (obj) {
//   if (Buffer.isBuffer(obj)) return obj
//   if (typeof obj === 'string') return new Buffer(obj, 'binary')
//   if (!obj || typeof obj !== 'object') throw new Error('invalid argument')

//   return new Buffer(utils.stringify(obj), 'binary')
// }

// function getKey (msg) {
//   typeforce({
//     them: 'String',
//     me: 'String',
//     incoming: 'Boolean',
//     type: 'Number',
//     msg: 'Buffer'
//   }, msg)

//   return [
//     msg.them,
//     msg.me,
//     msg.incoming ? '0' : '1',
//     pad(2, msg.type, '0'),
//     crypto.createHash('sha256').update(msg.msg).digest('hex')
//   ].join('!')
// }

function getFingerprint (identity) {
  return find(identity.pubkeys, function (k) {
    return k.type === 'dsa'
  }).fingerprint
}

function keyForFingerprint (identityJSON, fingerprint) {
  var key
  identityJSON.pubkeys.some(function (k) {
    if (k.fingerprint === fingerprint) {
      key = k
      return true
    }
  })

  return key && toKey(key)
}

// function isIdentityTx (txInfo) {
//   return txInfo.tx.outs.some(function (out) {
//     return utils.getAddressFromOutput(out, networkName) === constants.IDENTITY_PUBLISH_ADDRESS
//   })
// }

function copyDHTKeys (dest, src, curHash) {
  if (typeof curHash === 'undefined') {
    curHash = typeof src === 'string' ? src : src[CUR_HASH] || src[ROOT_HASH]
    src = dest
  }

  dest[ROOT_HASH] = src[ROOT_HASH] || curHash
  dest[CUR_HASH] = curHash
}
