
var assert = require('assert')
var crypto = require('crypto')
var util = require('util')
var EventEmitter = require('events').EventEmitter
var Writable = require('readable-stream').Writable
var Q = require('q')
var typeforce = require('typeforce')
var debug = require('debug')('chained-chat')
var sublevel = require('level-sublevel')
var levelup = require('levelup')
// var levelQuery = require('level-queryengine')
// var jsonqueryEngine = require('jsonquery-engine')
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
var constants = require('tradle-constants')
var TYPE = constants.TYPE
var ROOT_HASH = constants.ROOT_HASH
var CUR_HASH = constants.CUR_HASH
var PREFIX = constants.OP_RETURN_PREFIX
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
    lookup: this._lookupByFingerprint2
  })

  this._fingerprintToIdentity = {} // in-memory cache of recent conversants
  this._setupP2P()
  this._setupLog()
  this._logRS.on('error', this._onLogReadError)
  this._logWS.on('error', this._onLogWriteError)

  var tasks = [
    this._prepIdentity(),
    this._setupTxStream()
  ]

  Q.all(tasks)
    .done(function () {
      self.emit('ready')
    })
}

Driver.prototype._onLogReadError = function (err) {
  debugger
  throw err
}

Driver.prototype._onLogWriteError = function (err) {
  debugger
  throw err
}

// Driver.prototype._processIncoming = function (obj) {
//   debugger
//   var self = this

//   assert(ROOT_HASH in obj, 'expected ' + ROOT_HASH)
//   var query = pick(obj, ROOT_HASH)
//   var stream = this._logDB.createValueStream({
//       reverse: true
//     })
//     .on('error', rethrow) // TODO: handle
//     .on('data', function (saved) {
//       if (!saved.data) return

//       saved = normalizeJSON(saved)
//       stream.destroy()
//       if (!bufferEqual(saved.data, obj.data)) return

//       self.emit('saved', obj)
//       self.emit('resolved', obj)
//       saved.status = 'resolved'

//       var update = pick(obj, ['tx', ROOT_HASH, CUR_HASH])
//       update.status = 'resolved'
//       self._log.append(update)
//     })
// }

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
    cbstreams.stream.txs({
        live: true,
        interval: self.syncInterval || 60000,
        api: self.blockchain,
        height: self._lastBlock.height(),
        addresses: [
          self.wallet.addressString,
          constants.IDENTITY_PUBLISH_ADDRESS
        ]
      })
      .pipe(mapStream(function (txInfo, done) {
        var entry = extend(newLogWrapper(['tx']), txInfo)
        entry.tx = entry.tx.toBuffer()
        done(null, entry)
      }))
      .pipe(self._logWS)


    self._txStream
      .pipe(toValueStream())
      .pipe(self._lastBlock)

    defer.resolve()
  })

  return defer.promise
}

Driver.prototype._setupLog = function () {
  var self = this

  this._logDB = levelup(this._prefix('msg-log.db'), {
    db: this.leveldown,
    valueEncoding: 'json'
  })

  this._log = changesFeed(this._logDB)

  this._logWS = new Writable({ objectMode: true })
  this._logWS._write = function (chunk, enc, next) {
    self._log.append(chunk)
    next()
  }

  this._logRS = this._log
    .createReadStream({ live: true })
    .pipe(mapStream(function (data, cb) {
      var id = data.change
      data = normalizeJSON(data.value)
      // so whoever processes this can refer to this entry
      data._l.id = id
      cb(null, data)
    }))

  this._logRS.setMaxListeners(0)

  // this._log
  //   .createReadStream({ live: true })
  //   .pipe(mapStream(function (data, cb) {
  //     cb(null, utils.stringify(data))
  //   }))
  //   .pipe(process.stdout)

  // this._logStream = duplexify(ws, rs)

//   this._logRS.pipe(mapStream(function (data, cb) {
//     debugger
//     cb()
//   }))

  this._txStream = this._logRS.pipe(logFilter('tx'))
    .pipe(mapStream(function (data, cb) {
      data.tx = bitcoin.Transaction.fromBuffer(data.tx)
      cb(null, data)
    }))

  var inbound = this._logRS.pipe(logFilter('inbound'))
  inbound.on('data', this._onInboundMessage)
  // this._newInboundPlain = this._logRS.pipe(logFilter('plain', 'inbound'))
  // this._newPublicStruct = this._logRS.pipe(logFilter('struct', 'public'))
  // this._

  var inboundStruct = this._logRS.pipe(logFilter('struct', 'inbound'))
  inboundStruct.on('data', this._onInboundStruct)

  var newOutboundPlain = this._logRS.pipe(logFilter('new', 'plain', 'outbound'))
  newOutboundPlain.on('data', this._onNewOutboundPlainMessage)

  var newOutboundStruct = this._logRS.pipe(logFilter('new', 'struct', 'outbound'))
  newOutboundStruct.on('data', this._onNewOutboundStructMessage)

  var storedOutboundStruct = this._logRS.pipe(logFilter('struct', 'outbound', 'stored'))
  storedOutboundStruct.on('data', this._onStoredOutboundStructMessage)

  // this._newOutbound.on('data', this._onNewOutboundMessage)

  // this._newPlain = this._logRS.pipe(logFilter('new', 'plain'))

  // this._newPlainInbound = this._newPlain.pipe(logFilter('inbound'))
  // this._newPlainInbound.on('data', this._onInboundPlain)

  // this._newStructured = this._logRS.pipe(logFilter('new', 'struct'))
  var structStored = this._logRS.pipe(logFilter('struct', 'stored'))
  var structChained = this._logRS.pipe(logFilter('struct', 'chained'))
  structChained.on('data', this._onStructChained)

  this._addressBook = identityStore({
    path: this._prefix('identities.db'),
    leveldown: this.leveldown
  })

  var objStream = chainstream({
    chainloader: this.chainloader,
    lookup: this.lookupByDHTKey
  })

  this._txStream
    .pipe(objStream)
    .pipe(mapStream(function (chainedObj, cb) {
      // how do we know prev?
      // necessary? maybe just write this straight to log
      var entry = extend(
        pick(chainedObj, ['height', 'data']),
        entry,
        newLogWrapper([
          'struct',
          'chained',
          chainedObj.type === 'public' ? 'public' : 'private'
        ], chainedObj)
      )

      var from = chainedObj.from.getOriginalJSON()
      var to = chainedObj.to && chainedObj.to.getOriginalJSON()
      entry.from = from[ROOT_HASH]
      entry.tx = chainedObj.tx.toBuffer()
      entry[CUR_HASH] = chainedObj.key
      entry[ROOT_HASH] = chainedObj.parsed.data[ROOT_HASH] || entry[CUR_HASH]
      entry[TYPE] = chainedObj.parsed.data[TYPE]

      if (to) entry.to = to[ROOT_HASH]

      self._debug('picked up object from chain', entry)
      cb(null, entry)
    }))
    .pipe(this._logWS)
}

Driver.prototype._onInboundMessage = function (data) {
  this.emit('message', data)
}

Driver.prototype._onInboundStruct = function (data) {
  debugger
  this.emit('message', data)
}

// Driver.prototype._onPub = function (data) {
//   debugger
// }

Driver.prototype._onStructChained = function (data) {
  var self = this
  if (!data.data) return

  this.emit('chained', data)
  Parser.parse(data.data, function (err, parsed) {
    if (err) return self.emit('error', 'stored invalid struct', err)

    if (parsed.data[TYPE] === Identity.TYPE) {
      // what about attachments?
      copyDHTKeys(parsed.data, data)
      self._addressBook.update(parsed.data)
    }
  })
}

Driver.prototype._onNewOutboundStructMessage = function (obj) {
  var self = this
  var isPublic = hasTag(obj, 'public')
  var builder = new Builder()
    .data(obj.data)

  if (obj.attachments) {
    obj.attachments.forEach(builder.attach, builder)
  }

  if (obj.sign) {
    builder.signWith(this.signingKey)
  }

  var lookup = isPublic ? this._lookupBTCAddress : this._lookupBTCPubKey
  var tasks = [
    Q.ninvoke(builder, 'build')
  ].concat(obj.to.map(lookup))

  var wrapper = newLogWrapper([
    'struct',
    'stored',
    'outbound',
    getPrivacyTag(obj)
  ], obj) // obj is prev
  wrapper.chain = obj.chain // whether to put it on chain
  wrapper.to = obj.to
  wrapper.sign = obj.sign
  return Q.all(tasks)
    .then(function (results) {
      var build = results[0]
      var recipients = results.slice(1)
      wrapper.data = build.form
      return self.chainwriter.create()
        .data(wrapper.data)
        .setPublic(isPublic)
        .recipients(recipients)
        .execute()
    })
    .then(function (resp) {
      copyDHTKeys(wrapper, resp.key)
      return self._logIt(wrapper)
    })
}

Driver.prototype._sendP2P = function (obj) {
  // TODO:
  //   do we log that we sent it?
  //   do we log when we delivered it? How do we know it was delivered?
  var self = this
  var msg = toBuffer({
    data: obj.data,
    type: getMsgTypeTag(obj)
  })

  return Q.allSettled(obj.to.map(this.lookupIdentity))
    .then(function (results) {
      var found = results.filter(function (r) {
          return r.state === 'fulfilled'
        })
        .map(function (r) {
          return r.value
        })

      if (found.length !== results.length) {
        var missing = results.map(function (r, i) {
          return r.state === 'fulfilled' ? null : obj.to[i]
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

Driver.prototype._onStoredOutboundStructMessage = function (obj) {
  if (obj.chain) this.putOnChain(obj)
  if (hasTag(obj, 'public')) return

  this._sendP2P(obj)
}

Driver.prototype._onNewOutboundPlainMessage = function (data) {
  this._sendP2P(data)
}

// Driver.prototype._onInboundPlaintext = function (data) {
// }

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


// Driver.prototype._setupInputStreams = function () {
//   var self = this
//   var objStream = chainstream({
//     name: this.pathPrefix,
//     chainloader: this.chainloader,
//     lookup: this.lookupByDHTKey
//   })

//   // var publicObjStream = objStream.pipe(mapStream(function (obj, cb) {
//   //   if (obj.type === 'public') cb(null, obj)
//   //   else cb()
//   // }))

//   // var identityStream = publicObjStream.pipe(mapStream(function (obj, cb) {
//   //   if (obj.parsed.data._type === Identity.TYPE) {
//   //     cb(null, obj)
//   //   } else {
//   //     cb()
//   //   }
//   // }))

//   // var privateObjStream = objStream.pipe(mapStream(function (obj, cb) {
//   //   if (obj.type !== 'public') cb(null, obj)
//   //   else cb()
//   // }))

// }

Driver.prototype._lookupByFingerprint = function (fingerprint, private) {
  var self = this
  return this.lookupIdentity({
      fingerprint: fingerprint
    })
    .then(function (identity) {
      var key = private && self.getPrivateKey(fingerprint)
      key = key || keyForFingerprint(identity, fingerprint)
      return {
        key: key,
        identity: identity
      }
    })
}

Driver.prototype._lookupByFingerprint2 = function (fingerprint, private) {
  return this._lookupByFingerprint.apply(this, arguments)
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

Driver.prototype.getPrivateKey = function (fingerprint) {
  return find(this.identityKeys, function (k) {
    return k.fingerprint === fingerprint
  })
}

Driver.prototype.lookupIdentity = function (query) {
  if (query.fingerprint === '80dd7c0eae1faa1531710ba9fa1b759ae670846b') debugger

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
  }
  else if (query.fingerprint) {
    var pub = this.getPublicKey(query.fingerprint)
    if (pub) {
      return Q.resolve(me)
    }
  }

  return Q.ninvoke(this._addressBook, 'query', query)
    .then(function (results) {
      return results[0]
    })
    .catch(function (err) {
      self._debug('unable to find identity', query, err)
      throw err
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
  return Q.ninvoke(this._log, 'append', obj)
}

// Driver.prototype._logIt = function (obj) {
//   if (obj.tx) {
//     typeforce({
//       id: 'String',
//       body: 'Object'
//     }, obj.tx)
//   } else {
//     typeforce({
//       from: 'String',
//       to: 'Array'
//     }, obj)
//   }

//   var parsed = obj.parsed
//   var type = obj.type || (parsed && parsed.data[TYPE])
//   if (type !== 'text') {
//     assert(obj[ROOT_HASH], 'expected root hash')
//     assert(obj[CUR_HASH], 'expected current hash')
//   }

//   var copy = omit(obj, 'parsed')
//   if (type) copy.type = type

//   return Q.ninvoke(this._log, 'append', copy)
// }

Driver.prototype._chainWriterWorker = function (task, cb) {
  var self = this

  task = normalizeJSON(task)
  // var promise

  var update = {
    _l: extend(true, {}, task._l),
    tags: ['struct', 'chained']
  }

  return this.chainwriter.chain()
    .type(task.type)
    .data(task.data)
    .address(task.address)
    .execute()
    .then(function (tx) {
      // ugly!
      update.tx = tx.toBuffer()

      copyDHTKeys(update, task)
      return self._logIt(update)
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
    msg = normalizeJSON(JSON.parse(msg))
  } catch (err) {
    return this.emit('warn', 'received message not in JSON format', msg)
  }

  this._debug('received msg', msg)
  var tags = ['inbound']
  var msgTypeTag
  if (msg.type !== 'plain' && msg.type !== 'struct') {
    this.emit('warn', 'unexpected message type: ' + msg.type)
  } else {
    tags.push(msg.type)
  }

  msg = extend({
    data: msg.data,
    to: this._myRootHash
  }, newLogWrapper(tags))

  this.lookupIdentity({ fingerprint: fingerprint })
    .then(function (from) {
      msg.from = from[ROOT_HASH]
    })
    .catch(function (err) {
      self._debug('failed to find identity by fingerprint', fingerprint, err)
    })
    .finally(function () {
      return self._logIt(msg)
    })
    .done()

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
  assert(ROOT_HASH in obj && CUR_HASH in obj)

  var isPublic = hasTag(obj, 'public')
  var type = isPublic ? TxData.types.public : TxData.types.permission
  var options = extend({
    data: obj[CUR_HASH],
    type: type
  }, newLogWrapper([], obj))

  var recipients = obj.to
  if (!recipients && obj.to) {
    var to = obj.to
    var pubKey = to.keys({
      type: 'bitcoin',
      networkName: this.networkName
    })[0].pubKeyString()

    recipients = [pubKey]
  }

  // TODO: do we really need a separate queue/log for this?
  copyDHTKeys(options, obj, obj[CUR_HASH])
  recipients.forEach(function (r) {
    var chainOpts = extend(true, {}, options) // copy
    self._lookupBTCAddress(r)
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

  validateRecipients(to)
  this._logIt(extend({
    data: msg,
    from: this._myRootHash,
    to: to
  }, newLogWrapper(['new', 'plain', 'outbound'])))
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

  var wrapper = extend(
    {
      type: obj[TYPE],
      data: obj,
      from: this._myRootHash
    },
    pick(options, ['to', 'attachments', 'sign', 'chain']),
    newLogWrapper([
      'new',
      'struct',
      'outbound',
      isPublic ? 'public' : 'private'
    ])
  )

  var to
  if (isPublic) {
    if (wrapper.type === Identity.TYPE) {
      to = [{
        fingerprint: constants.IDENTITY_PUBLISH_ADDRESS
      }]
    } else {
      var me = {}
      me[ROOT_HASH] = this._myRootHash
      wrapper.to = [me]
    }
  } else {
    to = options.to
  }

  validateRecipients(to)
  wrapper.to = to
  this._logIt(wrapper)
}

Driver.prototype._lookupBTCKey = function (recipient) {
  var self = this
  return this.lookupIdentity(recipient)
    .then(function (identity) {
      return find(identity.pubkeys, function (k) {
        return k.type === 'bitcoin' && k.networkName === self.networkName
      })
    })
}

Driver.prototype._lookupBTCPubKey = function (recipient) {
  return this._lookupBTCKey(recipient).then(function(k) {
    return k.value
  })
}

Driver.prototype._lookupBTCAddress = function (recipient) {
  if (recipient.fingerprint === constants.IDENTITY_PUBLISH_ADDRESS) {
    return Q.resolve(recipient.fingerprint)
  }

  return this._lookupBTCKey(recipient).then(function(k) {
    return k.fingerprint
  })
}

// Driver.prototype.sendStructured = function (options) {
//   debugger
//   var self = this

//   typeforce({
//     msg: 'Object',
//     to: 'Array',
//     sign: '?Boolean'
//   }, options)

//   var tasks = options.to.map(function (r) {
//     return Q.ninvoke(this, '_lookupBTCKey')
//   })

//   tasks.unshift(Q.ninvoke(builder, 'build'))
//   Q.all(tasks)
//     .then(function (results) {
//       debugger
//       var build = results[0]
//       var pubKeys = results.slice(1)
//       wrapper.data = build.form
//       return self.chainwriter.create()
//         .data(wrapper.data)
//         .recipients(pubKeys)
//         .execute()
//     })
//     .then(function (result) {
//       debugger
//       var wrapper = {
//         type: msg[TYPE],
//         mode: 'private',
//         to: options.to,
//         toPubKeys: result.shares
//       }

//       copyDHTKeys(wrapper, msg, result.key)
//       self._logIt(wrapper)
//     })


//   // normalizeMsg(options.msg, function (err, normalized) {
//   //   if (err) throw err

//   //   normalized.from = self._myRootHash
//   //   normalized.to = options.to
//   //   normalized.mode = 'private'
//   //   delete normalized.parsed
//   //   self._logIt(normalized)
//   // })
// }

Driver.prototype.destroy = function () {

  // sync
  this.chainwriter.destroy()

  // async
  return Q.all([
    this.keeper.destroy(),
    Q.ninvoke(this.p2p, 'destroy'),
    Q.ninvoke(this.queueDB, 'close'),
    Q.ninvoke(this._addressBook, 'close'),
    Q.ninvoke(this._logDB, 'close')
  ])
}

Driver.prototype._debug = function () {
  var args = [].slice.call(arguments)
  args.unshift(this.identity.name())
  return debug.apply(null, args)
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

function rethrow (err) {
  if (err) throw err
}

// function validateMsgType (type) {
//   for (var name in MessageType) {
//     if (MessageType[name] === type) return true
//   }

//   return false
// }

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
    curHash = typeof src === 'string' ? src : src[CUR_HASH] || src[ROOT_HASH]
    src = dest
  }

  dest[ROOT_HASH] = src[ROOT_HASH] || curHash
  dest[CUR_HASH] = curHash
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

  return mapStream(function (data, cb) {
    var matches = tags.every(hasTag.bind(null, data))
    if (matches) return cb(null, data)
    else cb()
  })
}

function hasTag (obj, tag) {
  return obj._l.tags.indexOf(tag) !== -1
}

function getPrivacyTag (obj) {
  if (hasTag(obj, 'public')) return 'public'
  if (hasTag(obj, 'private')) return 'private'

  throw new Error('no privacy tag')
}

function getMsgTypeTag (obj) {
  if (hasTag(obj, 'plain')) return 'plain'
  if (hasTag(obj, 'struct')) return 'struct'

  throw new Error('no type tag')
}

function notNull (o) {
  return !!o
}

function toValueStream () {
  return mapStream(function (data, cb) {
    cb(null, data.value)
  })
}

function newLogWrapper (tags, prev) {
  var wrapper = {
    _l: {
      tags: tags
    }
  }

  if (prev) {
    wrapper._l.prev = prev._l && prev._l.id
  }

  return wrapper
}
