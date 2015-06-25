
var crypto = require('crypto')
var util = require('util')
var EventEmitter = require('events').EventEmitter
var typeforce = require('typeforce')
var pad = require('pad')
var debug = require('debug')('chained-chat')
var sublevel = require('level-sublevel')
var levelup = require('levelup')
var levelQuery = require('level-queryengine')
var jsonqueryEngine = require('jsonquery-engine')
var changesFeed = require('changes-feed')
var Queue = require('level-jobs')
var bufferEqual = require('buffer-equal')
var extend = require('extend')
var omit = require('object.omit')
var utils = require('tradle-utils')
var parallel = require('run-parallel')
var concat = require('concat-stream')
var mapStream = require('map-stream')
var safe = require('safecb')
var typeforce = require('typeforce')
var Parser = require('chained-obj').Parser
var normalize = require('./normalizeJSON')
var chainstream = require('./chainstream')
var OneBlock = require('./oneblock')
var PREFIX = 'tradle'
var INDICES = [
  'key',
  'tx',
  'them',
  'me',
  'incoming',
  'status',
  'type'
]

var normalizeStream = mapStream.bind(function (data, cb) {
  cb(null, normalize(data))
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
    identityPriv: 'Object', // maybe allow read-only mode if this is missing
    identityPub: 'Object',
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

  // this.identity = Identity.fromJSON(identityPriv)

  var networkName = this.networkName
  var keeper = this.keeper
  var dht = this.dht
  var identity = this.identity
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
    key: identity.keys({
      type: 'dsa'
    })[0].priv()
  })

  this.p2p.on('data', this._onmessage)

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
    lookup: function byFingerprint (fingerprint, cb) {
      self.lookupIdentity({
        private: true,
        query: {
          fingerprint: fingerprint
        }
      }, function (err, identity) {
        return {
          key: identity.keys({ fingerprint: fingerprint })[0],
          identity: identity
        }
      })
    }
  })

  this._setupTxStream(this._setupInputStreams)
  this._setupStorage()
}

Driver.prototype._setupTxStream = function (cb) {
  var self = this

  this._lastBlock = new OneBlock({
    path: this._prefix('lastBlock.db')
  }, function (err) {
    if (err) return cb(err)

    self.rawtxstream = cbstreams.stream.txs({
      live: true,
      interval: self.syncInterval || 60000,
      api: blockchain,
      height: self._lastBlock.height(),
      addresses: [
        this.wallet.addressString,
        constants.IDENTITY_PUBLISH_ADDRESS
      ]
    })

    cb()
  })
}

Driver.prototype._setupStorage = function (cb) {
  // this.p2p.once('ready', this.init)

  // this.p2p.on('connect', function (fingerprint, addr) {
  //   self.emit('connect', fingerprint, addr)
  // })


  var logDB = levelup(this._prefix('msg-log.db'), {
    db: this.leveldown,
    valueEncoding: 'json'
  })

  this._log = changesFeed(logDB)
  this._logStream = this._log.createReadStream({ live: true, keys: false })

  // this._db = levelQuery(levelup(this._prefix('msgs.db'), {
  //   db: this.leveldown,
  //   valueEncoding: 'json'
  // }))

  // this._db.query.use(jsonqueryEngine())
  // INDICES.forEach(function (i) {
  //   this._db.ensureIndex(i)
  // }, this)

  this._logStream.on('data', function(data) {
    var val = data.value
    if (val.type === Identity.TYPE) {
      self._updateIdentity(val)
    }

    if (val.tx) {
      self._lastBlock.push(pick(val, ['tx', 'height']))
    }
  })

  this._identityDB = levelQuery(levelup(this._prefix('identities.db'), {
    db: this.leveldown,
    valueEncoding: 'json'
  }))

  this._identityDB.query.use(jsonqueryEngine())

  var iddb = sublevel(this._identityDB)
  this._iByDHTKey = iddb.sublevel('byDHTKey')
  this._iDHTKeyByFingerprint = iddb.sublevel('byFingerprint')

  // this.chainstreams.identity.on('data', this._updateIdentity)
  this.chainstreams.public.on('data', this._logIt)
  this.chainstreams.private.on('data', this._logIt)

  // TODO: record last block height
  // this.chainstream.public.on('data', function (chainedObj) {
  //   var height = chainedObj.tx.height
  // })

  this.chaindb.on('saved', function (chainedObj) {
    console.log('saved', chainedObj.key)

    if (!chainedObj.from) return

    var from = Identity.fromJSON(chainedObj.from)
    var fromKeys = from.keys()
    var isFromMe = self.identity.keys().some(function (k1) {
      return fromKeys.some(function (k2) {
        return k2.equals(k1)
      })
    })

    // var from = Identity.fromJSON(chainedObj.from)
    // var isFromMe = self.identity.keys({ type: 'bitcoin' })
    //   .some(function (k) {
    //     var f = k.fingerprint()
    //     return from.keys({ fingerprint: f }).length
    //   })

    var fromFingerprint = getFingerprint(from)
    var to = chainedObj.to && Identity.fromJSON(chainedObj.to)
    var toFingerPrint = to && getFingerprint(to)
    var tasks = [
      self.chaindb.keyForFingerprint(fromFingerprint)
    ]

    if (toFingerprint) {
      tasks.push(self.chaindb.keyForFingerprint(toFingerprint))
    }

    Q.allSettled(tasks)
      .spread(function (fromIdKey, toIdKey) {

      })

    self.chaindb.keyForFingerprint(fromFingerprint)
      .then(function (theirDHTKey) {
        var key = getKey({
          msg: chainedObj.data,
          type: MessageType.chained,
          them: theirDHTKey,
          incoming: theirFingerprint !== myFingerprint
        })
      })

    // var myFingerprint = self.p2p.fingerprint

    self._db.get(key, function (err, saved) {
      if (err) return

      var savedObj = normalize(saved.msg)
      if (!bufferEqual(savedObj, chainedObj.data)) return

      self.emit('resolved', chainedObj)
      saved.status = 'resolved'

      self._log.append({
        msgKey: key,
        status: saved.status
      })

      self._msgs.put(key, saved)
    })
  })
}

Driver.prototype._updateIdentity = function (identity, cb) {
  var self = this
  assert(identity._r, 'expected identity root hash')
  assert(identity._c, 'expected identity current hash')
  self._iByDHTKey.get(identity._r, function (err, stored) {
    if (err) {
      if (!/not found/i.test(err.message)) {
        return cb(err)
      }
    }
  })

  if (stored) {
    stored.history.push(identity)
  } else {
    stored = {
      history: [identity]
    }
  }

  self._iByDHTKey.put(identity._r, stored)

  var fingerprintBatch = Identity.fromJSON(identity).keys().map(function (k) {
    return { type: 'put', key: k.fingerprint(), value: identity._r }
  })

  self._iDHTKeyByFingerprint.batch(fingerprintBatch, cb)
}

Driver.prototype._setupInputStreams = function () {
  // var txstream = this.rawtxstream.pipe(through2.obj(function (txInfo, enc, done) {
  //   var wrapper = {
  //     type: 'tx',
  //     processed: false,
  //     data: txInfo
  //   }

  //   self._log.append(wrapper, function (err) {
  //     if (err) return done()

  //     txstream.push(txInfo)
  //     done()
  //   })
  // }))

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

  txstream.pipe(publicObjStream)
  txstream.pipe(privateObjStream)

  this.chainstreams = {
    public: publicObjStream,
    private: privateObjStream,
    identity: identityStream
  }
}

Driver.prototype.lookupByDHTKey = function (key, cb) {
  return this._db.query({ key: key })
    .once('data', function (data) {
      cb(null, data.value)
    })
}

Driver.prototype.lookupIdentity = function (options, cb) {
  typeforce({
    query: 'Object',
    private: '?Boolean'
  }, options)

  var done = false
  var me = options.private ? this.identityPriv : this.identityPub
  var query = options.query
  assert(query.fingerprint || query.key)

  cb = safe(cb)
  if (query.fingerprint) {
    if (found(me)) return
  }

  this._identityDB.query(query)
    .on('data', function (data) {
      found(data.value)
    })
    .once('error', cb)
    .once('end', function () {
      cb() // no results
    })

  function found (identity) {
    if (done) return
    if (!(identity instanceof Identity)) identity = Identity.fromJSON(identity)

    identity = me.equals(identity) ? me : identity
    var key = identity.keys({ fingerprint: fingerprint })[0]
    if (key) {
      cb(null, {
        key: key,
        identity: identity
      })

      done = true
      return true
    }
  }
}

Driver.prototype._logIt = function (obj) {
  obj.type = obj.parsed && obj.parsed.data._type
  this._log.append(omit(obj, 'parsed'))
}

Driver.prototype._lookup = function (fingerprint, cb) {
  var key = this.identity && this.identity.keys({ fingerprint: fingerprint })[0]
  if (key) {
    return cb(null, {
      key: key,
      identity: this.identity
    })
  }

  this.byFingerprint(fingerprint)
    .then(function (identityJSON) {
      // TODO: optimize this, probably not necessary
      var identity = Identity.fromJSON(identityJSON)
      cb(null, {
        key: identity.keys({ fingerprint: fingerprint })[0],
        identity: identityJSON
      })
    })
    .catch(cb)
    .done()
}

// Driver.prototype.init = function () {
//   var self = this
//   var chainwriter = this.chainwriter
//   if (this._initialized) return

//   this._initialized = true


//   // this.chaindb.on('invalid', function (txId, chainedObj) {
//   //   self._msgDB.get(chainedObj.key, function (err, saved) {
//   //     if (err) return

//   //     saved.invalid = true
//   //     self._invalidDB.put(chainedObj)
//   //     self._msgDB.del(chainedObj.key)
//   //   })
//   // })

//   this.emit('ready')
// }

Driver.prototype._chainWriterWorker = function (task, cb) {
  var self = this

  task = normalize(task)
  var promise

  if (task.data) {
    // create
    typeforce({
      data: 'Object',
      public: '?Boolean',
      recipients: '?Array'
    }, task)

    promise = this.chainwriter.create()
      .data(task.data)
      .recipients(task.recipients || [])
      .setPublic(!!task.public)
      .execute()
  } else {
    // share
    typeforce({
      key: 'Buffer',
      encryptionKey: 'Buffer',
      shareWith: 'Buffer',
      public: '?Boolean'
    }, task)

    promise = this.chainwriter.share()
      .shareAccessTo(task.key, task.encryptionKey)
      .shareAccessWith(task.shareWith)
      .setPublic(!!task.public)
      .execute()
  }

  promise.then(function (resp) {
    // do we need to append to log,
    // or can we just wait till it's incoming on a chainstream?
    self.emit('chained', task, resp)
    cb()
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

  msg = bufToMsg(msg)

  if (!validateMsgType(msg.type)) {
    this.emit('warn', 'unknown message type', msg)
    return
  }

  // emit a copy
  this.emit('message', extend(true, {}, msg), fingerprint)

  msg.status = msg.type === MessageType.chained ? 'pending' : 'resolved'
  msg.theirFingerprint = fingerprint
  msg.incoming = true

  this.chaindb.keyForFingerprint(fingerprint)
    .catch(function (err) {
      self.emit('warn', 'ignoring message from unchained identity', err)
    })
    .done(function (dhtKey) {
      if (dhtKey) return

      msg.from = dhtKey

      parseAndSave()
    })

  function save () {
    self.emit('saved', msg, fingerprint)
    self._log.append(msg)
    self._db.put(getKey(msg), msg)
  }

  function parseAndSave (onsuccess) {
    if (msg.type !== MessageType.chained) return save()

    Parser.parse(msg.data, function (err, parsed) {
      if (err) return debug('failed to parse message', err)

      utils.getStorageKeyFor(msg.msg, function (err, key) {
        if (err) return self.emit('error', err)

        msg.key = key
        save()
      })
    })
  }
}

Driver.prototype.createReadStream = function (options) {
  return this._db.createReadStream(options)
}

Driver.prototype.getMessages = function (theirFingerprint, results) {
  this._db.createReadStream({
      start: theirFingerprint,
      end: theirFingerprint + '\xff'
    })
    .pipe(concat(function(results) {
      cb(null, results)
    }))
}

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

Driver.prototype._sendMsg = function (options) {
  var self = this

  typeforce({
    msg: 'Object',
    to: 'Identity'
  }, options)

  var to = options.to
  var theirFingerprint = getFingerprint(to)
  var msg = {
    type: options.chain ? MessageType.chained : MessageType.plaintext,
    msg: options.msg
  }

  this.lookupIdentity({
    query: {
      fingerprint: theirFingerprint
    }
  }, function (err, identityHash) {
    // self._db.put(extend({
    //   from: this.identityHash,
    //   to: identityHash,
    //   incoming: false
    // }, msg))
  })

  // this._log.append({
  //   msgKey: getKey(extend({
  //     to: theirFingerprint
  //   }, msg))
  // })

  var msgBuf = msgToBuf(msg)
  var to = options.to

  this.p2p.send(msgBuf, theirFingerprint)
}

Driver.prototype.send = function (options) {
  var self = this

  typeforce({
    msg: 'Buffer',
    to: 'Identity',
    chain: '?Object'
  }, options)

  this._sendMsg(options)
  var chainOpts = options.chain
  if (!chainOpts) return

  // setTimeout(function () {
    var to = options.to
    var msg = options.msg
    var pubKey = to.keys({
      type: 'bitcoin',
      networkName: self.networkName
    })[0].pubKeyString()

    self.chainwriterq.push({
      data: msg,
      public: chainOpts.public,
      recipients: [pubKey]
    })
  // }, 5000)
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
    function killChainDB(cb) {
      self.chaindb.destroy().finally(function() {
        cb()
      })
    },
    this.p2p.destroy.bind(this.p2p),
    this.queueDB.close.bind(this.queueDB)
  ], cb)
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
  return new Buffer(utils.stringify(data), 'binary')
}

function msgToBuf (msg) {
  var contents = msg.msg
  var buf = new Buffer(1 + contents.length)
  buf[0] = msg.type
  contents.copy(buf, 1, 0, contents.length)
  return buf
}

function bufToMsg (buf) {
  return {
    type: buf[0],
    data: buf.slice(1)
  }
}

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

function getKey (msg) {
  typeforce({
    them: 'String',
    me: 'String',
    incoming: 'Boolean',
    type: 'Number',
    msg: 'Buffer'
  }, msg)

  return [
    msg.them,
    msg.me,
    msg.incoming ? '0' : '1',
    pad(2, msg.type, '0'),
    crypto.createHash('sha256').update(msg.msg).digest('hex')
  ].join('!')
}

function getFingerprint (identity) {
  return identity.keys({ type: 'dsa' })[0]
    .fingerprint()
}

// function isIdentityTx (txInfo) {
//   return txInfo.tx.outs.some(function (out) {
//     return utils.getAddressFromOutput(out, networkName) === constants.IDENTITY_PUBLISH_ADDRESS
//   })
// }
