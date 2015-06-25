
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
var pick = require('object.pick')
var utils = require('tradle-utils')
var parallel = require('run-parallel')
var concat = require('concat-stream')
var mapStream = require('map-stream')
var safe = require('safecb')
var typeforce = require('typeforce')
var ChainedObj = require('chained-obj')
var Parser = ChainedObj.Parser
var Builder = ChainedObj.Builder
var normalizeJSON = require('./normalizeJSON')
var chainstream = require('./chainstream')
var OneBlock = require('./oneblock')
var identityStore = require('./identityStore')
var mi = require('midentity')
var toKey = mi.toKey
var constants = require('./constants')
var ROOT_HASH = constants.rootHash
var CUR_HASH = constants.currentHash
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
    identityHash: '?String',
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
    key: identity.keys({
      type: 'dsa'
    })[0].priv()
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

  var tasks = []
  if (!this.identityHash) {
    tasks.push(this._prepIdentity)
  }

  tasks.push(this._setupTxStream)
  tasks.push(this._setupStorage)

  parallel(tasks, function (err) {
    if (err) return cb(err)

    self._setupInputStreams(function (err) {
      if (err) return cb(err)

      self._logStream.on('data', self._onLogged)
      self.emit('ready')
    })
  })
}

Driver.prototype._onLogged = function (data) {
  var self = this
  var val = data.value
  if (val.type === Identity.TYPE) {
    this._addressBook.update(val)
  }

  if (val.tx) {
    this._lastBlock.push(pick(val, ['tx', 'height']))
  }

  if (val.from === this.identityHash) {
    var query = {}
    query[ROOT_HASH] = options.to
    this.lookupIdentity({
      query: query
    }, function (err, identity) {
      identity = identity.history.pop()
      self.p2p.send(val.data, getFingerprint(identity))
    })
  }
}

Driver.prototype._prepIdentity = function (cb) {
  var self = this

  getDHTKey(this.identityJSON, function (err, key) {
    if (err) return cb(err)

    self.identityHash = key
    cb()
  })
}

Driver.prototype._setupPDP = function () {
  var self = this

  this.p2p.on('data', this._onmessage)
  this.p2p.on('connect', function (fingerprint, addr) {
    // var identity = self._fingerprintToIdentity[fingerprint]
    // if (identity) return self.emit('connect', identity, addr)

    // self.identityByFingerprint
  })
}

Driver.prototype._setupTxStream = function (cb) {
  var self = this

  this._lastBlock = new OneBlock({
    path: this._prefix('lastBlock.db'),
    leveldown: this.leveldown
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
  this._logDB = levelup(this._prefix('msg-log.db'), {
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


  this._addressBook = identityStore({
    path: this._prefix('identities.db'),
    leveldown: this.leveldown
  })

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
          // type: MessageType.chained,
          them: theirDHTKey,
          incoming: theirFingerprint !== myFingerprint
        })
      })
      .done()

    // var myFingerprint = self.p2p.fingerprint

    self._db.get(key, function (err, saved) {
      if (err) return

      var savedObj = normalizeJSON(saved.msg)
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
}

Driver.prototype._lookupByFingerprint = function (fingerprint, cb) {
  this.lookupIdentity({
    private: true,
    query: {
      fingerprint: fingerprint
    }
  }, function (err, identity) {
    return {
      key: keyForFingerprint(identity, fingerprint),
      identity: identity
    }
  })
}

Driver.prototype.lookupByDHTKey = function (key, cb) {
  return this._db.query({ key: key })
    .once('data', function (data) {
      cb(null, data.value)
    })
}

Driver.prototype.getPrivateKey = function (fingerprint) {
  var priv
  this._keys.some(function (k) {
    if (k.fingerprint() === fingerprint) {
      priv = k
      return true
    }
  })

  return priv
}

Driver.prototype.lookupIdentity = function (options, cb) {
  typeforce({
    query: 'Object',
    private: '?Boolean'
  }, options)

  cb = safe(cb)

  var done = false
  var meJSON = this.identityJSON
  var query = options.query
  var valid = !!query.fingerprint ^ !!query[ROOT_HASH]
  if (!valid) {
    return cb(new Error('query by "fingerprint" OR "' + ROOT_HASH + '" (root hash)'))
  }

  if (query[ROOT_HASH]) {
    if (found(me)) return

    return this.addressBook.query(query)
  }

  if (query.fingerprint) {
    var pub = this.identity.keys({ fingerprint: fingerprint })[0]
    if (pub) {
      if (!options.private) return pub

      var priv = this.getPrivateKey(query.fingerprint)
      return priv || pub
    }
  }

  if (query.fingerprint) {
    if (found(me)) return
  }

  var stream = this._identityDB.query(query)
    .on('data', function (data) {
      if (found(data.value)) {
        stream.destroy()
      }
    })
    .once('error', cb)
    .once('end', function () {
      cb() // no results
    })

  function found (identity) {
    if (done) return

    var isMe = me[ROOT_HASH] === identity[ROOT_HASH]
    var key
    if (isMe && options.private) {
      key = self.getPrivateKey(query.fingerprint)
    }

    var key = key || keyForFingerprint(identity, fingerprint)
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
  var parsed = obj.parsed
  var copy = omit(obj, 'parsed')

  if (!copy.type && parsed) {
    copy.type = parsed.data._type
  }

  if (copy.type !== 'text') {
    assert(copy[ROOT_HASH], 'expected root hash')
    assert(copy[CUR_HASH], 'expected current hash')
  }

  this._log.append(copy)
}

Driver.prototype._chainWriterWorker = function (task, cb) {
  var self = this

  task = normalizeJSON(task)
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

Driver.prototype.putOnChain = function (options) {
  typeforce({
    data: 'Buffer'
  }, options)

  var recipients = options.recipients
  if (!recipients && options.to) {
    var to = options.to
    var pubKey = to.keys({
      type: 'bitcoin',
      networkName: this.networkName
    })[0].pubKeyString()

    recipients = [pubKey]
  }

  this.chainwriterq.push(extend(true, {
    recipients: recipients
  }, options)
}

Driver.prototype.send = function (options) {
  var self = this

  typeforce({
    msg: 'Object',
    to: 'String'
  }, options)

  parallel([
    normalizeMsg.bind(null, options.msg)
    // ,
    // this.lookupIdentity.bind(this, { query: query })
  ], function (err, results) {
    if (err) throw err

    var normalized = results[0]
    normalized.from = self.identityHash
    normalized.to = options.to
    self._logIt(normalized)
  })
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
