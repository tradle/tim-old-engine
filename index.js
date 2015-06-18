
var util = require('util')
var EventEmitter = require('events').EventEmitter
var debug = require('debug')
var levelup = require('levelup')
var Queue = require('level-jobs')
var bufferEqual = require('buffer-equal')
var extend = require('extend')
var utils = require('tradle-utils')
var parallel = require('run-parallel')
var concat = require('concat-stream')
var through2 = require('through2')
var typeforce = require('typeforce')
var loader = require('./components')
var Parser = require('chained-obj').Parser
var MSG_TYPES = {
  plaintext: 1 << 1,
  chain: 1 << 2
}

var normalizeStream = through2.ctor({ objectMode: true }, function (data, enc, done) {
  this.push(normalize(data))
  done()
})

module.exports = Driver
util.inherits(Driver, EventEmitter)

function Driver (options) {
  var self = this
  this.pathPrefix = options.pathPrefix

  loader(options, function (err, components) {
    if (err) throw err

    extend(self, components)
    self.init()
  })
}

Driver.prototype.init = function () {
  var self = this
  var chainwriter = this.chainwriter
  if (this._initialized) return

  this._initialized = true
  this.queueDB = levelup(this._prefix('txs.db'), {
    db: this.leveldown,
    valueEncoding: 'json'
  })

  this.chainwriterq = Queue(this.queueDB, chainWriterWorker, 1)
  this.chainwriterq.on('error', function (err) {
    debugger
    self.emit('error', err)
  })

  // this.zlorp.on('connect', function (fingerprint, addr) {
  //   self.emit('connect', fingerprint, addr)
  // })

  this.zlorp.on('data', this._onmessage.bind(this))
  this._pendingDB = levelup(this._prefix('pending.db'), {
    db: this.leveldown,
    valueEncoding: 'json'
  })

  this.chaindb.on('saved', function (chainedObj) {
    console.log('saved', chainedObj.key)
    self._pendingDB.get(chainedObj.key, function (err, saved) {
      if (err) return

      saved = normalize(saved)
      if (bufferEqual(saved, chainedObj.file)) {
        self.emit('resolved', chainedObj)
        self._pendingDB.del(chainedObj.key)
      }
    })
  })

  // this.chaindb.on('invalid', function (txId, chainedObj) {
  //   self._pendingDB.get(chainedObj.key, function (err, saved) {
  //     if (err) return

  //     saved.invalid = true
  //     self._invalidDB.put(chainedObj)
  //     self._pendingDB.del(chainedObj.key)
  //   })
  // })

  this.emit('ready')

  function chainWriterWorker(task, cb) {
    task = normalize(task)
    var promise
    if (task.data) {
      // create
      typeforce({
        data: 'Object',
        public: '?Boolean',
        recipients: '?Array'
      }, task)

      promise = chainwriter.create()
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

      promise = chainwriter.share()
        .shareAccessTo(task.key, task.encryptionKey)
        .shareAccessWith(task.shareWith)
        .setPublic(!!task.public)
        .execute()
    }

    promise.then(function (resp) {
      self.emit('chained', task, resp)
      cb()
    })
    .catch(cb)
    .done()
  }
}

Driver.prototype.getPending = function (cb) {
  return this._pendingDB
    .createReadStream()
    .pipe(normalizeStream())
    .pipe(concat(cb))
}

Driver.prototype.getResolved = function (cb) {
  return this.chaindb
    .createReadStream()
    .pipe(normalizeStream())
    .pipe(concat(cb))
}

Driver.prototype._prefix = function (path) {
  return this.pathPrefix + '-' + path
}

Driver.prototype._onmessage = function (msg, fingerprint) {
  msg = bufToMsg(msg)
  var type = msg.type
  msg = msg.msg
  // try {
  //   msg = JSON.parse(msg.msg)
  // } catch (err) {
  //   return this.emit('warn', 'received message is not valid json', msg)
  // }

  this.emit('message', msg, fingerprint)
  switch (type) {
    case MSG_TYPES.plaintext:
      break;
    case MSG_TYPES.chain:
      this._onChainMessage(msg, fingerprint)
      break;
    default:
      this.emit('warn', 'unknown message type', msg)
      break;
  }
}

Driver.prototype._onChainMessage = function (msg, fingerprint) {
  var self = this
  // typeforce({
  //   txId: 'String',
  //   key: 'String',
  //   data: 'Object|Buffer'
  // })

  // var txId = json.txId
  // var key = json.key // dht key
  // var value = json.value

  utils.getStorageKeyFor(msg, function (err, key) {
    if (err) {
      return self._debug('invalid msg', msg)
    }

    self._pendingDB.put(key.toString('hex'), msg, rethrow)
  })


  // this.chaindb._processChainedObj({
  //   key: json.key,
  //   addresses:
  // })

  // this.chaindb.byFingerprint(fingerprint)
  //   .then(function (identity) {
  //     identity = Identity.fromJSON(identity)
  //     var parser = new Parser()
  //     parser.verifyWith(identity.keys({
  //       type: 'dsa'
  //     }))
  //   })

  // var buf = toBuffer(data) // wasteful
  // Parser.parse(buf, function (err, obj) {
  //   if (err) {
  //     return self.emit('error', 'Failed to parse data msg from peer: ' + err.message)
  //   }

  //   self.chaindb.
  //   // obj.data
  //   // obj.attachments
  // })
}

Driver.prototype._sendMsg = function (options) {
  typeforce({
    msg: 'Object',
    to: 'Identity'
  }, options)

  var msg = {
    type: options.chain ? MSG_TYPES.chain : MSG_TYPES.plaintext,
    msg: options.msg
  }

  var msgBuf = msgToBuf(msg)
  var to = options.to

  this.zlorp.send(msgBuf, to.keys({
    type: 'dsa'
  })[0].fingerprint())
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

  setTimeout(function () {
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
  }, 5000)
}

Driver.prototype.destroy = function (cb) {
  var self = this

  // sync
  this.chainwriter.destroy()

  // async
  // this.keeper.destroy().done(console.log.bind(console, 'keeper dead'))
  // this.zlorp.destroy(console.log.bind(console, 'zlorp dead')),
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
    this.zlorp.destroy.bind(this.zlorp),
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
    msg: buf.slice(1)
  }
}

function normalize (json) {
  if (Object.prototype.toString.call(json) !== '[object Object]') return json

  if (json && json.type === 'Buffer' && json.data && Object.keys(json).length === 2) {
    return new Buffer(json.data)
  } else {
    for (var p in json) {
      json[p] = normalize(json[p])
    }

    return json
  }
}
