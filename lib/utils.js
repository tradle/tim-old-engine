var Q = require('q')
var extend = require('xtend')
var typeforce = require('typeforce')
var constants = require('tradle-constants')
var find = require('array-find')
var tutils = require('tradle-utils')
var Builder = require('chained-obj').Builder
var NONCE = constants.NONCE
var CUR_HASH = constants.CUR_HASH
var ROOT_HASH = constants.ROOT_HASH
var TxData = require('tradle-tx-data').TxData
var Entry = require('logbase').Entry

var MSG_SCHEMA = {
  txData: 'Buffer',
  txType: 'Number',
  encryptedPermission: 'Buffer',
  encryptedData: 'Buffer'
}

var utils = module.exports = {
  getMsgProps: function (info) {
    return {
      txData: info.encryptedKey,
      txType: info.txType, // no need to send this really
      encryptedPermission: info.encryptedPermission,
      encryptedData: info.encryptedData
    }
  },

  validateMsg: function (msg) {
    try {
      typeforce(MSG_SCHEMA, msg)
      return true
    } catch (err) {
      return false
    }
  },

  msgToBuffer: function (msg) {
    if (!utils.validateMsg(msg)) throw new Error('invalid msg')

    msg = extend(msg)
    for (var p in MSG_SCHEMA) {
      var type = MSG_SCHEMA[p]
      if (type === 'Buffer') {
        msg[p] = msg[p].toString('base64')
      }
    }

    return utils.toBuffer(msg, 'binary')
  },

  bufferToMsg: function (buf) {
    var msg = JSON.parse(buf.toString('binary'))
    for (var p in MSG_SCHEMA) {
      var type = MSG_SCHEMA[p]
      if (type === 'Buffer') {
        msg[p] = new Buffer(msg[p], 'base64')
      }
    }

    return msg
  },

  getUID: function (entry) {
    var uid = entry.get('uid')
    if (uid) return uid

    var isPublic = getProp(entry, 'txType') === TxData.types.public
    var curHash = getProp(entry, CUR_HASH)
    var rootHash = getProp(entry, ROOT_HASH)
    var keyParts = [rootHash, curHash]
    if (isPublic) {
      keyParts.push('public')
    } else {
      var from = getProp(entry, 'from')
      from = from && from[ROOT_HASH]
      var to = getProp(entry, 'to')
      to = to && to[ROOT_HASH]
      if (!(from && to && curHash)) {
        throw new Error('unable to derive uid for entry: ' + JSON.stringify(entry))
      }

      keyParts.push(from, to)
    }

    return keyParts.join('-')
  },

  toErrInstance: function (err) {
    var n = new Error(err.message)
    for (var p in err) {
      n[p] = err[p]
    }

    return n
  },

  pluck: function (arr, prop) {
    var vals = []
    for (var i = 0; i < arr.length; i++) {
      vals.push(arr[i][prop])
    }

    return vals
  },

  pick: function (obj) {
    var a = {}
    for (var i = 1; i < arguments.length; i++) {
      var p = arguments[i]
      a[p] = obj[p]
    }

    return a
  },

  setUID: function (entry) {
    entry.set('uid', utils.getUID(entry))
  },

  firstKey: function (keys, where) {
    if (typeof where === 'string') where = { fingerprint: where }

    return find(keys, function (k) {
      for (var p in where) {
        if (where[p] !== k[p]) return
      }

      return true
    })
  },

  getDHTKey: function (obj, cb) {
    if (!obj[NONCE]) return Q.reject(new Error('missing nonce'))

    var b = new Builder()
      .data(obj)

    return Q.ninvoke(b, 'build')
      .then(function (result) {
        return Q.ninvoke(tutils, 'getStorageKeyFor', result.form)
      })
      .then(function (key) {
        return key.toString('hex')
      })
  },

  addError: function (entry, err) {
    var errs = entry.get('errors') || []
    errs.push(utils.errToJSON(err))
    entry.set('errors', errs)
  },

  now: Date.now.bind(Date),

  toObj: function (/* k1, v1, k2, v2... */) {
    var obj = {}
    for (var i = 0; i < arguments.length; i += 2) {
      obj[arguments[i]] = arguments[i + 1]
    }

    return obj
  },

  errToJSON: function (err) {
    var json = {}

    Object.getOwnPropertyNames(err).forEach(function (key) {
      json[key] = err[key]
    })

    delete json.cause
    delete json.stack
    return json
  },

  toBuffer: function (data, enc) {
    if (typeof data.toBuffer === 'function') return data.toBuffer()
    if (Buffer.isBuffer(data)) return data
    if (typeof data === 'object') data = tutils.stringify(data)

    return new Buffer(data, enc || 'binary')
  }
}

function getProp (entry, prop) {
  return entry instanceof Entry ? entry.get(prop) : entry[prop]
}
