
var bitcoin = require('@tradle/bitcoinjs-lib')
var map = require('map-stream')
var Q = require('q')
var debug = require('debug')('unchainer')
var extend = require('xtend/mutable')
var typeforce = require('typeforce')
var Parser = require('@tradle/chained-obj').Parser
var Identity = require('@tradle/identity').Identity
var Verifier = require('@tradle/verifier')
var CONSTANTS = require('@tradle/constants')
var utils = require('./utils')
var Errors = require('./errors')
var TYPE = CONSTANTS.TYPE
var CUR_HASH = CONSTANTS.CUR_HASH
var ROOT_HASH = CONSTANTS.ROOT_HASH
var plugins = Verifier.plugins
var DEFAULT_PLUGINS = [
  plugins.sigcheck,
  plugins.prev,
  plugins.identity
]

/**
 * transactions go in, verified on-chain objects come out (or die inside)
 * @param  {[type]} options [description]
 * @return {[type]}         [description]
 */
module.exports = function chainstream (options) {
  typeforce({
    lookup: 'Function',
    chainloader: 'Object'
  }, options)

  var chainloader = options.chainloader
  var verifier = new Verifier({
    lookup: options.lookup
  })

  var plugins = DEFAULT_PLUGINS.slice()
  if (options.plugins) {
    options.plugins.forEach(function (p) {
      if (plugins.indexOf(p) === -1) {
        plugins.push(p)
      }
    })
  }

  verifier.use(plugins)

  return {
    unchain: unchain
  }

  function unchain (txEntry, opts) {
    opts = opts || {}
    typeforce({
      parse: '?Boolean',
      verify: '?Boolean'
    }, opts)

    var doVerify = opts.verify !== false
    var doParse = doVerify || opts.parse !== false
    var processed = extend({}, txEntry)
    return chainloader.load(txEntry)
      .catch(saveProgressAndRethrow(processed))
      .then(function (obj) {
        extend(processed, obj)
        if (doParse) {
          return parse(processed)
            .then(checkFrom.bind(null, processed))
        }
      })
      .then(function () {
        processed[CUR_HASH] = processed.key
        processed[ROOT_HASH] = processed.parsed.data[ROOT_HASH] || processed.key
        processed[TYPE] = processed.parsed.data[TYPE]

        if (doVerify) {
          return verify(verifier, processed)
        }
      })
      .then(function () {
        return processed
      })
  }
}

function parse (chainedObj) {
  return Parser.parse(chainedObj.data)
    .then(function (parsed) {
      chainedObj.parsed = parsed
    })
    .catch(saveProgressAndRethrow(chainedObj, Errors.Parse))
}

function checkFrom (chainedObj) {
  var parsed = chainedObj.parsed
  var json = parsed.data
  var isIdentity = json[TYPE] === Identity.TYPE
  var from = chainedObj.from
  // only identities are allowed to be created without an identity
  if (!from && isIdentity) {
    chainedObj.from = {
      identity: Identity.fromJSON(json)
    }

    chainedObj.from[ROOT_HASH] = chainedObj[ROOT_HASH] || chainedObj.key
  }
}

function verify (verifier, chainedObj) {
  return Q.ninvoke(verifier, 'verify', chainedObj)
    .catch(saveProgressAndRethrow(chainedObj, Errors.Verification))
}

function saveProgressAndRethrow (chainedObj, ErrType) {
  return function (err) {
    debug('unchain failed', chainedObj.key, err.message)
    if (ErrType) err = new ErrType(err)

    utils.addError({
      entry: chainedObj,
      group: 'unchain',
      error: err
    })

    throw err
  }
}
