
var bitcoin = require('@tradle/bitcoinjs-lib')
var map = require('map-stream')
var debug = require('debug')('unchainer')
var extend = require('xtend/mutable')
var combine = require('stream-combiner2')
var typeforce = require('typeforce')
var Parser = require('@tradle/chained-obj').Parser
var Identity = require('@tradle/identity').Identity
var Verifier = require('@tradle/verifier')
var CONSTANTS = require('@tradle/constants')
var utils = require('./utils')
var Errors = require('./errors')
var TYPE = CONSTANTS.TYPE
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

  return combine.obj(
    map(function (txEntry, done) {
      var processed = extend({}, txEntry)
      // var tx = bitcoin.Transaction.fromBuffer(txEntry.tx)
      chainloader.load(txEntry)
        .then(function (obj) {
          extend(processed, obj)
        })
        .catch(function (err) {
          debug('unchain failed', err.message, txEntry.txId)
          extend(processed, err.progress)
          utils.addError({
            entry: processed,
            group: 'unchain',
            error: err
          })
        })
        .done(function () {
          done(null, processed)
        })
    }),
    mkParser(),
    basicIdCheck(),
    verify(verifier)
  )

  // var full = txProcessor
  //   .pipe(map(function (chainedObj, cb) {
  //     if (chainedObj.data) cb(null, chainedObj)
  //     else cb()
  //   }))
  //   .pipe(mkParser())
  //   .pipe(basicIdCheck())
  //   .pipe(verifier)

  // var empty = txProcessor
  //   .pipe(map(function (chainedObj, cb) {
  //     if (chainedObj.data) cb()
  //     else cb(null, chainedObj)
  //   }))

  // var passThrough = new PassThrough({ objectMode: true })
  // full.pipe(passThrough)
  // empty.pipe(passThrough)
  // return combine(
  //   txProcessor,
  //   passThrough
  // )
}

function mkParser () {
  return map(function (chainedObj, done) {
    if (!chainedObj.data) return done(null, chainedObj)

    Parser.parse(chainedObj.data, function (err, parsed) {
      if (err) {
        debug('invalid chained-obj', chainedObj.key)
        utils.addError(chainedObj, 'unchain', new Errors.Parse(err))
      } else {
        chainedObj.parsed = parsed
      }

      done(null, chainedObj)
    })

  })
}

function basicIdCheck () {
  return map(function (chainedObj, done) {
    if (!chainedObj.parsed) return done(null, chainedObj)

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

    done(null, chainedObj)
  })
}

function verify (verifier) {
  return map(function (chainedObj, cb) {
    if (!chainedObj.parsed) return cb(null, chainedObj)

    verifier.verify(chainedObj, function (err) {
      if (err) {
        utils.addError({
          entry: chainedObj,
          group: 'unchain',
          error: new Errors.Verification({
            message: Errors.getMessage(err)
          })
        })

        debug('failed to verify', chainedObj.key, err)
      }

      cb(null, chainedObj)
    })
  })
}