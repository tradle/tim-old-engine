
var bitcoin = require('bitcoinjs-lib')
var map = require('map-stream')
var debug = require('debug')('chainstream')
var extend = require('extend')
var combine = require('stream-combiner2')
var typeforce = require('typeforce')
var Parser = require('chained-obj').Parser
var Identity = require('midentity').Identity
var Verifier = require('tradle-verifier')
var CONSTANTS = require('tradle-constants')
var TYPE = CONSTANTS.TYPE
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
      processed.errors = []
      var tx = bitcoin.Transaction.fromBuffer(txEntry.tx)
      chainloader.load(tx)
        .then(function (chainedObjs) {
          var obj = chainedObjs && chainedObjs[0]
          if (obj) {
            extend(processed, obj)
          }
        })
        .catch(function (err) {
          debug('unchain failed', err.message)
          processed.errors.push(err)
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
        chainedObj.errors.push(err)
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
    if (from || isIdentity) {
      if (from) chainedObj.from = from.identity
      else if (isIdentity) chainedObj.from = Identity.fromJSON(json)

      if (chainedObj.to) chainedObj.to = chainedObj.to.identity
    }

    done(null, chainedObj)
  })
}

function verify (verifier) {
  return map(function (chainedObj, cb) {
    if (!chainedObj.parsed) return cb(null, chainedObj)

    verifier.verify(chainedObj, function (err) {
      if (err) {
        chainedObj.errors.push(err)
        debug('failed to verify', chainedObj.key, err)
      }

      cb(null, chainedObj)
    })
  })
}
