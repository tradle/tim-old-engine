
var through2 = require('through2')
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

  var trans = combine(
    // through2.obj(function (txInfo, enc, done) {
    //   // var tx = txInfo.tx
    //   // tx.height = txInfo.height
    //   this.push(txInfo.tx)
    //   done()
    // }),
    through2.obj(function (txInfo, enc, done) {
      var self = this
      chainloader.load(txInfo.tx)
        .then(function (chainedObjs) {
          var obj = chainedObjs && chainedObjs[0]
          if (obj) {
            obj = extend({}, obj, txInfo)
            self.push(obj)
          }
        })
        .catch(function (err) {
          debug('nothing to load in tx?', err)
        })
        .finally(function () {
          done()
        })
        .done()
    }),
    // options.chainloader,
    mkParser(),
    basicIdCheck(),
    verifier
  )

  return trans
}

function mkParser () {
  return through2.obj(function (chainedObj, enc, done) {
    var self = this
    Parser.parse(chainedObj.data, function (err, parsed) {
      if (err) {
        debug('invalid chained-obj', chainedObj.key)
        return done()
      }

      chainedObj.parsed = parsed
      self.push(chainedObj)
      done()
    })

  })
}

function basicIdCheck () {
  return through2.obj(function (chainedObj, enc, done) {
    var parsed = chainedObj.parsed
    var json = parsed.data
    var isIdentity = json[TYPE] === Identity.TYPE
    var from = chainedObj.from
    // only identities are allowed to be created without an identity
    if (!from && !isIdentity) return done()

    if (from) chainedObj.from = from.identity
    else if (isIdentity) chainedObj.from = Identity.fromJSON(json)

    if (chainedObj.to) chainedObj.to = chainedObj.to.identity

    this.push(chainedObj)
    done()
  })
}
