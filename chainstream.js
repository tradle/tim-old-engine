
var through2 = require('through2')
var PassThrough = require('readable-stream').PassThrough
var combine = require('stream-combiner2')
var typeforce = require('typeforce')
var Parser = require('chained-obj').Parser
var Verifier = require('tradle-verifier')
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
module.exports = function chainstream(options) {
  typeforce({
    lookup: 'Function',
    chainloader: 'Object'
  }, options)

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
    through2.obj(function (txInfo, enc, done) {
      // var tx = txInfo.tx
      // tx.height = txInfo.height
      this.push(txInfo.tx)
      done()
    }),
    options.chainloader,
    mkParser(),
    basicIdCheck(),
    verifier
  )

  return trans
}

function mkParser () {
  return through2.obj(function (chainedObj, enc, done) {
    var ps = this
    Parser.parse(chainedObj.data, function (err, parsed) {
      if (err) {
        debug('invalid chained-obj', chainedObj.key)
        return done()
      }

      chainedObj.parsed = parsed
      ps.push(chainedObj)
      done()
    })

  })
}

function basicIdCheck () {
  return through2.obj(function (chainedObj, enc, done) {
    var parsed = chainedObj.parsed
    var json = parsed.data
    var isIdentity = json._type === Identity.TYPE
    var from = chainedObj.from
    // only identities are allowed to be created without an identity
    if (!from && !isIdentity) return done()

    chainedObj.from = from && from.identity
    chainedObj.to = chainedObj.to && chainedObj.to.identity
    this.push(chainedObj)
    done()
  })
}
