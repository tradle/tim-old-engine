
var safe = require('safecb')
var utils = require('tradle-utils')
var ChainedObj = require('chained-obj')
var Builder = ChainedObj.Builder
var Parser = ChainedObj.Parser

module.exports = function normalizeMsg (msg, cb) {
  parse(msg, function (err, parsed) {
    if (err) return cb(err)

    build(msg, function (err, buf) {
      if (err) return cb(err)

      cb(null, {
        data: buf,
        parsed: parsed
      })
    })
  })
}


function parse (msg, cb) {
  cb = safe(cb)
  if (!Buffer.isBuffer(msg)) return cb(null, msg)

  Parser.parse(msg, cb)
}

function build (msg, cb) {
  cb = safe(cb)
  if (Buffer.isBuffer(msg)) return cb(null, msg)

  new Builder()
    .data(msg)
    .build(cb)
}
