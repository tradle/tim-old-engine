
var constants = require('tradle-constants')
var CUR_HASH = constants.CUR_HASH
var ROOT_HASH = constants.ROOT_HASH
var TxData = require('tradle-tx-data').TxData
var Entry = require('logbase').Entry

module.exports = function getUID (entry) {
  var uid = entry.get('uid')
  if (uid) return uid

  var isPublic = getProp(entry, 'txType') === TxData.types.public
  var curHash = getProp(entry, CUR_HASH)
  if (isPublic) return 'public-' + curHash

  var from = getProp(entry, 'from')
  from = from && from[ROOT_HASH]
  var to = getProp(entry, 'to')
  to = to && to[ROOT_HASH]
  if (!(from && to && curHash)) {
    throw new Error('unable to derive uid for entry: ' + JSON.stringify(entry))
  }

  return from + '-' + to + '-' + curHash
}

function getProp (entry, prop) {
  return entry instanceof Entry ? entry.get(prop) : entry[prop]
}
