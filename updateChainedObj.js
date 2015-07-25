
var constants = require('tradle-constants')
var PREV_HASH = constants.PREV_HASH
var ROOT_HASH = constants.ROOT_HASH

module.exports = function updateChainedObj (obj, prevHash) {
  obj[ROOT_HASH] = obj[ROOT_HASH] || prevHash
  obj[PREV_HASH] = prevHash
  return obj
}
