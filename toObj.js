
module.exports = function toObj (/* k1, v1, k2, v2... */) {
  var obj = {}
  for (var i = 0; i < arguments.length; i += 2) {
    obj[arguments[i]] = arguments[i + 1]
  }

  return obj
}
