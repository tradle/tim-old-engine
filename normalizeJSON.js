
module.exports = function normalize (json) {
  if (Object.prototype.toString.call(json) !== '[object Object]') return json

  if (json && json.type === 'Buffer' && json.data && Object.keys(json).length === 2) {
    return new Buffer(json.data)
  } else {
    for (var p in json) {
      json[p] = normalize(json[p])
    }

    return json
  }
}

