module.exports = function errToJson (err) {
  var json = {}

  Object.getOwnPropertyNames(err).forEach(function (key) {
    json[key] = err[key]
  })

  delete json.stack
  return json
}
