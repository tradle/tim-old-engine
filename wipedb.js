
var safe = require('safecb')

module.exports = function wipedb (db, cb) {
  cb = safe(cb)
  var keys = []
  db.createKeyStream()
    .on('data', keys.push.bind(keys))
    .on('error', cb)
    .on('end', function () {
      db.batch(keys.map(function (k) {
        return { type: 'del', key: k }
      }), cb)
    })
}
