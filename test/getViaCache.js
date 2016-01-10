
var test = require('tape')
var getViaCache = require('../lib/getViaCache')

test('getViaCache', function (t) {

  var cached = {}
  var UNCACHED = 0
  var CACHED = 1

  function getRegular (ids, cb) {
    ids.forEach(function (id) {
      cached[id] = true
    })

    cb(null, ids.map(function () {
      return UNCACHED
    }))
  }

  function getFromCache (ids, cb) {
    cb(null, ids.map(function (id) {
      return cached[id] ? CACHED : UNCACHED
    }))
  }

  var get = getViaCache(
    getRegular,
    getFromCache
  )

  var query = [1, 2, 3]
  get(query, function (err, results) {
    if (err) throw err

    results.forEach(function (r, i) {
      t.equal(r, UNCACHED)
    })

    query = [2, 3, 4]
    get(query, function (err, results) {
      if (err) throw err

      t.equal(results[0], CACHED)
      t.equal(results[1], CACHED)
      t.equal(results[2], UNCACHED)

      query = [2, 7, 1]
      get(query, function (err, results) {
        if (err) throw err

        t.equal(results[0], CACHED)
        t.equal(results[1], UNCACHED)
        t.equal(results[2], CACHED)
        t.end()
      })
    })
  })
})
