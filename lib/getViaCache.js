
module.exports = function getViaCache (get, getFromCache) {
  return function (ids, cb) {
    getFromCache(ids, function (err, fromCache) {
      if (err) {
        return fetchAndMerge(ids.map(function () {
          return null
        }), ids, cb)
      }

      if (fromCache.length !== ids.length) {
        throw new Error('getFromCache should return an array the same length as the input')
      }

      return fetchAndMerge(fromCache, ids.filter(function (id, idx) {
        return !fromCache[idx]
      }), cb)
    })

    function fetchAndMerge (results, missing) {
      if (missing.length) get(missing, onFetched)
      else onFetched(null, [])

      function onFetched (err, fetched) {
        if (err) return cb(err)

        var offset = 0
        results = results.map(function (r) {
          return r == null ? fetched[offset++] : r
        })
        .filter(function (r) {
          return !r.ignore
        })

        cb(null, results)
      }
    }
  }
}
