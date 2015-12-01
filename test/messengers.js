
var http = require('http')
var test = require('tape')
var Q = require('q')
var collect = require('stream-collector')
var ROOT_HASH = require('@tradle/constants').ROOT_HASH
var Messengers = require('../lib/messengers')
var utils = require('./helpers/utils')
var billPub = require('./fixtures/bill-pub')
var billPriv = require('./fixtures/bill-priv')
var tedPub = require('./fixtures/ted-pub')
var tedPriv = require('./fixtures/ted-priv')

test('p2p', function (t) {
  var identityInfos = [
    {
      pub: billPub,
      priv: billPriv
    },
    {
      pub: tedPub,
      priv: tedPriv
    }
  ]
  utils.makeConnectedZlorps(identityInfos, function (zlorps) {
    var messengers = zlorps.map(function (z) {
      return new Messengers.P2P({
        zlorp: z
      })
    })

    var msg = new Buffer('blah')
    messengers[0].send('123', msg, { identity: identityInfos[1].pub })
      .catch(t.fail)
      .finally(function () {
        t.ok(received)
        return Q.all(messengers.map(function (m) {
          m.destroy()
        }))
      })
      .then(function () {
        return Q.all(zlorps.map(function (z) {
          return Q.ninvoke(z._dht, 'destroy')
        }))
      })
      .then(function () {
        t.end()
      })
      .done()

    messengers[1].on('message', function () {
      received = true
    })
  })
})

test('http', function (t) {
  var messenger = new Messengers.HttpClient()
  var senderRootHash = 'abc'
  messenger.setRootHash(senderRootHash)

  var msg = new Buffer('blah')
  var received
  var server = http.createServer(function (req, res) {
    collect(req, function (err, bufs) {
      if (err) throw err

      var buf = Buffer.concat(bufs)
      t.deepEqual(buf, msg)
      var resp = new Buffer(JSON.stringify({ hey: 'ho' }))
      res.writeHead(200, {
        'Content-Length': resp.length,
        'Content-Type': 'application/json'
      })

      res.write(resp)
      res.end()
    })

    received = true
  })

  server.listen(function () {
    var recipientRootHash = '123'
    var recipientInfo = {}
    recipientInfo[ROOT_HASH] = recipientRootHash

    messenger.addEndpoint(recipientRootHash, 'http://127.0.0.1:' + server.address().port)
    messenger.send('123', msg, recipientInfo)
      .catch(function (err) {
        console.log(err)
        throw err
      })
      .finally(function () {
        t.ok(received)
      })
      .then(function () {
        server.close()
        return messenger.destroy()
      })
      .done(function () {
        t.end()
      })
  })
})
