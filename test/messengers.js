
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

test('p2p', async function (t) {
  var received
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

  var zlorps = await Q.Promise(function (resolve) {
    utils.makeConnectedZlorps(identityInfos, resolve)
  })

  var messengers = zlorps.map(function (z) {
    return new Messengers.P2P({
      zlorp: z
    })
  })

  messengers[1].on('message', function () {
    received = true
  })

  var msg = new Buffer('blah')
  try {
    await messengers[0].send('123', msg, { identity: identityInfos[1].pub })

    t.ok(received)
    await* messengers.map(async function (m) {
      m.destroy()
    })

    await* zlorps.map(async function (z) {
      return Q.ninvoke(z._dht, 'destroy')
    })
  } catch (err) {
    t.error(err)
  }

  t.end()
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

  server.listen(async function () {
    var recipientRootHash = '123'
    var recipientInfo = {}
    recipientInfo[ROOT_HASH] = recipientRootHash

    messenger.addEndpoint(recipientRootHash, 'http://127.0.0.1:' + server.address().port)
    try {
      await messenger.send('123', msg, recipientInfo)
      t.ok(received)
      server.close()
      await messenger.destroy()
    } catch (err) {
      t.error(err)
    }

    t.end()
  })
})
