
var typeforce = require('typeforce')
var Zlorp = require('zlorp')
var DSA = Zlorp.DSA
var memdown = require('memdown')
var DHT = require('@tradle/bittorrent-dht')
var basePort = 22222

module.exports = {
  makeConnectedDHTs: makeConnectedDHTs,
  destroyZlorps: destroyZlorps,
  connectDHTs: connectDHTs,
  makeConnectedZlorps: makeConnectedZlorps
}

function makeConnectedDHTs (n, cb) {
  var dhts = []
  for (var i = 0; i < n; i++) {
    var dht = new DHT({ bootstrap: false })
    dht.listen(basePort++, finish)
    dhts.push(dht)
  }

  function finish () {
    if (--n === 0) {
      connectDHTs(dhts)
      cb(dhts)
    }
  }

  return dhts
}

function makeConnectedZlorps (identityInfos, cb) {
  identityInfos.forEach(function (identityInfo) {
    typeforce({
      pub: 'Object',
      priv: 'Array'
    }, identityInfo)
  })

  makeConnectedDHTs(identityInfos.length, function (dhts) {
    var nodes = dhts.map(function (dht, i) {
      var identityInfo = identityInfos[i]
      return new Zlorp({
        name: identityInfo.pub.name.firstName,
        port: process.env.MULTIPLEX ? dht.address().port : basePort++,
        dht: dht,
        key: getDSAKey(identityInfo.priv),
        leveldown: memdown
      })
    })

    cb(nodes)
  })
}

function destroyZlorps (nodes, cb) {
  nodes = [].concat(nodes)
  var togo = nodes.length * 2
  nodes.forEach(function (node) {
    node.destroy(finish)
    node._dht.destroy(finish)
  })

  function finish () {
    if (--togo === 0 && cb) cb()
  }
}

function connectDHTs (dhts) {
  var n = dhts.length

  for (var i = 0; i < n; i++) {
    var next = dhts[(i + 1) % n]
    dhts[i].addNode('127.0.0.1:' + next.address().port, next.nodeId)
  }
}

function getDSAKey (keys) {
  var key = keys.filter(function (k) {
    return k.type === 'dsa'
  })[0]

  return DSA.parsePrivate(key.priv)
}
