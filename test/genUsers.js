#!/usr/bin/env node

var fs = require('fs')
var path = require('path')
var Q = require('q')
var typeforce = require('typeforce')
var randomName = require('random-name')
var kiki = require('@tradle/kiki')
var identityLib = require('@tradle/identity')
var Identity = identityLib.Identity
var constants = require('@tradle/constants')
var NONCE = constants.NONCE
var argv = require('minimist')(process.argv.slice(2), {
  alias: {
    f: 'file',
    n: 'number'
  }
})

var utils = require('../lib/utils')
genUsers(argv)

function genUsers (opts) {
  typeforce({
    file: 'String',
    number: 'Number'
  }, opts)

  var file = path.resolve(opts.file)
  var number = opts.number
  var users = []
  for (var i = 0; i < number; i++) {
    var first = randomName.first()
    var last = randomName.last()
    var identity = new Identity()
      .name({
        firstName: first,
        lastName: last,
        formatted: first + ' ' + last
      })
      .set(NONCE, '' + i)

    var keys = identityLib.defaultKeySet({
      networkName: 'testnet'
    })

    if (keys.every(function (k) {
      return k.type() !== 'dsa'
    })) {
      keys.push(kiki.Keys.DSA.gen({ purpose: 'sign' }))
    }

    keys.forEach(identity.addKey, identity)

    users.push({
      pub: identity.toJSON(),
      priv: keys.map(function (k) {
        return k.exportPrivate()
      })
    })
  }

  Q.all(users.map(function (u) {
      return utils.getDHTKey(u.pub)
    }))
    .done(function (hashes) {
      users.forEach(function (u, i) {
        u.rootHash = hashes[i]
      })

      fs.writeFile(file, JSON.stringify(users, null, 2))
    })
}
