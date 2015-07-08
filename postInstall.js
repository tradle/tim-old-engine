#!/usr/bin/env node

var path = require('path')
var fs = require('fs-extra')
var path = require('path')
var nmDir = path.join(__dirname, 'node_modules/')

var zlorpOtrDir = nmDir + 'zlorp/node_modules/otr'
fs.exists(zlorpOtrDir, function (exists) {
  if (exists) {
    fs.move(zlorpOtrDir, nmDir + 'otr', function (err) {
      if (err) throw err

      clean()
    })
  } else clean()
})

function clean () {
  ;[
    nmDir + 'midentity/node_modules/otr',
    nmDir + 'tradle-verifier/node_modules/midentity',
    nmDir + 'tradle-verifier/node_modules/chained-obj'
  ].forEach(function (dir) {
    fs.exists(dir, function (exists) {
      if (exists) fs.remove(dir, rethrow)
    })
  })
}

function rethrow (err) {
  if (err) throw err
}
