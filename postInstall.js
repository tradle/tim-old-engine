#!/usr/bin/env node

// BS hack to dedupe git dependencies (which npm doesn't do for us)

var path = require('path')
var fs = require('fs-extra')
var nmDir = path.join(__dirname, 'node_modules/')

var zlorpOtrDir = nmDir + 'zlorp/node_modules/otr'
fs.exists(nmDir + 'otr', function (exists) {
  if (exists) clean()
  else {
    raiseOTR(function (err) {
      if (err) throw err

      clean
    })
  }
})

function raiseOTR (cb) {
  fs.exists(zlorpOtrDir, function (exists) {
    if (exists) {
      fs.move(zlorpOtrDir, nmDir + 'otr', cb)
    } else cb()
  })
}

function clean () {
  ;[
    nmDir + 'kiki/node_modules/otr'
  ].forEach(function (dir) {
    fs.exists(dir, function (exists) {
      if (exists) fs.remove(dir, rethrow)
    })
  })
}

function rethrow (err) {
  if (err) throw err
}
