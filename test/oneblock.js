
var test = require('tape')
var crypto = require('crypto')
var leveldown = require('memdown')
var OneBlock = require('../oneblock')

test('one block', function (t) {
  t.plan(1)

  var block = new OneBlock({
    path: 'block.db',
    leveldown: leveldown
  })

  block.on('finish', function () {
    t.equal(block.height(), 5)
  })

  for (var i = 0; i <= 10; i++) {
    block.write({
      tx: {
        getId: getId,
        toHex: toHex
      },
      height: i / 2 | 0
    })
  }

  block.end()

  function getId () {
    return crypto.randomBytes(20).toString('hex')
  }

  function toHex() {
    return 'abc'
  }
})
