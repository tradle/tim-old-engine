
var test = require('tape')
var Entry = require('logbase').Entry
var utils = require('../lib/utils')

test('merge entries - simple', function (t) {
  var a = new Entry({
    a: 1,
    b: 2
  })

  var b = new Entry({
    a: 1,
    c: 3
  })

  var merged = utils.updateEntry(a, b)
  t.equal(merged.get('a'), 1)
  t.equal(merged.get('b'), 2)
  t.equal(merged.get('c'), 3)
  t.end()
})

test('entry or json', function (t) {
  var a = new Entry({
    a: 1,
    b: 2
  })

  var b = new Entry({
    a: 1,
    c: 3
  })

  var ab1 = utils.updateEntry(a, b)
  var ab2 = utils.updateEntry(a.toJSON(), b)
  var ab3 = utils.updateEntry(a, b.toJSON())
  var ab4 = utils.updateEntry(a.toJSON(), b.toJSON())

  t.deepEqual(ab1, ab2)
  t.deepEqual(ab3, ab4)
  t.deepEqual(ab1, ab4)
  t.end()
})

test('merge entries - don\'t override timestamp', function (t) {
  var a = new Entry({
    timestamp: 2
  })

  var b = new Entry({
    timestamp: 1
  })

  var merged = utils.updateEntry(a, b)
  t.equal(merged.get('timestamp'), 2)
  merged = utils.updateEntry(b, a)
  t.equal(merged.get('timestamp'), 1)
  t.end()
})

test('merge entries - merge errors', function (t) {
  var a = new Entry({
    errors: {
      chain: [new Error('good'), new Error('bad')]
    }
  })

  var b = new Entry({
    errors: {
      chain: [new Error('ugly')],
      send: [new Error('weird')]
    }
  })

  var merged = utils.updateEntry(a, b).toJSON()
  t.deepEqual(merged.errors.chain.map(function (e) {
    return e.message
  }), ['good', 'bad', 'ugly'])

  t.equal(merged.errors.send[0].message, 'weird')
  t.end()
})
