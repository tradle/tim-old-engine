
var typeforce = require('typeforce')
var extend = require('extend')
var omit = require('object.omit')
var ArrayProto = Array.prototype

module.exports = LogEntry

function LogEntry (id) {
  if (!(this instanceof LogEntry)) return new LogEntry(id)

  this._props = {}
  this._metadata = {
    tags: []
    // id: null,
    // prev: null
  }

  if (typeof id !== 'undefined') this._metadata.id = id
}

LogEntry.prototype.metadata = function () {
  return extend(true, {}, this._metadata)
}

LogEntry.prototype.hasTag = function (tag) {
  return this._metadata.tags.indexOf(tag) !== -1
}

LogEntry.prototype.tag = function (tags) {
  var myTags = this._metadata.tags
  tags = ArrayProto.concat.apply([], arguments)
  tags.forEach(function (tag) {
    if (myTags.indexOf(tag) === -1) {
      myTags.push(tag)
    }
  })

  return this
}

LogEntry.prototype.id = function (id) {
  if (typeof id === 'number') {
    this._metadata.id = id
    return this
  } else {
    return this._metadata.id
  }
}

LogEntry.prototype.tags = function () {
  return this._metadata.tags.slice()
}

LogEntry.prototype.prev = function (id) {
  if (arguments.length === 0) return this._metadata.prev

  var prevId
  if (typeof id === 'number') {
    prevId = id
  } else if (id instanceof LogEntry) {
    prevId = id.id()
  } else if (id._l) {
    prevId = id._l.id
  }

  typeforce('Number', prevId)
  this._metadata.prev = prevId
  return this
}

LogEntry.prototype.get = function (name) {
  return this._props[name]
}

LogEntry.prototype.set = function (name, value) {
  if (typeof name === 'object') {
    extend(true, this._props, name)
  } else {
    this._props[name] = value
  }

  return this
}

LogEntry.prototype.copy = function (props) {
  if (arguments.length === 1) this.set(props)
  else {
    for (var i = 1; i < arguments.length; i++) {
      var prop = arguments[i]
      this._props[prop] = getProp(props, prop)
    }
  }

  return this
}

LogEntry.prototype.toJSON = function () {
  this.validate()
  return extend(true, {
    _l: extend(true, {}, this._metadata)
  }, this._props)
}

LogEntry.prototype.validate = function () {
  return !!this._metadata.tags.length
}

LogEntry.fromJSON = function (json) {
  var metadata = json._l
  typeforce('Object', metadata)
  typeforce('Array', metadata.tags)

  var entry = new LogEntry()
    .set(omit(json, '_l'))
    .tag(metadata.tags)

  if ('id' in metadata) entry.id(metadata.id)
  if ('prev' in metadata) entry.prev(metadata.prev)

  return entry
}

function getProp (obj, name) {
  return obj instanceof LogEntry ? obj.get(name) : obj[name]
}
