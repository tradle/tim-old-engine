'use strict';

var rebuf = require('logbase').rebuf;

module.exports = {
  encode: function encode(entry) {
    return JSON.stringify(entry);
  },
  decode: function decode(entry) {
    try {
      return rebuf(JSON.parse(entry));
    } catch (err) {
      console.warn('failed to decode entry', entry);
      return entry;
    }
  },
  buffer: false,
  type: 'jsonWithBufs'
};