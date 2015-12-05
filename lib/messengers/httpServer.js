'use strict';

var util = require('util');
var EventEmitter = require('events').EventEmitter;
var debug = require('debug')('http-messenger-server');
var typeforce = require('typeforce');
var Q = require('q');
var collect = require('stream-collector');
var constants = require('@tradle/constants');
// var Messenger = require('tim/lib/messenger')

var ROOT_HASH = constants.ROOT_HASH;

module.exports = HttpMessengerServer;
util.inherits(HttpMessengerServer, EventEmitter);

function HttpMessengerServer(opts) {
  typeforce({
    // express app or express.Router
    router: typeforce.oneOf('EventEmitter', 'Function'),
    receive: '?Function'
  }, opts);

  EventEmitter.call(this);
  // Messenger.call(this)

  this._onmessage = this._onmessage.bind(this);
  this._recipients = {};
  this._responses = [];

  var router = opts.router;
  router.put('/:fromRootHash', this._onmessage);
  this.receive = opts.receive;
}

HttpMessengerServer.prototype.send = function (rootHash, msg, identityInfo) {
  debug('queueing response for', rootHash);
  var q = this._recipients[rootHash] = this._recipients[rootHash] || [];
  q.push(JSON.parse(msg));

  // hack, otherwise tim will wait indefinitely
  // before sending another message to this recipient
  return Q.resolve();
};

// HttpMessengerServer.prototype.release = function (rootHash, res) {
//   var q = this._recipients[rootHash] || []
//   var data = q.map(function (item) {
//     return item.data
//   })

//   res.json(data)
// }

HttpMessengerServer.prototype._onmessage = function (req, res, next) {
  var self = this;
  if (this._destroyed) {
    return sendErr(res, 503, 'not available');
  }

  var from = {};
  var fromRootHash = from[ROOT_HASH] = req.params.fromRootHash;
  // if (!receiveMsg) {
  //   return sendErr(res, 404, 'recipient not found')
  // }

  if (!this.receive) throw new Error('please set "receive" function');

  debug('received msg from', fromRootHash);
  collect(req, function _callee(err, bufs) {
    var buf, queued;
    return regeneratorRuntime.async(function _callee$(_context) {
      while (1) switch (_context.prev = _context.next) {
        case 0:
          if (!err) {
            _context.next = 2;
            break;
          }

          return _context.abrupt('return', sendErr(res, 501, 'Something went wrong'));

        case 2:
          buf = Buffer.concat(bufs);

          debug('processing msg from', fromRootHash);

          _context.prev = 4;
          _context.next = 7;
          return regeneratorRuntime.awrap(self.receive(buf, from));

        case 7:
          debug('processed msg from', fromRootHash);
          queued = self._recipients[fromRootHash] || [];

          res.json(queued);
          queued.length = 0;
          _context.next = 17;
          break;

        case 13:
          _context.prev = 13;
          _context.t0 = _context['catch'](4);

          debug('failed to process message', buf.toString(), _context.t0);
          if ('code' in _context.t0) {
            sendErr(res, _context.t0);
          } else {
            // for security purposes
            // don't propagate internal errs
            sendErr(res, 501, 'Something went wrong');
          }

        case 17:
        case 'end':
          return _context.stop();
      }
    }, null, this, [[4, 13]]);
  });
};

HttpMessengerServer.prototype.destroy = function () {
  this._destroyed = true;
  return Q.resolve();
};

// HttpMessengerServer.prototype.addReceipient = function (rootHash, receiveMsg) {
//   typeforce('String', rootHash)
//   typeforce('Function', receiveMsg)
//   if (this._recipients[rootHash]) return

//   this._recipients[rootHash] = receiveMsg
//   app.put('/send/' + rootHash + '/:fromRootHash', this._onmessage)
// }

function sendErr(res, code, err) {
  if (typeof code !== 'number') {
    err = code;
    code = err.code;
  }

  code = typeof code === 'undefined' ? 501 : code;
  res.status(code).json({
    message: err.message || err
  });
}