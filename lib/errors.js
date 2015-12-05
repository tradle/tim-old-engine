'use strict';

var TypedError = require('error/typed');
var WrappedError = require('error/wrapped');
module.exports = {
  NotEnoughFunds: WrappedError({
    type: 'notEnoughFunds',
    message: 'current balance: {balance}',
    balance: '[unspecified]',
    timestamp: null
  }),
  ChainWrite: TypedError({
    type: 'chainWrite',
    message: 'tx failed: {origMessage}',
    timestamp: null
  }),
  ChainRead: WrappedError({
    type: 'chainRead',
    message: 'read failed: {origMessage}',
    timestamp: null
  }),
  Parse: WrappedError({
    type: 'parse',
    message: 'obj parse failed: {origMessage}',
    timestamp: null
  }),
  Verification: TypedError({
    type: 'verifier',
    message: 'obj verify failed: {origMessage}',
    timestamp: null
  }),

  getMessage: function getMessage(err) {
    return err.message || err.type || err.name;
  },

  group: {
    send: 'send',
    receive: 'receive',
    chain: 'chain',
    unchain: 'unchain'
  },

  MAX_CHAIN: 10,
  MAX_UNCHAIN: 10,
  MAX_RESEND: 10
};