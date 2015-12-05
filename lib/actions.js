'use strict';

module.exports = {
  chain: {
    name: 'chain',
    maxErrors: Infinity
  },
  unchain: {
    name: 'unchain',
    maxErrors: 3
  },
  send: {
    name: 'send',
    maxErrors: 10
  },
  receive: {
    name: 'receive',
    maxErrors: 3
  }
};

// for (var action in actions) {
//   actions[action].name = action
// }