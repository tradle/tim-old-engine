
var WrappedError = require('error/wrapped')

module.exports = {
  NotEnoughFundsError: WrappedError({
    type: 'notEnoughFunds',
    message: 'current balance: {balance}',
    balance: '[unspecified]',
    timestamp: null
  }),
  ChainWriteError: WrappedError({
    type: 'chainWriteError',
    message: 'tx failed: {origMessage}',
    timestamp: null
  })
}
