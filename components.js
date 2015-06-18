
var assert = require('assert')
var fs = require('fs')
var levelup = require('levelup')
var leveldown = require('leveldown')
var typeforce = require('typeforce')
var leveldown = require('leveldown')
var ChainLoader = require('chainloader')
var ChainDB = require('chaindb')
var Identity = require('midentity').Identity
var ChainWriter = require('bitjoe-js')
var Fakechain = require('blockloader/fakechain')
var Zlorp = require('zlorp')
var Keeper = require('bitkeeper-js')
var Wallet = require('simple-wallet')
var extend = require('extend')
var PREFIX = 'tradle'
// var conf = require('./conf')

module.exports = function loadComponents (options, cb) {
  assert('identity' in options &&
    'blockchain' in options &&
    'networkName' in options &&
    'keeper' in options &&
    'dht' in options)

  var networkName = options.networkName
  var keeper = options.keeper
  var dht = options.dht
  var identity = options.identity
  var blockchain = options.blockchain
  var wallet = options.wallet || new Wallet({
    networkName: networkName,
    blockchain: blockchain,
    priv: identity.keys({
      networkName: networkName,
      type: 'bitcoin'
    })[0].priv()
  })

  var zlorp = new Zlorp({
    name: identity.name(),
    available: true,
    leveldown: leveldown,
    port: options.port,
    dht: dht,
    key: identity.keys({
      type: 'dsa'
    })[0].priv()
  })

  var chainwriter = new ChainWriter({
    wallet: wallet,
    keeper: keeper,
    networkName: networkName,
    minConf: 0,
    prefix: PREFIX
  })

  var chainloader = new ChainLoader({
    keeper: keeper,
    networkName: networkName,
    prefix: PREFIX
  })

  var chaindb = new ChainDB({
    path: './' + options.pathPrefix + '-chaindb.db',
    networkName: networkName,
    leveldown: leveldown,
    chainloader: chainloader,
    blockchain: blockchain,
    syncInterval: 1000
  })

  chaindb.run()

  zlorp.once('ready', function () {
    cb(null, {
      leveldown: leveldown,
      blockchain: blockchain,
      networkName: networkName,
      keeper: keeper,
      wallet: wallet,
      zlorp: zlorp,
      identity: identity,
      chaindb: chaindb,
      chainloader: chainloader,
      chainwriter: chainwriter
    })
  })
}

function chainWithTed () {
  var tedBlockHexPath = path.join(__dirname, '/fixtures/blocks/' + FIRST_BLOCK)
  var tedBlock = fs.readFileSync(tedBlockHexPath)
  return new Fakechain({ networkName: 'testnet' })
    .addBlock(tedBlock, FIRST_BLOCK)
}
