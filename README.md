# Trust in Motion (TiM)

Tradle's P2P chat with plaintext and structured messages, and the ability to externalize messages onto a blockchain (currently bitcoin's).

_this module is used by [Tradle](https://github.com/tradle/about/wiki)_  
_this npm module's name was graciously donated by [Sean Robertson](https://www.npmjs.com/~spro)_

This module comprises a wrapper around and an API to a number of Tradle components, and a datalog of all activity.  A UI is developed separately. Currently we focus on [React Native based UI](https://github.com/pgmemk/TiM) (on iOS and hopefully soon on Android), see the [preview video](https://www.youtube.com/watch?v=S66T0dPNn5I) and the [screenshots](https://docs.google.com/document/d/1vPR4tt8S-hZAG17Z_PbKwh_Pe4mvzhAzGBZiVpfbwqs). 

Prior to React Native UI for Tim we developed a [Chrome App UI](https://github.com/tradle/chromeapp), see this [Identity verification video](https://www.youtube.com/watch?v=rrt8U4M-yMg) and a [work completion video](https://www.youtube.com/watch?v=mRnaw4pdifA). And prior to that we developed a very cool node-webkit-based [craigslist on-chain concept app](https://github.com/tradle/craigslist-on-chain) on TiM, but it is very much behind now. Let us know if you are interested in these environments.

## TiM uses the following Tradle components:

### [zlorp](https://github.com/tradle/zlorp)

[Zlorp](https://github.com/tradle/zlorp) is just the core chat module. It uses OTR for secure sessions (later will add support for Axolotl, used in [TextSecure](https://github.com/WhisperSystems/TextSecure/) and [Pond](https://pond.imperialviolet.org/)). Peer discovery today is done via bittorrent-dht. But DHT's regular announce messages leak IP/Port, so we will see if we can use BEP 44 to encrypt them. Zlorp provides UDP NAT traversal (firewall hole-punching), and a direct connection via rUDP (later via uTP). We plan to further investigate anonymous packet delivery, possibly via I2P (TOR does not support UDP).

### [bitkeeper-js](https://github.com/tradle/bitkeeper-js)

[Bitkeeper](https://github.com/tradle/bitkeeper-js) module uses [WebTorrent](https://github.com/feross/webtorrent) for storing and ensuring replication of arbitrary files. In Tradle, bitkeeper is used to store the original (encrypted) versions of on-chain objects (structured messages).

### [chained-obj](https://github.com/tradle/chained-obj)

[Chained-obj](https://github.com/tradle/chained-obj) is object builder and parser. Currently uses multipart to store arbitrary JSON data + attachments. These objects are later encrypted and put on-chain.

### [bitjoe-js](https://github.com/tradle/bitjoe-js) (to be renamed to: chainwriter)

A collection of requests that can be used to put an object "on chain": encrypt an object for its recipients, store/seed it from a bitkeeper node and put private links on blockchain.

### [tradle-constants](https://github.com/tradle/tradle-constants)

Wait for it...a bunch of constants

### [tradle-utils](https://github.com/tradle/tradle-utils)

A small set of crypto and torrent-related functions used by a number of Tradle components

### [Identity](https://github.com/tradle/identity)

[Identity](https://github.com/tradle/identity) is wrapper around an OpenName-compatible Identity schema. Used for building/parsing/validating identity objects.

### [kiki](https://github.com/tradle/kiki)

[kiki](https://github.com/tradle/kiki) Wrappers for DSA, EC, Bitcoin and other keys to provide a common API for signing/verifying/importing/exporting.

### [chainloader](https://github.com/tradle/chainloader)

Parses bitcoin transactions, attempts to process embedded links, loads intermediate files and original files from a bitkeeper node, decrypts and returns files and metadata. Implements stream.Transform.

### [simple-wallet](https://github.com/tradle/simple-wallet)

One-key [common blockchain](https://github.com/common-blockchain/common-blockchain) based wallet.

### [tradle-tx-data](https://github.com/tradle/tx-data)

For building/parsing bitcoin-transaction-embedded data

### [tradle-verifier](https://github.com/tradle/tradle-verifier)

Plugin-based verifier for on-chain objects. Implements several default plugins:  
    Signature Check  
    Identity verification  
    Previous Version verification  

## Exports

### Datalog

This module uses a datalog, a log-based database of experienced activity that you can write to and use to bootstrap your own databases from.

### Messaging API

Details to follow

## Usage

### Initialization

```js
var leveldown = require('leveldown') // or asyncstorage-down or whatever you're using
var DHT = require('bittorrent-dht') // use tradle/bittorrent-dht fork
var Blockchain = require('cb-blockr') // use tradle/cb-blockr fork
var Identity = require('midentity').Identity
var Bitkeeper = require('bitkeeper-js')
var kiki = require('kiki')
var Wallet = require('simple-wallet')
var Tim = require('tim')

// Setup components:
var networkName = 'testnet'
var blockchain = new Blockchain(networkName)
//   Create an identity or load an existing one (see midentity readme):
var jack = Identity.fromJSON(jackJSON)
//   a dht node is your first point of contact with the outside world
var dht = new DHT()
//   a keeper stores/replicates your encrypted files
var keeper = new Bitkeeper({
  dht: dht
})

var wallet = new Wallet({
  networkName: networkName,
  blockchain: blockchain,
  // temporary insecure API
  priv: jackBTCKey 
})

var tim = new Tim({
  pathPrefix: 'tim', // for playing nice with other levelup-based storage
  leveldown: leveldown,
  networkName: networkName,
  identity: jack,
  // temporary insecure API
  identityKeys: jackPrivKeys, 
  keeper: keeper,
  wallet: wallet,
  blockchain: blockchain,
  port: 12345, // your choice
  // optional
  syncInterval: 60000 // how often to bother cb-blockr
})
```

### Publishing your identity

```js
tim.publishMyIdentity()
```

### Sending messages

```js

tim.send({
  msg: Object|String|Buffer,
  // record message on chain
  chain: true,
  // send message p2p
  deliver: true,
  to: [{ 
    fingerprint: 'a fingerprint of a key of an identity known to you' 
  }]
})

```

### Sharing existing messages (via the blockchain)

```js

var constants = require('tradle-constants')
var curHash = '...' // the "hash" of the existing message
var shareOpts = {
  // record message on chain
  chain: true,
  // send message p2p
  deliver: true,
  to: [{
    fingerprint: 'a fingerprint of a key of an identity known to you'
  }]
}

shareOpts[constants.CUR_HASH] = curHash
tim.share(shareOpts)

```

### Publishing on chain

Same as sending a message, but use tim.publish instead of tim.send

```js

tim.publish({
  msg: Object|String|Buffer,
  to: [{
    fingerprint: 'a [bitcoin] address where to record the link to the object' 
  }]
})

```

## Messages

```js
var db = tim.messages() // read-only levelup instance

// e.g.
db.createValueStream()
  .on('data', function (err, msg) {
    // issue tim.lookupObject(msg) to get decrypted metadata and contents
  })
```

## Identities (loaded from chain)

```js
var db = tim.identities() // read-only levelup instance

// e.g.
db.createValueStream()
  .on('data', function (err, identityJSON) {
    // do something with "identity"
    // console.log('yo', identityJSON.name.firstName)
  })

// additional convenience methods
db.byRootHash(identityRootHash, callback)
db.byFingerprint(fingerprint, callback)
```

## Events

### tim.on('ready', function () {...})

Tim's ready to do stuff

### tim.on('chained', function (info) {...}

An object was successfully put on chain<sup>1</sup>

### tim.on('unchained', function (info) {...}

An object was read off chain<sup>1</sup>

### tim.on('message', function (info) {...}

A message was received peer-to-peer<sup>1</sup>

### tim.on('resolved', function (info) {...}

An object was both received peer-to-peer and read from the chain<sup>1</sup>

<sup>1</sup> Note: does NOT contain chained-object contents. Use tim.lookupObject(info) to obtain those.
