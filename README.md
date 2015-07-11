# Trust in Motion (TiM)

Tradle's P2P chat with plaintext and structured messages, and the ability to externalize messages onto a blockchain (currently bitcoin's).

_this module is used by [Tradle](https://github.com/tradle/about/wiki)_  
_this npm module's name was graciously donated by [Sean Robertson](https://www.npmjs.com/~spro)_

This module comprises a wrapper around and an API to a number of Tradle components, and a datalog of all activity.  A UI is developed separately. Currently we focus on [React Native based UI](https://github.com/pgmemk/TiM) (on iOS and hopefully soon on Android), see the [preview video](https://www.youtube.com/watch?v=S66T0dPNn5I) and the [screenshots](https://docs.google.com/document/d/1vPR4tt8S-hZAG17Z_PbKwh_Pe4mvzhAzGBZiVpfbwqs). 

Prior to React Native UI for Tim we developed a [Chrome App UI](https://github.com/tradle/chromeapp), see this [Identity verification video](https://www.youtube.com/watch?v=rrt8U4M-yMg) and a [work completion video](https://www.youtube.com/watch?v=mRnaw4pdifA). And prior to that we developed a very cool node-webkit-based [craigslist on-chain concept app](https://github.com/tradle/craigslist-on-chain) on TiM, but it is very much behind now. Let us know if you are interested in these environments.

## TiM uses the following Tradle components:

### zlorp

[Zlorp](https://github.com/tradle/zlorp) is just the core chat module. It uses OTR for secure sessions (later will add support for Axolotl, used in [TextSecure](https://github.com/WhisperSystems/TextSecure/) and [Pond](https://pond.imperialviolet.org/)). Peer discovery today is done via bittorrent-dht. But DHT's regular announce messages leak IP/Port, so we will see if we can use BEP 44 to encrypt them. Zlorp provides UDP NAT traversal (firewall hole-punching), and a direct connection via rUDP (later via uTP). We plan to further investigate anonymous packet delivery, possibly via I2P (TOR does not support UDP).

### bitkeeper-js

[Bitkeeper](https://github.com/tradle/bitkeeper-js) module uses [WebTorrent](https://github.com/feross/webtorrent) for storing and ensuring replication of arbitrary files. In Tradle, bitkeeper is used to store the original (encrypted) versions of on-chain objects (structured messages).

### chained-obj

[Chained-obj](https://github.com/tradle/chained-obj) is object builder and parser. Currently uses multipart to store arbitrary JSON data + attachments. These objects are later encrypted and put on-chain.

### bitjoe-js (to be renamed to: chainwriter)

A collection of requests that can be used to put an object "on chain": encrypt an object for its recipients, store/seed it from a bitkeeper node and put private links on blockchain.

### tradle-constants

Wait for it...a bunch of constants

### tradle-utils

A small set of crypto and torrent-related functions used by a number of Tradle components

### Identity

[Identity](https://github.com/tradle/identity) is wrapper around an OpenName-compatible Identity schema. Used for building/parsing/validating identity objects.

### chainloader

Parses bitcoin transactions, attempts to process embedded links, loads intermediate files and original files from a bitkeeper node, decrypts and returns files and metadata. Implements stream.Transform.

### simple-wallet

One-key [common blockchain](https://github.com/common-blockchain/common-blockchain) based wallet.

### tradle-tx-data

For building/parsing bitcoin-transaction-embedded data

### tradle-verifier

Plugin-based verifier for on-chain objects. Implements several default plugins:  
    Signature Check  
    Identity verification  
    Previous Version verification  

## Exports

### Datalog

This module uses a datalog, a log-based database of experienced activity that you can write to and use to bootstrap your own databases from. Details will follow...

### Messaging API

Details to follow

## Usage

See test/live.js for an example
