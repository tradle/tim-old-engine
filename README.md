# Trust in Motion (TiM)

Tradle's P2P chat with plaintext and structured messages, and the ability to externalize messages onto a blockchain (currently bitcoin's).

_this module is used by [Tradle](https://github.com/tradle/about/wiki)_

This module comprises a wrapper around and an API to a number of Tradle components, and a datalog of all activity.  A UI is developed separately. Currently we focus on React Native based UI (on iOS and hopefully soon on Android). Prior to that we developed a Chrome App UI. And prior to that we developed a node-webkit UI, but it is very much behind now. Let us know if you are interested in these environments.

## Wraps the following Tradle components:

### zlorp

Peer-to-peer OTR-based chat. Discovery via bittorrent-dht, hole-punching, and chat via rUDP (later via UTP).

### bitkeeper-js

A module that uses [WebTorrent](https://github.com/feross/webtorrent) for storing and ensuring replication of arbitrary files. In Tradle, bitkeeper is used to store the original (encrypted) versions of on-chain files.

### chained-obj

An on-chain object builder and parser. Currently uses multipart to store arbitrary JSON data + attachments.

### bitjoe-js (to be renamed to: chainwriter)

A collection of requests that can be used to put an object "on chain": encrypt an object for its recipients, store/seed it from a bitkeeper node and put private links on blockchain.

### tradle-constants

Wait for it...a bunch of constants

### tradle-utils

A small set of crypto and torrent-related functions used by a number of Tradle components

### midentity

A wrapper around an OpenName-compatible Identity schema. Used for building/parsing/validating identity objects.

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
