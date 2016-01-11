// var groupOffset = 0
var types = module.exports = {}
var reverse = exports.reverse = {}

;[
  {
    group: 'tx',
    events: {
      new: 0,
      confirmation: 1,
      ignore: 2,
      watch: 3
    }
  },
  {
    group: 'msg',
    events: {
      new: 0,
      // stored: 1,
      sendSuccess: 2,
      sendError: 3,
      delivered: 4,
      receivedValid: 5,
      receivedInvalid: 6,
      edit: 7
    }
  },
  {
    group: 'chain',
    events: {
      readSuccess: 0,
      readError: 1,
      writeSuccess: 2,
      writeError: 3
    }
  },
  {
    group: 'misc',
    events: {
      forget: 0,
      addIdentity: 1,
      watchAddresses: 2,
      unwatchAddresses: 3
    }
  }
].forEach(function (group, i) {
  var offset = 1000 * (i + 1)
  var g = types[group.group] = {}
  for (var event in group.events) {
    var code = offset + group.events[event]
    g[event] = code
    reverse[code] = group.group + '.' + event
  }
})

// addGroup({
//   tx: {
//     new: 0,
//     confirmation: 1
//   }
// })

// addGroup({
//   msg: {
//     new: 0,
//     // stored: 1,
//     sendSuccess: 2,
//     sendError: 3,
//     delivered: 4,
//     receivedValid: 5,
//     receivedInvalid: 6
//   }
// })

// addGroup({
//   chain: {
//     readSuccess: 0,
//     readError: 1,
//     writeSuccess: 2,
//     writeError: 3
//   }
// })

// function addGroup (group) {
//   groupOffset += 1000
//   enumerate(group, 0)
//   for (var p in group) {
//     types[p] = group[p]
//   }
// }

// function enumerate (group, offset) {
//   for (var p in group) {
//     var sub = group[p]
//     if (typeof sub === 'number') {
//       group[p] += groupOffset
//     } else {
//       enumerate(sub, offset)
//     }
//   }
// }
