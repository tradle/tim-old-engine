
var groupOffset = 0
var types = module.exports = {}

addGroup({
  tx: 0
})

addGroup({
  msg: {
    new: 0,
    stored: 1,
    sent: 2,
    delivered: 3,
    received: 4
  }
})

addGroup({
  chain: {
    readSuccess: 0,
    readError: 1,
    writeSuccess: 2,
    writeError: 3
  }
})

function addGroup (group) {
  groupOffset += 1000
  enumerate(group, 0)
  for (var p in group) {
    types[p] = group[p]
  }
}

function enumerate (group, offset) {
  for (var p in group) {
    var sub = group[p]
    if (typeof sub === 'number') {
      group[p] += groupOffset
    } else {
      enumerate(sub, offset)
    }
  }
}
