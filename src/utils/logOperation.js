'use strict'

const _get = require('lodash/get')

function logOperation(op) {
  const now = new Date(new Date().getTime())
  let opId
  let opTime

  if (op && op.o && op.op) {
    opId = _get(op, 'o._id') || _get(op, 'o2._id')
    opTime = _get(op, 'o.updatedAt') || _get(op, 'o.$set.updatedAt')

    console.log('Now', now)

    if (opId && opTime) {
      console.log('Operation time', opTime)
      console.log('Processing operation', op.op, op.o, 'with date', opTime, 'for _id', opId)
      console.log('Difference', Math.abs(now - opTime) / 1000, 'seconds')
    } else {
      console.log('Processing operation', op.op, op.o)
    }
  }
}

module.exports = logOperation
