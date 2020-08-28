'use strict'

const _get = require('lodash/get')

function logOperation(op, operationEnteredAt) {
  const operationUpsertedAt = new Date(new Date().getTime())
  let operationUpdatedAt
  let opId

  if (op && op.o && op.op) {
    opId = _get(op, 'o._id') || _get(op, 'o2._id')
    operationUpdatedAt = _get(op, 'o.updatedAt') || _get(op, 'o.$set.updatedAt')

    if (opId && operationUpdatedAt && operationEnteredAt) {
      console.log('Operation processed for _id', opId)
      console.log('Operation updated at', operationUpdatedAt)
      console.log(
        'Operation total delay (Difference)',
        Math.abs(operationUpsertedAt - operationUpdatedAt) / 1000,
        'seconds'
      )
      console.log('Operation process delay', Math.abs(operationUpsertedAt - operationEnteredAt) / 1000, 'seconds')
      console.log('--------')
    } else {
      console.log('Operation processed', op.op, op.o)
      console.log('--------')
    }
  }
}

module.exports = logOperation
