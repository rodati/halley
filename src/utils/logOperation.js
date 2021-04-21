'use strict'

const _get = require('lodash/get')

function logOperation(op, operationEnteredAt, streamName) {
  const operationUpsertedAt = new Date(new Date().getTime())
  let operationUpdatedAt
  let opId
  let opType

  if (op) {
    opId = _get(op, 'o._id') || _get(op, 'o2._id') || _get(op, 'documentKey._id')
    opType = _get(op, 'operationType')
    operationUpdatedAt =
      _get(op, 'o.updatedAt') ||
      _get(op, 'o.$set.updatedAt') ||
      _get(op, 'updateDescription.updatedFields.updatedAt') ||
      _get(op, 'fullDocument.updatedAt')

    if (opId && operationUpdatedAt && operationEnteredAt && opType) {
      console.log(`Operation ${opType} processed for _id ${opId}`)
      console.log(`Operation updated at ${operationUpdatedAt}`)
      console.log(
        `Operation (${streamName}) total delay (Difference) ${
          Math.abs(operationUpsertedAt - operationUpdatedAt) / 1000
        } seconds`
      )
      console.log(`Operation process delay ${Math.abs(operationUpsertedAt - operationEnteredAt) / 1000} seconds`)
      console.log('--------')
    } else {
      if (op.op && op.o) {
        console.log('Operation processed', op.op, op.o)
      } else {
        console.log('Operation processed', op)
      }
      console.log('--------')
    }
  }
}

module.exports = logOperation
