'use strict'

const sql = require('../interfaces/sql')
const lockUtil = require('../utils/lock')

async function deleteInner(spec, pgPool, doc) {
  const { table } = spec.target
  const { _id } = doc

  const pgClient = await pgPool.connect()

  try {
    const deleteResult = await sql.query(pgClient, {
      name: `delete-d-${spec.ns}`,
      text: `DELETE FROM "${table}" WHERE id = $1`,
      values: [_id.toString()]
    })

    if (deleteResult.rowCount === 0) {
      console.warn(
        `[${spec.ns}] Huh? document with id: ${_id} was deleted in mongo, but was not found in postgres db`
      )
    }
  } finally {
    pgClient.release()
  }
}

function getDeleteForConcurrency(concurrency) {
  return concurrency > 1
    ? lockUtil.lockify(
        deleteInner,
        lockUtil.getPromisifiedLock(),
        (spec) => spec.ns
      )
    : deleteInner
}

module.exports = {
  getDeleteForConcurrency
}
