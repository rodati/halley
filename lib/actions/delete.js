'use strict'

const winston = require('winston')

const sql = require('../interfaces/sql')
const lockUtil = require('../utils/lock')

async function deleter (spec, pgClient, doc) {
  const { table } = spec.target
  const { _id } = doc

  try {
    const deleteResult = await sql.query(pgClient, {
      name: `delete-d-${spec.ns}`,
      text: `DELETE FROM "${table}" WHERE id = $1`,
      values: [_id.toString()]
    })

    if (deleteResult.rowCount === 0) {
      winston.warn(`Huh? document with id: ${_id} was deleted in mongo, but was not found in postgres db`)
    }
  } catch (error) {
    sql.query(pgClient, 'ROLLBACK')
    throw error
  }
}

async function lockeableDelete (spec, pgPool, doc) {
  const pgClient = await pgPool.connect()
  try {
    deleter(spec, pgClient, doc)
  } finally {
    pgClient.release()
  }
}

function getDeleteForConcurrency (concurrency) {
  return concurrency > 1
    ? lockUtil.lockify(deleter, lockUtil.getPromisifiedLock(), spec => spec.ns)
    : deleter
}

module.exports = {
  getDeleteForConcurrency,
  deleter
}
