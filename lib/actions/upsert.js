'use strict'

const winston = require('winston')

const schema = require('../utils/Schema')
const sql = require('../interfaces/sql')
const lockUtil = require('../utils/lock')

async function upsertInner (spec, pgPool, doc) {
  const { table, columns } = spec.target

  const columnNames = schema.getColumnNames(spec)
  const placeholders = schema.getPlaceholders(spec)

  let i = 0
  const updates = []
  const whereClauses = []
  const values = []

  for (const value of schema.transformValues(spec, doc)) {
    const eq = `${columnNames[i]} = ${placeholders[i]}`

    if (i < columns.length && columns[i].isPrimaryKey) {
      whereClauses.push(eq)
    } else {
      updates.push(eq)
    }

    values.push(value)

    i++
  }

  const pgClient = await pgPool.connect()

  try {
    // use prepared statements so that pg can cache them
    const updateResult = await sql.query(pgClient, {
      name: `upsert-u-${spec.ns}`,
      text: `UPDATE "${table}" SET ${updates.join(', ')} WHERE ${whereClauses.join(' AND ')}`,
      values
    })

    if (updateResult.rowCount === 0) {
      await sql.query(pgClient, {
        name: `upsert-i-${spec.ns}`,
        text: `INSERT INTO "${table}" (${columnNames.join(',')}) VALUES (${placeholders.join(',')})`,
        values
      })
    } else if (updateResult.rowCount > 1) {
      winston.warn(`Huh? Updated ${updateResult.rowCount} > 1 rows: upsert(${table}, ${JSON.stringify(doc)})`)
    }
  } finally {
    pgClient.release()
  }
}

function getUpsertForConcurrency (concurrency) {
  return concurrency > 1
    // lock upsert by ns to avoid concurrent upserts on the same collection
    ? lockUtil.lockify(upsertInner, lockUtil.getPromisifiedLock(), spec => spec.ns)
    : upsertInner
}

module.exports = {
  getUpsertForConcurrency
}
