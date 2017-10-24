'use strict'

const Bluebird = require('bluebird')

const sql = require('./sql')

async function replicateDeletions (spec, pgClient, mongoConnection, lastReplicationKeyValue) {
  const table = spec.target.table
  const [mongoStock, pgStock] = await Bluebird.all([
    spec.source.getCollection(mongoConnection)
      .find({ createdAt: { $lte: lastReplicationKeyValue } })
      .sort({ updatedAt: 1 })
      .toArray(),

    sql.query(pgClient, `SELECT * FROM ${table} ORDER BY updated_at`)
  ])

  compare(mongoStock, pgStock.rows, pgClient, table)
}

async function compare (mongoArray, pgArray, pgClient, table) {
  if (mongoArray.length === 1 && pgArray.length > 1) {
    const deletions = pgArray.filter((item) => item.id !== mongoArray[0]._id.toString())

    try {
      sql.query(pgClient, 'BEGIN')

      while (deletions.length) {
        const deletion = deletions.pop()
        await sql.query(pgClient, {
          text: `DELETE FROM ${table} WHERE id = $1`,
          values: [deletion.id]
        })
      }

      sql.query(pgClient, 'COMMIT')
    } catch (e) {
      throw e
    }

    return
  }
  if (mongoArray.length === pgArray.length) {
    return
  }

  const index = Math.ceil((mongoArray.length / 2))

  const mongoLeft = mongoArray.splice(0, index)
  const mongoRight = mongoArray

  const pgLeft = pgArray.splice(0, index)
  const pgRight = pgArray

  // fix deviation caused by deletions:
  while (mongoRight[0]._id.toString() !== pgRight[0].id) {
    pgLeft.unshift(pgRight.shift())
  }

  compare(mongoLeft, pgLeft, pgClient, table)
  compare(mongoRight, pgRight, pgClient, table)
}

module.exports = replicateDeletions