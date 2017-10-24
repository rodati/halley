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

  compare(mongoStock, pgStock.rows)
}

async function compare (mongoArray, pgArray, pgClient, table) {
  if (mongoArray.length === 0 && pgArray.length !== 0) {
    try {
      sql.query(pgClient, 'BEGIN')

      while (pgArray.length) {
        const doc = pgArray.pop()
        await sql.query(pgClient, `DELETE * FROM ${table} WHERE id = ${doc._id}`)
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

  const index = mongoArray.length / 2

  const mongoLeft = mongoArray.splice(0, index)
  const mongoRight = mongoArray

  const pgLeft = pgArray.splice(0, index)
  const pgRight = pgArray

  // fix deviation caused by deletions:
  // console.log('pgright', pgRight[0])
  // console.log('mongoRight', mongoRight[0])

  while (mongoRight[0]._id != pgRight[0].id) {
    console.log('onde?', mongoRight[0]._id, pgRight[0].id)
    pgLeft.unshift(pgRight.shift())
  }

  compare(mongoLeft, pgLeft, pgClient, table)
  compare(mongoRight, pgRight, pgClient, table)
}

module.exports = replicateDeletions
