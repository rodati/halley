'use strict'

const Bluebird = require('bluebird')
const copyFrom = require('pg-copy-streams').from
const Readable = require('stream').Readable
const values = require('lodash/values')

const sql = require('../interfaces/sql')
const { endOfStream } = require('../utils/stream')
const Oplog = require('../utils/Oplog')
const Schema = require('../utils/Schema')

async function replicateOplogDeletions (rawSpecs, pgPool, localDb, concurrency) {
  console.log(`[oplog] Searching for last sync date ...`)
  const pgClient = await pgPool.connect()
  const specs = values(rawSpecs)
  const selectMax = []
  const replicableSpecs = []
  for (const spec of specs) {
    // specs without a IRK will get fully imported anyway, skip them.
    const irk = spec.keys.incrementalReplicationKey
    if (irk) {
      const max = sql.query(pgClient, `SELECT MAX("${irk.name}") FROM "${spec.target.table}"`)
      replicableSpecs.push(spec.ns)
      selectMax.push(max)
    }
  }

  // TODO save this results in each spec so that this query can be avoided in incremental import
  const maxArray = await Bluebird.all(selectMax)
  const lastReplicationKeyValue = maxArray
    .map(result => result.rows[0].max)
    .filter(item => item)
    .sort((a, b) => a.getTime() > b.getTime())
    .pop()

  await pgClient.release()

  const oplog = new Oplog(localDb)

  if (!lastReplicationKeyValue) {
    console.log(`[oplog] no incremental replication key found.`)
    return
  }

  const docsCursor = await oplog.getNewOps(lastReplicationKeyValue)

  console.log(`[oplog] searching oplog for delete operations since ${lastReplicationKeyValue}...`)

  if (!docsCursor) {
    console.log(`[oplog] No new delete operations found in oplog...`)
    return
  }
  const collections = []

  while (await docsCursor.hasNext()) {
    const doc = await docsCursor.next()
    if (doc.op !== 'd') {
      // the op is not of concern of this feature
      continue
    }
    const namespace = doc.ns
    if (!replicableSpecs.includes(namespace)) {
      // the delete operation belongs to a foreign spec or to a spec without an IRK
      continue
    }
    const collection = collections.find((collection) => collection.spec.ns === namespace)
    if (collection) {
      collection.values.push(doc.o._id)
      if (collection.values.length === 1000) {
        console.log(`[${namespace}] Reached batch size. Processing batch before continuing`)
        await deleteBatch(pgPool, collection)
        delete collection.values
      }
    } else {
      collections.push({
        spec: rawSpecs[namespace],
        values: [doc.o._id]
      })
    }
  }

  console.log(`[oplog] Finished iterating oplog operations. Deleting batches in parallel...`)

  await Bluebird.map(collections, deleteBatch.bind(null, pgPool), { concurrency })
}

async function deleteBatch (pgPool, collection) {
  const { spec, values } = collection

  if (!spec.keys.deleteKey) {
    console.warn(`[${spec.ns}] Skipping delete operations batch because of lack of delete key. See: https://github.com/rodati/halley/issues/7`)
    return false
  }

  const key = spec.keys.deleteKey
  const tableName = spec.target.table
  const tempTableName = `${tableName}_del_temp`

  const pgClient = await pgPool.connect()
  await sql.query(pgClient, `CREATE TEMPORARY TABLE "${tempTableName}" ("${key}" TEXT, PRIMARY KEY ("${key}"))`)

  const targetStream = pgClient.query(copyFrom(
    `COPY "${tempTableName}" ("${key}") FROM STDIN`
  ))

  const data = values.map(doc => Schema.escapeText(doc.toString()))
  const sourceStream = new Readable()
  sourceStream._read = function noop () {}
  sourceStream.pipe(targetStream)
  sourceStream.push(data.join('\n'))
  sourceStream.destroy()

  await endOfStream(targetStream)

  const result = await sql.query(pgClient, {
    name: `oplog-delete-${spec.ns}`,
    text: `DELETE FROM "${tableName}"
               USING "${tempTableName}"
               WHERE "${tableName}"."${key}" = "${tempTableName}"."${key}"`
  })

  if (result.rowCount > 0) {
    // because we use the last updated_at value to search the oplog
    // there might be delete operations on rows that don't exist and
    // therefore they are in the 'values' array, but not on postgres
    console.log(`[${spec.ns}] Deleted ${result.rowCount} rows...`)
  }
  await pgClient.release()
  return result
}

module.exports = replicateOplogDeletions
