'use strict'
const Bluebird = require('bluebird')
const winston = require('winston')
const copyFrom = require('pg-copy-streams').from
const Readable = require('stream').Readable
const values = require('lodash/values')

const sql = require('../interfaces/sql')
const Oplog = require('../utils/Oplog')
const Schema = require('../utils/Schema')

async function replicateOplogDeletions (rawSpecs, pgPool, mongoConnection, concurrency) {
  winston.info(`[oplog] Searching for last sync date ...`)
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

  const oplog = new Oplog(mongoConnection)
  const docsCursor = await oplog.getNewOps(lastReplicationKeyValue)

  winston.info(`[oplog] searching oplog for delete operations since ${lastReplicationKeyValue}...`)

  if (!docsCursor) {
    winston.info(`[oplog] No new delete operations found in oplog...`)
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
        winston.info(`[${namespace}] Reached batch size. Processing batch before continuing`)
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

  winston.info(`[oplog] Finished iterating oplog operations. Deleting batches in parallel...`)

  await Bluebird.map(collections, deleteBatch.bind(null, pgPool), { concurrency })
}

async function deleteBatch (pgPool, collection) {
  const { spec, values } = collection
  if (!spec.keys.deleteKey) {
    winston.warn(`[${spec.ns}] Skipping delete operations batch because of lack of delete key. See: https://github.com/rodati/halley/issues/7`)
    return false
  }
  const key = spec.keys.deleteKey
  const name = spec.target.table
  const pgClient = await pgPool.connect()
  await sql.query(pgClient, `CREATE TEMPORARY TABLE "${name}_del_temp" ("${key}" TEXT, PRIMARY KEY ("${key}"))`)

  const targetStream = pgClient.query(copyFrom(
    `COPY "${name}_del_temp" ("${key}") FROM STDIN`
  ))
  const data = values.map(doc => Schema.escapeText(doc.toString()))
  const sourceStream = new Readable()
  sourceStream._read = function noop () {}
  sourceStream.pipe(targetStream)
  sourceStream.push(data.join('\n'))
  sourceStream.destroy()

  await endOfStream(targetStream)

  const clause = `"${name}"."${key}" = "${name}_del_temp"."${key}"`
  const result = await sql.query(pgClient, {
    name: `oplog-delete-${spec.ns}`,
    text: `DELETE FROM "${name}"
               USING "${name}_del_temp"
               WHERE (${clause})`
  })
  if (result.rowCount > 0) {
    // because we use the last updated_at value to search the oplog
    // there might be delete operations on rows that don't exist and
    // therefore they are in the 'values' array, but not on postgres
    winston.info(`[${spec.ns}] Deleted ${result.rowCount} rows...`)
  }
  await pgClient.release()
  return result
}

/**
 * @param {NodeJS.ReadableStream} stream
 */
function endOfStream (stream) {
  return new Promise(function (resolve, reject) {
    function endHandler () {
      cleanup()
      resolve()
    }

    function errorHandler (error) {
      cleanup()
      reject(error)
    }

    function cleanup () {
      stream
        .removeListener('end', endHandler)
        .removeListener('error', errorHandler)
    }

    stream
      .on('end', endHandler)
      .on('error', errorHandler)
  })
}

module.exports = replicateOplogDeletions
