'use strict'
const Bluebird = require('bluebird')
const winston = require('winston')
const copyFrom = require('pg-copy-streams').from
const Readable = require('stream').Readable

const sql = require('../interfaces/sql')
const Oplog = require('../utils/Oplog')
const Schema = require('../utils/Schema')

async function replicateDeleteOps (specs, pgPool, mongoConnection, concurrency) {
  const pgClient = await pgPool.connect()

  const selectMax = []
  const replicableSpecs = []
  for (const spec of specs) {
    // specs without a IRK will get fully imported anyway, skip them.
    if (spec.keys.incrementalReplicationKey) {
      const max = sql.query(pgClient, `SELECT MAX(updated_at) FROM ${spec.target.table}`)
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
  const operations = {}

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
    } else if (operations[namespace]) {
      operations[namespace].values.push(doc.o._id)

      if (operations[namespace].values.length === 1000) {
        winston.info(`[${namespace}] Reached batch size. Processing batch before continuing`)
        await deleteBatch(operations[namespace], pgPool)
        delete operations[namespace]
      }
    } else {
      operations[namespace] = {
        spec: specs.filter(spec => spec.ns === namespace)[0],
        values: [doc.o._id]
      }
    }
  }

  let results
  if (concurrency >= Object.keys(operations).length) {
    winston.info(`[oplog] Finished iterating oplog operations. Deleting batches in parallel...`)

    const deletePromises = []
    for (const namespace in operations) {
      const deletions = deleteBatch(operations[namespace], pgPool)
      deletePromises.push(deletions)
    }
    results = await Bluebird.all(deletePromises)
  } else {
    winston.info(`[oplog] Finished iterating oplog operations. Deleting batches synchronously...`)
    results = []
    for (const namespace in operations) {
      const result = await deleteBatch(operations[namespace], pgPool)
      results.push(result)
    }
  }
}

async function deleteBatch (operations, pgPool) {
  const pgClient = await pgPool.connect()
  const { spec, values } = operations
  const name = spec.target.table
  const key = spec.keys.primaryKey[0]
  const clause = `${name}.${key.name} = ${name}_del_temp.${key.name}`

  await sql.query(pgClient, `CREATE TEMPORARY TABLE ${name}_del_temp ("${key.name}" TEXT, PRIMARY KEY ("id"))`)

  const destinyStream = pgClient.query(copyFrom(
    `COPY "${name}_del_temp" ("${key.name}") FROM STDIN`
  ))

  const data = values.map(doc => Schema.transformValues(spec, doc))
  const sourceStream = new Readable()
  sourceStream._read = function noop () {}
  sourceStream.pipe(destinyStream)
  sourceStream.push(data.join(''))
  sourceStream.destroy()

  await endOfStream(destinyStream)

  winston.info(`[${spec.ns}] Deleting ${values.length} rows...`)

  const result = await sql.query(pgClient, {
    name: `oplog-delete-${spec.ns}`,
    text: `DELETE FROM ${name}
               USING ${name}_del_temp
               WHERE ${clause}`
  })
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

module.exports = replicateDeleteOps
