'use strict'

const winston = require('winston')
const copyFrom = require('pg-copy-streams').from
const Rx = require('rxjs')
const Bluebird = require('bluebird')
const Readable = require('stream').Readable
const RxNode = require('../rx-node')
const schema = require('../utils/Schema')
const sql = require('../interfaces/sql')
const replicateDeletions = require('./replicateDeletions')

module.exports = async function importCollections (mongoConnection, pgPool, specs, isIncremental) {
  const collectionPromises = []

  for (const spec of specs) {
    // use a different postgres client for each collection so we can have multiple isolated transactions in parallel
    const pgClient = await pgPool.connect()

    const collectionPromise = importCollection(mongoConnection, pgClient, spec, isIncremental)

    // release client when done
    Bluebird.resolve(collectionPromise)
      .finally(() => pgClient.release())

    collectionPromises.push(collectionPromise)
  }

  return Promise.all(collectionPromises).then(collections => collections.length)
}

async function importCollection (mongoConnection, pgClient, spec, isIncremental) {
  const tableBody = schema.getTableBody(spec)
  let cursor

  if (isIncremental && spec.keys.incrementalReplicationKey) {
    // incremental import is possible.
    await sql.query(pgClient, `CREATE TABLE IF NOT EXISTS "${spec.target.table}" (${tableBody})`)

    const irk = spec.keys.incrementalReplicationKey
    const result = await sql.query(pgClient, `SELECT MAX("${irk.name}") FROM "${spec.target.table}"`)

    const lastReplicationKeyValue = result.rows[0].max

    if (lastReplicationKeyValue) {
      cursor = spec.source.getCollection(mongoConnection)
        .find({ updatedAt: { $gt: lastReplicationKeyValue } })

      await importDocs(spec, cursor, pgClient, incrementalImport)
      return
    }
  }

  await sql.query(pgClient, `DROP TABLE IF EXISTS "${spec.target.table}"`)
  await sql.query(pgClient, `CREATE TABLE "${spec.target.table}" (${tableBody})`)

  cursor = spec.source.getCollection(mongoConnection)
    .find({}, { fields: spec.source.fields })

  await importDocs(spec, cursor, pgClient, fullImport)

  if (spec.target.tableInit) {
    winston.info(`[${spec.ns}] Initializing table "${spec.target.table}"...`)

    const queries = spec.target.tableInit
    for (let i = 0; i < queries.length; i++) {
      winston.info(`[${spec.ns}] [${i + 1}/${queries.length}] executing:`, queries[i])
      const result = await sql.query(pgClient, queries[i])
      winston.info(`[${spec.ns}] [${i + 1}/${queries.length}] result:`, result)
    }
  }
}

/**
 * @param {MongoDB.cursor} cursor ** a mongodb cursor
 * @param {Function} importFn ** the function used to import
 */
async function importDocs (spec, cursor, pgClient, importFn) {
  let docs = []
  let acc = 0
  let more = await cursor.hasNext()

  while (more) {
    const doc = await cursor.next()
    docs.push(doc)

    more = await cursor.hasNext()

    if (docs.length === 1000 || !more) {
      const table = {
        body: schema.getTableBody(spec),
        name: spec.target.table,
        columns: schema.getColumnNames(spec),
        placeholders: schema.getPlaceholders(spec)
      }

      const result = await importFn(spec, table, docs, pgClient)
      acc += docs.length
      docs = []

      winston.info(`[${spec.ns}] Imported ${acc} rows...`)
    }
  }
}

async function fullImport (spec, table, docs, pgClient) {
  winston.info(`[${spec.ns}] Importing all documents...`)
  // console.log('docs', docs)
  const { name, columns, placeholders } = table
  try {
    const destinyStream = pgClient.query(copyFrom(
      `COPY "${name}" (${columns.join(',')}) FROM STDIN`
    ))

    const data = docs.map(doc => schema.toTextFormat(spec, doc))
    const sourceStream = new Readable()
    sourceStream._read = function noop () {}
    sourceStream.push(data.join(''))
    sourceStream.push(null)

    sourceStream.pipe(destinyStream)

    const importResult = await endOfStream(destinyStream)

    return importResult
  } catch (error) {
    winston.warn(`[${spec.ns}] Bulk insert error, attempting individual inserts...`, error)
    winston.debug(`[${spec.ns}] Bulk insert error, attempting individual inserts...`, error)

    try {
      await sql.query(pgClient, 'BEGIN')

      for (const doc of docs) {
        await sql.query(pgClient, {
          name: `import-${spec.ns}`,
          text: `INSERT INTO "${name}"
                 (${columns.join(',')})
                 VALUES (${placeholders.join(',')})`,

          values: Array.from(schema.transformValues(spec, doc))
        })
      }

      await sql.query(pgClient, 'COMMIT')
    } catch (error) {
      await sql.query(pgClient, 'ROLLBACK')
      throw error
    }
  }
}

async function incrementalImport (spec, table, docs, pgClient) {
  winston.info(`[${spec.ns}] Importing new and updated documents`)

  const { name, columns, body, placeholders } = table
  try {
    await sql.query(pgClient,
      `CREATE TEMPORARY TABLE ${name}_copy_temp (${body})`
    )

    const destinyStream = pgClient.query(copyFrom(
      `COPY "${name}_copy_temp" (${columns.join(',')}) FROM STDIN`
    ))

    const data = docs.map(doc => schema.toTextFormat(spec, doc))
    const sourceStream = new Readable()
    sourceStream._read = function noop () {}
    sourceStream.push(data.join(''))
    sourceStream.push(null)

    sourceStream.pipe(destinyStream)
    await endOfStream(destinyStream)
    await sql.query(pgClient, {
      name: `import-delete-${spec.ns}`,
      text: `DELETE FROM ${name} 
              USING ${name}_copy_temp 
              WHERE ${name}.id = ${name}_copy_temp.id`
    })
    const updateResult = await sql.query(pgClient, {
      name: `import-upsert-${spec.ns}`,
      text: `INSERT INTO ${name} (${columns.join(',')}) 
              SELECT ${columns.join(',')} 
              FROM ${name}_copy_temp`
    })

    await sql.query(pgClient, `DROP TABLE ${name}_copy_temp`)

    winston.info(`[${spec.ns}] Updated ${updateResult.rowCount} rows...`)
    return updateResult
  } catch (error) {
    winston.warn(`[${spec.ns}] Bulk insert error, attempting individual inserts...`)
    winston.debug(`[${spec.ns}] Bulk insert error, attempting individual inserts...`, error)

    try {
      await sql.query(pgClient, 'BEGIN')

      for (const doc of docs) {
        const conditions = []
        for (const i in table.columns) {
          conditions.push(`${columns[i]} = ${placeholders[i]}`)
        }

        const keys = []
        for (const key of spec.keys.primaryKey) {
          keys.push(key.name)
        }

        await sql.query(pgClient, {
          name: `import-upsert-indiv-${spec.ns}`,
          text: `INSERT INTO ${spec.target.table} (${columns.join(',')}) 
                 VALUES (${placeholders.join(',')}) 
                 ON CONFLICT (${keys.join(',')}) DO UPDATE 
                 SET ${conditions.join(',')}
                 WHERE ${spec.target.table}.id = $1`,

          values: Array.from(schema.transformValues(spec, doc))
        })
      }

      await sql.query(pgClient, 'COMMIT')
    } catch (error) {
      await sql.query(pgClient, 'ROLLBACK')
      throw error
    }
  }
}

// helper:

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
