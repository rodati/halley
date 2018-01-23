'use strict'

const winston = require('winston')
const Bluebird = require('bluebird')

const schema = require('../utils/Schema')
const sql = require('../interfaces/sql')
const upsert = require('./upsertSingle').upsert
const copyBatch = require('./copyBatch')

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

  return Promise.all(collectionPromises)
    .then(collections => collections.length)
    .catch((error) => {
      const importType = isIncremental ? 'incremental import' : 'full import'
      const err = new Error(`Error while attempting ${importType}`)
      err.innerError = error
      throw err
    })
}

async function importCollection (mongoConnection, pgClient, spec, isIncremental) {
  const tableBody = schema.getTableBody(spec)
  let cursor

  if (isIncremental && spec.keys.incrementalReplicationKey) {
    // incremental import is possible
    await sql.query(pgClient, `CREATE TABLE IF NOT EXISTS "${spec.target.table}" (${tableBody})`)

    const irk = spec.keys.incrementalReplicationKey
    const result = await sql.query(pgClient, `SELECT MAX("${irk.name}") FROM "${spec.target.table}"`)

    const lastReplicationKeyValue = result.rows[0].max

    const replicationKeyName = irk.source
    if (lastReplicationKeyValue) {
      cursor = spec.source.getCollection(mongoConnection)
        .find({ [replicationKeyName]: { $gt: lastReplicationKeyValue } })

      winston.info(`[${spec.ns}] Importing new and updated documents`)
      await importDocs(spec, cursor, pgClient, incrementalImport)
      return
    }
  }

  await sql.query(pgClient, `DROP TABLE IF EXISTS "${spec.target.table}"`)
  await sql.query(pgClient, `CREATE TABLE "${spec.target.table}" (${tableBody})`)

  cursor = spec.source.getCollection(mongoConnection)
    .find({}, { fields: spec.source.fields })

  winston.info(`[${spec.ns}] Importing all documents...`)
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

      await importFn(spec, table, docs, pgClient)
      acc += docs.length
      docs = []

      winston.info(`[${spec.ns}] Imported ${acc} rows...`)
    }
  }
}

async function fullImport (spec, table, docs, pgClient) {
  const { name, columns, placeholders } = table
  try {
    const importResult = await copyBatch(spec, docs, pgClient)
    return importResult
  } catch (err) {
    winston.debug(`[${spec.ns}] Bulk insert error, attempting individual inserts...`, err)

    for (const doc of docs) {
      try {
        await sql.query(pgClient, {
          name: `import-${spec.ns}`,
          text: `INSERT INTO "${name}"
                  (${columns.join(',')})
                  VALUES (${placeholders.join(',')})`,
          values: Array.from(schema.transformValues(spec, doc))
        })
      } catch (error) {
        if (error.name === 'PgError' && error.innerError && error.innerError.code === '23505') {
          /**
           * PK violations during import can occur when a document is updated AFTER it's been
           * emitted by an open MongoDB cursor. It's sometimes emitted again, which results
           * in a PK violation when we try to import it into SQL.
           * 
           * So, as a workaround, we ignore PK violations during the individual import phase.
           * The updates on the document will be picked up later in the oplog.
           */
          winston.debug(`[${spec.ns}] Ignored PK violation: ${error.innerError.detail}`)
        } else {
          err.innerError = error
          throw err
        }
      }
    }
  }
}

async function incrementalImport (spec, table, docs, pgClient) {
  const { name, columns } = table
  try {
    await copyBatch(spec, docs, pgClient, true)
    const deleteClauses = []

    for (const key of spec.keys.primaryKey) {
      const clause = `"${name}"."${key.name}" = "${name}_copy_temp"."${key.name}"`
      deleteClauses.push(clause)
    }

    await sql.query(pgClient, {
      name: `import-delete-${spec.ns}`,
      text: `DELETE FROM "${name}"
              USING "${name}_copy_temp"
              WHERE ${deleteClauses.join(' AND ')}`
    })
    const updateResult = await sql.query(pgClient, {
      name: `import-upsert-${spec.ns}`,
      text: `INSERT INTO "${name}" (${columns.join(',')})
              SELECT ${columns.join(',')}
              FROM "${name}_copy_temp"`
    })

    await sql.query(pgClient, `DROP TABLE "${name}_copy_temp"`)

    winston.info(`[${spec.ns}] Updated ${updateResult.rowCount} rows...`)
    return updateResult
  } catch (error) {
    winston.warn(`[${spec.ns}] Bulk insert error, attempting individual inserts...`, error)

    await sql.query(pgClient, 'BEGIN')
    for (const doc of docs) {
      try {
        await upsert(spec, pgClient, doc)
      } catch (err) {
        error.innerError = err
        throw err
      }
    }

    await sql.query(pgClient, 'COMMIT')
  }
}
