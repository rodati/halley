'use strict'

const debug = require('debug')('halley')
const Bluebird = require('bluebird')
const { DateTime, Duration } = require('luxon')

const schema = require('../utils/Schema')
const sql = require('../interfaces/sql')
const { upsert } = require('./upsertSingle')
const copyBatch = require('./copyBatch')

module.exports = async function importCollections (mongoClient, pgPool, specs, options) {
  const collectionPromises = []

  for (const spec of specs) {
    // use a different postgres client for each collection so we can have multiple isolated transactions in parallel
    const pgClient = await pgPool.connect()

    const collectionPromise = importCollection(mongoClient, pgClient, spec, options)

    // release client when done
    Bluebird.resolve(collectionPromise)
      .finally(() => pgClient.release())

    collectionPromises.push(collectionPromise)
  }

  return Promise.all(collectionPromises).then(collections => collections.length)
}

async function importCollection (mongoClient, pgClient, spec, options) {
  const tableBody = schema.getTableBody(spec)

  const tryIncremental = options.incrementalImport
  const irk = spec.keys.incrementalReplicationKey

  if (tryIncremental && irk) {
    // incremental import is possible
    await sql.query(pgClient, `CREATE TABLE IF NOT EXISTS "${spec.target.table}" (${tableBody})`)

    const result = await sql.query(pgClient, `SELECT MAX("${irk.name}") FROM "${spec.target.table}"`)

    const lastReplicationKeyValue = result.rows[0].max

    const replicationKeyName = irk.source
    if (lastReplicationKeyValue) {
      const cursor = spec.source.getCollection(mongoClient)
        .find({ [replicationKeyName]: { $gt: lastReplicationKeyValue } })

      console.log(`[${spec.ns}] Importing new and updated documents...`)
      await importDocs(spec, cursor, pgClient, incrementalImport, options)
      return
    }
  }

  if (!options.dropTable) {
    await sql.query(pgClient, `DROP TABLE IF EXISTS "${spec.target.table}"`)
  }

  await sql.query(pgClient, `CREATE TABLE IF NOT EXISTS "${spec.target.table}" (${tableBody})`)

  const irl = spec.keys.incrementalReplicationLimit

  const query = {}
  if (irk && irl) {
    if (!irk.type.includes('timestamp')) {
      console.log(`[${spec.ns}] Replication limit does not support keys of types other than TIMESTAMP`)
    } else {
      const dateLimit = DateTime.local().minus(Duration.fromISO(irl)).toJSDate()
      query[irk.source] = {
        $gte: dateLimit
      }
      console.log(`[${spec.ns}] Importing all documents from ${irl}...`)
    }
  } else {
    console.log(`[${spec.ns}] Importing all documents...`)
  }

  const cursor = spec.source.getCollection(mongoClient)
    .find(query, { projection: spec.source.projection })

  await importDocs(spec, cursor, pgClient, fullImport, options)

  if (spec.target.tableInit) {
    console.log(`[${spec.ns}] Initializing table "${spec.target.table}"...`)

    const queries = spec.target.tableInit
    for (let i = 0; i < queries.length; i++) {
      console.log(`[${spec.ns}] [${i + 1}/${queries.length}] executing:`, queries[i])
      const result = await sql.query(pgClient, queries[i])
      debug(`[${spec.ns}] [${i + 1}/${queries.length}] result:`, result)
    }
  }
}

/**
 * @param {MongoDB.cursor} cursor ** a mongodb cursor
 * @param {Function} importFn ** the function used to import
 */
async function importDocs (spec, cursor, pgClient, importFn, options) {
  let docs = []
  let acc = 0
  let more = await cursor.hasNext()
  while (more) {
    const doc = await cursor.next()
    docs.push(doc)

    more = await cursor.hasNext()

    if (docs.length === 1000 || !more) {
      await importFn(spec, docs, pgClient, options)
      acc += docs.length
      docs = []

      console.log(`[${spec.ns}] Imported ${acc} rows...`)
    }
  }
}

async function fullImport (spec, docs, pgClient, options) {
  try {
    const importResult = await copyBatch(spec, docs, pgClient)
    return importResult
  } catch (error) {
    console.log(`[${spec.ns}] Bulk insert error, attempting individual inserts...`, error)

    const tableName = spec.target.table
    const columns = schema.getColumnNames(spec)
    const placeholders = schema.getPlaceholders(spec)

    for (const doc of docs) {
      try {
        await sql.query(pgClient, {
          name: `import-${spec.ns}`,
          text: `INSERT INTO "${tableName}"
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
          console.log(`[${spec.ns}] Ignored PK violation: ${error.innerError.detail}`)
        } else {
          const err = new Error('Individual insertion of documents failed')
          const docId = doc._id.toString()

          err.innerError = error
          err.extraData = {
            spec,
            docId
          }
          if (options.continueOnError) {
            console.log(err)
          } else {
            throw err
          }
        }
      }
    }
  }
}

async function incrementalImport (spec, docs, pgClient, options) {
  const tableName = spec.target.table
  const tempTableName = `${tableName}_copy_temp`
  const body = schema.getTableBody(spec)
  const columns = schema.getColumnNames(spec)

  try {
    await sql.query(pgClient, `CREATE TEMPORARY TABLE "${tempTableName}" (${body})`)

    await copyBatch(spec, docs, pgClient, tempTableName)

    const deleteClauses = []
    for (const key of spec.keys.primaryKey) {
      const clause = `"${tableName}"."${key.name}" = "${tempTableName}"."${key.name}"`
      deleteClauses.push(clause)
    }

    await sql.query(pgClient, {
      name: `import-delete-${spec.ns}`,
      text: `DELETE FROM "${tableName}"
              USING "${tempTableName}"
              WHERE ${deleteClauses.join(' AND ')}`
    })

    const updateResult = await sql.query(pgClient, {
      name: `import-upsert-${spec.ns}`,
      text: `INSERT INTO "${tableName}" (${columns.join(',')})
              SELECT ${columns.join(',')}
              FROM "${tempTableName}"`
    })

    await sql.query(pgClient, `DROP TABLE "${tempTableName}"`)

    console.log(`[${spec.ns}] Updated ${updateResult.rowCount} rows...`)
    return updateResult
  } catch (err) {
    console.warn(`[${spec.ns}] Bulk insert error, attempting individual inserts...`, err)

    await sql.query(pgClient, 'BEGIN')
    for (const doc of docs) {
      try {
        await upsert(spec, pgClient, doc)
      } catch (error) {
        const err = new Error('Individual insertion of docs failed')
        err.innerError = error
        const docId = doc._id.toString()
        err.extraData = {
          spec,
          docId
        }
        if (options.continueOnError) {
          console.log(err)
        } else {
          throw err
        }
      }
    }

    await sql.query(pgClient, 'COMMIT')
  }
}
