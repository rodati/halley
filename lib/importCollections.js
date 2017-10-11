'use strict'

const winston = require('winston')
const copyFrom = require('pg-copy-streams').from
const Rx = require('rxjs')
const Bluebird = require('bluebird')

const RxNode = require('./rx-node')
const schema = require('./schema')
const sql = require('./sql')

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
  const tableBody = getTableBody(spec)

  if (isIncremental) {
    await sql.query(pgClient, `CREATE TABLE IF NOT EXISTS "${spec.target.table}" (${tableBody})`)

    if (spec.keys.incrementalReplicationKey) {
      const irk = spec.keys.incrementalReplicationKey

      const result = await sql.query(pgClient, `SELECT MAX("${irk.name}") FROM "${spec.target.table}"`)
      const lastReplicationKeyValue = result.rows[0].max

      if (lastReplicationKeyValue) {
        winston.warn(`TODO: should pick up table ${spec.target.table} from replication key ${irk.name} with value ${lastReplicationKeyValue}`)
      }
    }

    winston.warn(`TODO do incremental import`)
    return
  } else {
    await sql.query(pgClient, `DROP TABLE IF EXISTS "${spec.target.table}"`)
    await sql.query(pgClient, `CREATE TABLE "${spec.target.table}" (${tableBody})`)
  }

  winston.info(`[${spec.ns}] Importing documents...`)

  const cursor = spec.source.getCollection(mongoConnection)
    .find({}, { fields: spec.source.fields })

  try {
    const pauser = new Rx.BehaviorSubject(false)
    const source = RxNode.fromReadableStream(cursor.stream(), pauser)

    await importDocs(spec, source, pauser, pgClient)
  } finally {
    cursor.close()
  }

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
 * @param {Rx.Observable<any>} source 
 * @param {Rx.BehaviorSubject<boolean>} pauser
 */
function importDocs (spec, source, pauser, pgClient) {
  /**
   * Import in batches so that if something fails, we can reprocess that batch
   * to narrow the issue down quickly
   */
  return source
    .bufferCount(1000)
    .do(() => pauser.next(true))
    .mergeMap(async (docs) => {
      try {
        // try to copy batch
        await copyImportDocs(spec, docs, pgClient)
      } catch (error) {
        winston.debug(`[${spec.ns}] Bulk insert error, attempting individual inserts...`, error)
        await insertImportDocs(spec, docs, pgClient)
      }

      // unpause
      pauser.next(false)

      return docs.length
    })
    .reduce((acc, batch) => {
      const total = acc + batch
      winston.info(`[${spec.ns}] Imported ${total} rows...`)
      return total
    }, 0)
    .toPromise()
}

/**
 * @param {Object[]} docs
 */
function copyImportDocs (spec, docs, pgClient) {
  const columnNames = schema.getColumnNames(spec)

  const textSource = Rx.Observable.from(docs)
    .map(doc => schema.toTextFormat(spec, doc))

  // TODO try to use FREEZE for better performance
  const copyStream = pgClient.query(copyFrom(
    `COPY "${spec.target.table}" (${columnNames.join(',')}) FROM STDIN`
  ))

  // Ignore output of copyStream because it's too verbose (several events per row).
  const endOfStreamPromise = endOfStream(copyStream)

  const subscription = RxNode.writeToStream(textSource, copyStream)
  Bluebird.resolve(endOfStreamPromise)
    .finally(() => subscription.unsubscribe())

  // Instead, return an end-of-stream promise
  return endOfStreamPromise
}

/**
 * @param {Object[]} docs
 */
async function insertImportDocs (spec, docs, pgClient) {
  const columnNames = schema.getColumnNames(spec)
  const placeholders = schema.getPlaceholders(spec)

  const name = `import-${spec.ns}`
  const text = `INSERT INTO "${spec.target.table}" (${columnNames.join(',')}) VALUES (${placeholders.join(',')})`

  try {
    await sql.query(pgClient, 'BEGIN')

    for (const doc of docs) {
      const values = Array.from(schema.transformValues(spec, doc))

      await sql.query(pgClient, {
        name,
        text,
        values
      })
    }

    await sql.query(pgClient, 'COMMIT')
  } catch (e) {
    await sql.query(pgClient, 'ROLLBACK')
    throw e
  }
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

function getTableBody (spec) {
  const tableBody = spec.target.columns.map(c => `"${c.name}" ${c.type}`)
  if (spec.target.extraProps) {
    let extraPropsType

    switch (spec.target.extraProps) {
      case 'JSON':
        extraPropsType = 'JSON'
        break
      case 'JSONB':
        extraPropsType = 'JSONB'
        break
      default:
        extraPropsType = 'TEXT'
        break
    }
    tableBody.push(`_extra_props ${extraPropsType}`)
  }
  tableBody.push(
    `PRIMARY KEY (${spec.keys.primaryKey.map(k => `"${k.name}"`).join(',')})`
  )

  return tableBody.join(',')
}
