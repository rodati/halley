'use strict'

const schema = require('../utils/Schema')
const sql = require('../interfaces/sql')
const lockUtil = require('../utils/lock')

async function upsert (spec, pgClient, doc) {
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
      whereClauses.push(`${spec.target.table}.${eq}`)
    } else {
      updates.push(eq)
    }

    values.push(value)

    i++
  }

  const keys = []
  for (const key of spec.keys.primaryKey) {
    keys.push(key.name)
  }

  const isTablePartitioned = spec.keys.partitioned || false

  // Note: The ON CONFLICT command doesn't work in partitioned tables in Postgres < 11, 
  // so for that we use a workaround to make the upsert
  if(isTablePartitioned){

    try {
      await sql.query(pgClient, {
        name: `insert-u-${spec.ns}`,
        text: `INSERT INTO "${table}" (${columnNames.join(',')}) 
               VALUES (${placeholders.join(',')})`,
  
        values
      })
    } catch (error) {
      // If the insert fails for unique_violation, try the update
      // https://www.postgresql.org/docs/9.2/errcodes-appendix.html
      if (error.name === 'PgError' && error.innerError && error.innerError.code === '23505') {

        try {
          const resultUpdate = await sql.query(pgClient, {
            name: `update-u-${spec.ns}`,
            text: `UPDATE "${table}"
                   SET ${updates.join(',')} 
                   WHERE ${whereClauses.join(' AND ')}`,
      
            values
          })

          if (resultUpdate.rowCount > 1) {
            console.warn(`Huh? Updated ${resultUpdate.rowCount} > 1 rows: upsert(${table}, ${JSON.stringify(doc)}`)
          }
        } catch (error) {
          await sql.query(pgClient, 'ROLLBACK')
          throw error
        }

      } else {
        throw error
      }
      
    }

  } else {
    try {
      // use prepared statements so that pg can cache them
      const result = await sql.query(pgClient, {
        name: `upsert-u-${spec.ns}`,
        text: `INSERT INTO "${table}" (${columnNames.join(',')}) 
               VALUES (${placeholders.join(',')})
               ON CONFLICT (${keys.join(',')}) DO UPDATE
               SET ${updates.join(',')} 
               WHERE ${whereClauses.join(' AND ')}`,
  
        values
      })
  
      if (result.rowCount > 1) {
        console.warn(`Huh? Updated ${result.rowCount} > 1 rows: upsert(${table}, ${JSON.stringify(doc)}`)
      }
    } catch (error) {
      await sql.query(pgClient, 'ROLLBACK')
      throw error
    }
  }
  
}

async function lockeableUpsert (spec, pgPool, doc) {
  const pgClient = await pgPool.connect()
  try {
    await upsert(spec, pgClient, doc)
  } finally {
    pgClient.release()
  }
}

function getUpsertForConcurrency (concurrency) {
  return concurrency > 1
    // lock upsert by ns to avoid concurrent upserts on the same collection
    ? lockUtil.lockify(lockeableUpsert, lockUtil.getPromisifiedLock(), spec => spec.ns)
    : upsert
}

module.exports = {
  getUpsertForConcurrency,
  upsert
}
