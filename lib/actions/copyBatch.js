'use strict'

const copyFrom = require('pg-copy-streams').from
const Readable = require('stream').Readable

const sql = require('../interfaces/sql')
const Schema = require('../utils/Schema')

/**
 * @param {PG.Connection} pgClient ** a postgres connection instance
 * @param {Boolean} isTemp ** a boolean indicating wether is a temporary table or not
 */

async function copyBatch (spec, docs, pgClient, isTemp) {
  const columns = Schema.getColumnNames(spec)
  let tableName = spec.target.table

  if (isTemp) {
    tableName += '_copy_temp'
    const body = Schema.getTableBody(spec)
    await sql.query(pgClient, `CREATE TEMPORARY TABLE "${tableName}" (${body})`)
  }

  const targetStream = pgClient.query(copyFrom(
    `COPY "${tableName}" (${columns.join(',')}) FROM STDIN`
  ))

  const data = docs.map(doc => Schema.toTextFormat(spec, doc))

  const sourceStream = new Readable()
  sourceStream._read = function noop () {}
  sourceStream.pipe(targetStream)

  const streamEnd = endOfStream(targetStream)

  sourceStream.push(data.join(''))
  sourceStream.destroy()

  return streamEnd
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

module.exports = copyBatch
