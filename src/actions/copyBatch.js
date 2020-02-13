'use strict'

const copyFrom = require('pg-copy-streams').from

const Schema = require('../utils/Schema')
const { endOfStream } = require('../utils/stream')

async function copyBatch(spec, docs, pgClient, tableName) {
  const columns = Schema.getColumnNames(spec)
  const table = tableName || spec.target.table

  const targetStream = pgClient.query(
    copyFrom(`COPY "${table}" (${columns.join(',')}) FROM STDIN`)
  )

  const data = docs.map((doc) => Schema.toTextFormat(spec, doc))

  const streamEnd = endOfStream(targetStream)

  targetStream.write(data.join(''))
  targetStream.end()

  return streamEnd
}

module.exports = copyBatch
