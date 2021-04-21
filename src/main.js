'use strict'

const { MongoClient } = require('mongodb')
const pg = require('pg')
const values = require('lodash/values')

const Specs = require('./utils/Specs')
const ChangeStreamUtil = require('./utils/ChangeStream')
const OperationFromChangeStreamHandler = require('./handlers/OperationFromChangeStream')
const importCollections = require('./actions/importCollections')
const replicateOplogDeletions = require('./actions/replicateOplogDeletions')

const { getUpsertForConcurrency } = require('./actions/upsertSingle')
const { getDeleteForConcurrency } = require('./actions/deleteSingle')

let del
let upsert

module.exports = async function main(options) {
  console.log(`Starting halley with options ${JSON.stringify(options)}`)

  upsert = getUpsertForConcurrency(options.concurrency)
  del = getDeleteForConcurrency(options.concurrency)

  // connect to mongo
  const mongoClient = await MongoClient.connect(options.mongo, {
    appname: 'halley',
    useNewUrlParser: true,
    useUnifiedTopology: true
  })

  // connect to pg
  const pgPool = new pg.Pool({
    connectionString: options.sql,
    application_name: 'halley',
    min: 1,
    max: Math.max(1, options.concurrency)
  })

  const localDb = mongoClient.db('local')

  console.log('Connections established successfully...')

  const rootDatabase = options.dbMode === 'single' ? options.dbName || mongoClient.db().databaseName : null

  const specs = await Specs.loadFromFile(options.collections, {
    rootDatabase
  })

  if (options.incrementalImport && options.deleteMode === 'normal') {
    // replicate deletions in oplog
    await replicateOplogDeletions(specs, pgPool, localDb, options.concurrency)
  } else {
    console.log(`Ignoring past deletions from oplog`)
  }

  // import
  await importCollections(mongoClient, pgPool, values(specs), options)

  // listen for changes
  const changeStream = new ChangeStreamUtil()
  const streams = changeStream.getChangeStreams(mongoClient, specs)
  const handler = new OperationFromChangeStreamHandler({
    options,
    specs,
    upsert,
    pgPool,
    del
  })

  console.log(`Listening change stream for ${Object.keys(specs)}...`)

  for (const streamItem of streams) {
    const stream = streamItem.stream

    stream.on('change', async function (event) {
      stream.pause()

      try {
        await handler.handle.call(handler, event, streamItem.name)
      } catch (innerErr) {
        const error = new Error(`Could not process event: ${JSON.stringify(event)}`)
        error.innerError = innerErr
        if (options.exitOnError) {
          throw error
        } else {
          console.log(error)
        }
      } finally {
        stream.resume()
      }
    })

    stream.on('error', function (error) {
      if (options.exitOnError) {
        throw error
      } else {
        console.log(error)
      }
    })

    stream.on('close', function () {
      throw new Error(`Stream closed!`)
    })

    stream.on('end', () => {
      console.log('Error: no more data to be consumed from the stream!')
    })
  }
}

process.on('unhandledRejection', (err) => {
  console.error(err)
  process.exit(1)
})

process.on('uncaughtException', (err) => {
  console.error(err)
  process.exit(1)
})
