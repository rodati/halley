'use strict'

const debug = require('debug')('halley')
const { MongoClient, Timestamp } = require('mongodb')
const pg = require('pg')
const values = require('lodash/values')

const Specs = require('./utils/Specs')
const OplogUtil = require('./utils/Oplog')
const importCollections = require('./actions/importCollections')
const replicateOplogDeletions = require('./actions/replicateOplogDeletions')

const { getUpsertForConcurrency } = require('./actions/upsertSingle')
const { getDeleteForConcurrency } = require('./actions/deleteSingle')

let del
let upsert

module.exports = async function main (options) {
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
  const oplogUtil = new OplogUtil(localDb)

  // find the last timestamp. If there isn't one found, get one from the local clock
  const tailFrom = await oplogUtil.getLastTimestamp() || getLocalTimestamp()

  console.log('Connections established successfully...')

  const rootDatabase = options.dbMode === 'single'
    ? options.dbName || mongoClient.db().databaseName
    : null

  const specs = await Specs.loadFromFile(options.collections, {
    rootDatabase
  })

  if (options.incrementalImport) {
    // replicate deletions in oplog
    await replicateOplogDeletions(specs, pgPool, localDb, options.concurrency)
  }

  // import
  await importCollections(mongoClient, pgPool, values(specs), options)

  const oplog = oplogUtil.observableTail({
    fromTimestamp: tailFrom
  })

  async function syncObject (spec, selector) {
    const obj = await spec.source
      .getCollection(mongoClient)
      .findOne(selector, { projection: spec.source.projection })

    if (obj) {
      await upsert(spec, pgPool, obj)
    } else {
      console.warn(`TODO: sync delete on ${spec.ns}`, JSON.stringify(selector))
    }
  }

  async function handleOp (op) {
    debug('processing op', op)

    if (op.op === 'n') {
      debug('Skipping no-op', op)
      return
    }

    if (!op.op || !op.ns) {
      console.warn('Weird op', op)
      return
    }

    /**
     * First, check if this was an operation performed via applyOps. If so, call handle_op with
     * for each op that was applied.
     * The oplog format of applyOps commands can be viewed here:
     * https://groups.google.com/forum/#!topic/mongodb-user/dTf5VEJJWvY
     */
    if (op.op === 'c' && op.o && op.o.applyOps) {
      for (const innerOp of op.o.applyOps) {
        await handleOp(innerOp)
      }
    }

    const ns = op.ns

    const spec = specs[ns]
    if (!spec) {
      debug('Skipping op for unknown ns', ns)
      return
    }

    switch (op.op) {
      case 'i':
        if (spec.source.collectionName === 'system.indexes') {
          debug('Skipping index update', op)
        } else {
          await upsert(spec, pgPool, op.o)
        }
        break

      case 'u': {
        const { o2: selector, o: update } = op

        if (Object.keys(update).some(k => k.startsWith('$'))) {
          debug(`re sync ${ns}: ${selector._id}`, update)
          await syncObject(spec, selector)
        } else {
          /**
           * The update operation replaces the existing object, but
           * preserves its _id field, so grab the _id off of the
           * 'query' field -- it's not guaranteed to be present on the
           * update.
           */
          const upsertObj = {}
          for (const key of spec.keys.primaryKey) {
            upsertObj[key.source] = selector[key.source]
          }

          Object.assign(upsertObj, update)

          debug(`upsert ${ns}`, upsertObj)
          await upsert(spec, pgPool, upsertObj)
        }
        break
      }

      case 'd':
        switch (options.deleteMode) {
          case 'ignore':
            debug(`Ignoring delete op on ${ns} as instructed.`)
            break

          case 'normal':
            await del(spec, pgPool, op.o)
            break

          default:
            console.warn(`Unknown delete mode "${options.deleteMode}" on ${ns}`, op.o.toString())
            break
        }
        break

      default:
        console.warn('Skipping unknown op', op)
    }
  }

  function handleOpWrapper (op) {
    return handleOp(op).catch(innerErr => {
      const error = new Error(`Could not process op: ${JSON.stringify(op)}`)
      error.innerError = innerErr
      if (options.exitOnError) {
        throw error
      } else {
        console.log(error)
      }
    })
  }

  console.log('Tailing oplog...')
  oplog.subscribe(
    handleOpWrapper,
    function onError (err) {
      throw err
    }
  )
}

function getLocalTimestamp () {
  return new Timestamp(0, Math.floor(new Date().getTime() / 1000))
}

process.on('unhandledRejection', err => {
  console.error(err)
  process.exit(1)
})

process.on('uncaughtException', err => {
  console.error(err)
  process.exit(1)
})
