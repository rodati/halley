'use strict'

const MongoDB = require('mongodb')
const pg = require('pg')
const _ = require('lodash')
const winston = require('winston')

const Specs = require('./utils/Specs')
const importCollections = require('./actions/importCollections')
const OplogUtil = require('./utils/Oplog')
const replicateOplogDeletions = require('./actions/replicateOplogDeletions')

const { getUpsertForConcurrency } = require('./actions/upsertSingle')
const { getDeleteForConcurrency } = require('./actions/deleteSingle')

let del
let upsert

module.exports = async function main (options) {
  winston.info(`Connecting to databases...`)
  winston.level = options.log

  const specs = await Specs.loadFromFile(options.collections)

  upsert = getUpsertForConcurrency(options.concurrency)
  del = getDeleteForConcurrency(options.concurrency)

  // connect to mongo
  const mongoConnection = await MongoDB.MongoClient.connect(options.mongo, {
    autoReconnect: true,
    appname: 'halley'
  })

  // connect to pg
  const pgPool = new pg.Pool({
    connectionString: options.sql,
    application_name: 'halley',
    min: 1,
    max: Math.max(1, options.concurrency)
  })

  const oplogUtil = new OplogUtil(mongoConnection)

  // find the last timestamp. If there isn't one found, get one from the local clock
  const tailFrom = await oplogUtil.getLastTimestamp() || getLocalTimestamp()
  // replicate deletions in oplog
  winston.info(`Connections established successfully...`)
  if (options.incrementalImport) {
    await replicateOplogDeletions(specs, pgPool, mongoConnection, options.concurrency)
  }

  // import
  await importCollections(mongoConnection, pgPool, _.values(specs), options.incrementalImport)

  const oplog = oplogUtil.observableTail({
    fromTimestamp: tailFrom
  })

  async function syncObject (spec, selector) {
    const obj = await spec.source
      .getCollection(mongoConnection)
      .findOne(selector, { fields: spec.source.fields })

    if (obj) {
      return upsert(spec, pgPool, obj)
    } else {
      // TODO
      winston.warn(`TODO: sync delete on ${spec.ns}`, obj)
    }
  }

  async function handleOp (op) {
    winston.debug('processing op', op)

    if (op.op === 'n') {
      winston.debug('Skipping no-op', op)
      return
    }

    if (!op.op || !op.ns) {
      winston.warn('Weird op', op)
      return
    }

    /**
     * First, check if this was an operation performed via applyOps. If so, call handle_op with
     * for each op that was applied.
     * The oplog format of applyOps commands can be viewed here:
     * https://groups.google.com/forum/#!topic/mongodb-user/dTf5VEJJWvY
     */
    if (op.op === 'c' && op.o && op.o.applyOps) {
      for (const op of op.o.applyOps) {
        await handleOp(op)
      }
    }

    const ns = op.ns

    const spec = specs[ns]
    if (!spec) {
      winston.debug('Skipping op for unknown ns', ns)
      return
    }

    switch (op.op) {
      case 'i':
        if (spec.source.collectionName === 'system.indexes') {
          winston.info('Skipping index update', op)
        } else {
          await upsert(spec, pgPool, op.o)
        }
        break

      case 'u':
        const { o2: selector, o: update } = op

        if (Object.keys(update).some(k => k.startsWith('$'))) {
          winston.debug(`re sync ${ns}: ${selector._id}`, update)
          syncObject(spec, selector)
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

          winston.debug(`upsert ${ns}`, upsertObj)

          await upsert(spec, pgPool, upsertObj)
        }
        break

      case 'd':
        switch (options.deleteMode) {
          case 'ignore':
            winston.debug(`Ignoring delete op on ${ns} as instructed.`)
            break

          case 'citus-multi-shard':
            // TODO
            // const example = `SELECT master_modify_multiple_shards('DELETE FROM table WHERE id = ' + op.o);`
            winston.warn(`TODO: "citus-multi-shard" delete mode on ${ns}`, op.o.toString())
            break

          case 'normal':
            await del(spec, pgPool, op.o)
            break

          default:
            // TODO
            winston.warn(`TODO: "${options.deleteMode}" delete mode on ${ns}`, op.o.toString())
            break
        }
        break

      default:
        winston.info('Skipping unknown op', op)
    }
  }

  winston.info('Tailing oplog...')
  oplog.subscribe(
    handleOp,
    function onError (err) {
      throw err
    }
  )
}

function getLocalTimestamp () {
  return new MongoDB.Timestamp(0, Math.floor(new Date().getTime() / 1000))
}

process.on('unhandledRejection', err => {
  winston.error(err)
  while (err.innerError) {
    err = err.innerError
    winston.error('inner', err)
  }
  process.exit(1)
})
