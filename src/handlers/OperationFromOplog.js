'use strict'

const debug = require('debug')('halley')
const logOperation = require('../utils/logOperation')

module.exports = class OperationFromOplogHandler {
  constructor({ options, specs, upsert, pgPool, del, mongoClient }) {
    this.options = options
    this.specs = specs
    this.upsert = upsert
    this.pgPool = pgPool
    this.del = del
    this.mongoClient = mongoClient
  }

  async syncObject(spec, selector) {
    const obj = await spec.source
      .getCollection(this.mongoClient)
      .findOne(selector, { projection: spec.source.projection })

    if (obj) {
      await this.upsert(spec, this.pgPool, obj)
    } else {
      console.warn(`TODO: sync delete on ${spec.ns}`, JSON.stringify(selector))
    }
  }

  async handle(op) {
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
        await this.handle(innerOp)
      }
    }

    const ns = op.ns

    const spec = this.specs[ns]
    if (!spec) {
      debug('Skipping op for unknown ns', ns)
      return
    }

    const opEnteredAt = new Date(new Date().getTime())

    switch (op.op) {
      case 'i':
        if (spec.source.collectionName === 'system.indexes') {
          debug('Skipping index update', op)
        } else {
          await this.upsert(spec, this.pgPool, op.o)
        }
        break

      case 'u': {
        const { o2: selector, o: update } = op

        if (Object.keys(update).some((k) => k.startsWith('$'))) {
          debug(`re sync ${ns}: ${selector._id}`, update)
          await this.syncObject(spec, selector)
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
          await this.upsert(spec, this.pgPool, upsertObj)
        }
        break
      }

      case 'd':
        switch (this.options.deleteMode) {
          case 'ignore':
            debug(`Ignoring delete op on ${ns} as instructed.`)
            break

          case 'normal':
          case 'ignore-past':
            await this.del(spec, this.pgPool, op.o)
            break

          default:
            console.warn(`Unknown delete mode "${this.options.deleteMode}" on ${ns}`, op.o.toString())
            break
        }
        break

      default:
        console.warn('Skipping unknown op', op)
    }

    logOperation(op, opEnteredAt)
  }
}
