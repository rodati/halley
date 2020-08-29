'use strict'

const debug = require('debug')('halley')
const logOperation = require('../utils/logOperation')

module.exports = class OperationFromChangeStreamHandler {
  constructor({ options, specs, upsert, pgPool, del }) {
    this.options = options
    this.specs = specs
    this.upsert = upsert
    this.pgPool = pgPool
    this.del = del
  }

  async handle(changeEvent) {
    const { ns, operationType } = changeEvent

    const spec = this.specs[`${ns.db}.${ns.coll}`]

    if (!spec) {
      debug('Skipping event for unknown ns', ns)
      return
    }

    const eventEnteredAt = new Date(new Date().getTime())

    switch (operationType) {
      case 'insert':
      case 'update':
      case 'replace': {
        await this.upsert(spec, this.pgPool, changeEvent.fullDocument)
        break
      }
      case 'delete': {
        switch (this.options.deleteMode) {
          case 'ignore':
            debug(`Ignoring delete op on ${ns} as instructed.`)
            break

          case 'normal':
            await this.del(spec, this.pgPool, changeEvent.documentKey)
            break

          default:
            console.warn(`Unknown delete mode "${this.options.deleteMode}" on ${ns}`)
            break
        }
        break
      }
      default:
        console.warn('Skipping event type', operationType)
    }

    logOperation(changeEvent, eventEnteredAt)
  }
}
