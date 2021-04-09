'use strict'

const uniq = require('lodash/uniq')

module.exports = class ChangeStreamUtil {
  getChangeStream(mongoClient, specsNs, operationTypes) {
    const ns = specsNs.map((name) => {
      return name.split('.')
    })
    const db = uniq(ns.map((name) => name && name[0]))
    const collections = uniq(ns.map((name) => name && name[1]))

    console.log(`Listen change stream for operation types ${operationTypes}...`)

    // If there is configured only one collection, listen the change in that collection, else, listen the whole DB, filtering by collection
    if (ns.length === 1) {
      return mongoClient
        .db()
        .collection(collections[0])
        .watch(
          [
            {
              $match: {
                operationType: {
                  $in: operationTypes
                }
              }
            }
          ],
          { fullDocument: 'updateLookup' }
        )
    } else {
      return mongoClient.db().watch(
        [
          {
            $match: {
              operationType: {
                $in: operationTypes
              },
              'ns.db': {
                $in: db
              },
              'ns.coll': {
                $in: collections
              }
            }
          }
        ],
        { fullDocument: 'updateLookup' }
      )
    }
  }

  getChangeStreams(mongoClient, specsNs) {
    // Create two cursors by operation type
    const changeStreamUpdates = this.getChangeStream(mongoClient, specsNs, ['update'])
    const changeStreamOther = this.getChangeStream(mongoClient, specsNs, ['insert', 'delete', 'replace'])
    return [changeStreamUpdates, changeStreamOther]
  }
}
