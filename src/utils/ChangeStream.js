'use strict'

const uniq = require('lodash/uniq')

module.exports = class ChangeStreamUtil {
  getChangeStream(mongoClient, specsNs) {
    const ns = specsNs.map((name) => {
      return name.split('.')
    })
    const db = uniq(ns.map((name) => name && name[0]))
    const collections = uniq(ns.map((name) => name && name[1]))

    // If there is configured only one collection, listen the change in that collection, else, listen the whole DB, filtering by collection
    if (ns.length === 1) {
      return mongoClient.db().collection(collections[0]).watch({ fullDocument: 'updateLookup' })
    } else {
      return mongoClient.db().watch(
        [
          {
            $match: {
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
}
