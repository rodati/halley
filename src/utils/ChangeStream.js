'use strict'

const uniq = require('lodash/uniq')

module.exports = class ChangeStreamUtil {
  getChangeStream(mongoClient, specsNs) {
    const ns = specsNs.map((name) => {
      return name.split('.')
    })
    const db = uniq(ns.map((name) => name && name[0]))
    const collections = uniq(ns.map((name) => name && name[1]))

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
