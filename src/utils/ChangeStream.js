'use strict'

const uniq = require('lodash/uniq')

module.exports = class ChangeStreamUtil {
  getChangeStreamForCollection(mongoClient, collection, pipe = []) {
    return mongoClient.db().collection(collection).watch(pipe, { fullDocument: 'updateLookup' })
  }

  getChangeStreamForCollections(mongoClient, db, collections) {
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

  getChangeStreams(mongoClient, specs) {
    const streams = []
    const specsNs = Object.keys(specs)

    // If there is only one collection, create a stream for only that one, else, create an stream for all the collections
    if (specsNs.length === 1) {
      const specsNsParts = specsNs[0].split('.')
      const collection = specsNsParts[1]
      const specsStreams = specs[specsNs[0]].streams

      // If there are pipes, create an stream for each pipe. Else, create a stream for only the collection
      if (specsStreams && specsStreams.length) {
        for (const specsStream of specsStreams) {
          console.log(`Creating stream '${specsStream.name}' for collection ${collection} and pipe ${specsStream.pipe}`)

          const pipeParsed = JSON.parse(specsStream.pipe)
          const stream = this.getChangeStreamForCollection(mongoClient, collection, pipeParsed)

          streams.push({
            name: `${collection}.${specsStream.name}`,
            stream
          })
        }
      } else {
        console.log(`Creating stream for collection ${collection}`)

        const stream = this.getChangeStreamForCollection(mongoClient, collection)

        streams.push({
          name: collection,
          stream
        })
      }
    } else {
      const specsNsParts = specsNs.map((name) => {
        return name.split('.')
      })
      const db = specsNsParts[0]
      const collections = uniq(specsNsParts.map((name) => name && name[1]))

      console.log(`Creating stream for collections ${collections}`)

      const stream = this.getChangeStreamForCollections(mongoClient, db, collections)

      streams.push({
        name: `${collections.join(',')}`,
        stream
      })
    }

    return streams
  }
}
