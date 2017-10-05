'use strict'

const RxNode = require('./rx-node')

module.exports = class OplogUtil {
  constructor (localDb) {
    this._oplog = localDb.collection('oplog.rs')
  }

  async getLastTimestamp () {
    const lastLogArray = await this._oplog
      .find({}, { ts: 1 })
      .sort({ $natural: -1 })
      .limit(1)
      .toArray()

    if (!lastLogArray.length) {
      return null
    }

    return lastLogArray[0].ts
  }

  observableTail ({ ns, fromTimestamp }) {
    // Create a cursor for tailing and set it to await data
    const cursor = this._oplog.find({
      // ns,
      ts: {
        $gt: fromTimestamp
      }
    }, {
      tailable: true,
      awaitData: true
    })

    return RxNode.fromReadableStream(cursor.stream())
  }
}
