'use strict'

const { streamToRx } = require('rxjs-stream')
const { Timestamp } = require('mongodb')

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

  observableTail ({ fromTimestamp }) {
    // Create a cursor for tailing and set it to await data
    const cursor = this._oplog.find({
      ts: {
        $gt: fromTimestamp
      }
    }, {
      tailable: true,
      awaitData: true
    })

    return streamToRx(cursor.stream())
  }

  async getNewOps (lastSyncTimestamp) {
    const newDocsCursor = await this._oplog
      .find({ ts: { $gt: makeLocalTimestamp(lastSyncTimestamp) } })

    const more = await newDocsCursor.hasNext()

    if (!more) {
      return null
    }
    return newDocsCursor
  }
}

function makeLocalTimestamp (date) {
  return new Timestamp(0, Math.floor(date.getTime() / 1000))
}
