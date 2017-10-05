'use strict'

const Rx = require('rxjs')

/**
 * @param {NodeJS.ReadableStream} stream 
 */
module.exports = function fromReadableStream (stream, pauser = undefined) {
  // pause until someone subscribes to avoid losing data
  stream.pause()

  const pauserObservable = pauser
    ? pauser.distinctUntilChanged()
    : null

  return new Rx.Observable(observer => {
    function dataHandler (data) {
      observer.next(data)
    }

    function errorHandler (err) {
      observer.error(err)
    }

    function endHandler () {
      observer.complete()
    }

    function pauserHandler (pause) {
      if (pause) {
        stream.pause()
      } else {
        stream.resume()
      }
    }

    stream.addListener('data', dataHandler)
    stream.addListener('error', errorHandler)
    stream.addListener('end', endHandler)

    const pauserSubscription = pauserObservable
      ? pauserObservable.subscribe(pauserHandler)
      : null

    stream.resume()

    return () => {
      stream.removeListener('data', dataHandler)
      stream.removeListener('error', errorHandler)
      stream.removeListener('end', endHandler)

      if (pauserSubscription) {
        pauserSubscription.unsubscribe()
      }
    }
  }).share()
}
