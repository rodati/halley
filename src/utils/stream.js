'use strict'

/**
 * @param {NodeJS.ReadableStream} stream
 */
function endOfStream(stream) {
  return new Promise(function(resolve, reject) {
    function endHandler() {
      cleanup()
      resolve()
    }

    function errorHandler(error) {
      cleanup()
      reject(error)
    }

    function cleanup() {
      stream.removeListener('end', endHandler).removeListener('error', errorHandler)
    }

    stream.on('end', endHandler).on('error', errorHandler)
  })
}

module.exports = {
  endOfStream
}
