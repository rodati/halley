'use strict'

const winston = require('winston')
const util = require('util')
const Raven = require('raven')

Raven.config('https://***REMOVED***@sentry.io/261123').install()

module.exports = function _sentryTransport (winstonLevel) {
  winstonLevel = winstonLevel || 'error'

  function sentryTransport (level, error) {
    level = level || winstonLevel

    const errors = [error]

    while (error.innerError) {
      error = error.innerError
      const message = error.message ? error.message : error
      errors.push(message)
    }
    const finalError = errors.join('\ninnerError:\n')

    Raven.captureException(finalError, {
      extra: error,
      level: level === 'warn' || level === 'fail' ? 'warning' : level
    })
  }

  return makeTransport(
    `sentry-${winstonLevel}`,
    winstonLevel,
    sentryTransport
  )
}

function makeTransport (name, level, log) {
  const transport = function () {
    this.name = name
    this.level = level
  }

  util.inherits(transport, winston.Transport)

  transport.prototype.log = log

  return transport
}
