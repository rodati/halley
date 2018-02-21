'use strict'

const winston = require('winston')
const util = require('util')
const Raven = require('raven')

const environment = process.env.ENV || 'staging'

module.exports = function _sentryTransport (winstonLevel, sentryDsn) {
  Raven.config(sentryDsn).install()

  winstonLevel = winstonLevel || 'error'

  function sentryTransport (level, msg, error) {
    level = level || winstonLevel
    const newContext = String(Math.floor((Math.random() * 100000)))
    Raven.context(function inContext () {
      Raven.setContext({ id: newContext })

      const outerError = error
      while (error.innerError) {
        error = error.innerError
        const extraData = error.extraData ? error.extraData : undefined
        Raven.captureBreadcrumb({
          message: error.message ? error.message : 'error',
          category: 'outerError',
          data: {
            stack: error.stack ? error.stack : error,
            extraData
          }
        })
      }

      const extra = {
        stack: outerError.stack ? outerError.stack : outerError,
        extraData: outerError.extraData ? error.extraData : undefined
      }

      level = level === 'warn' || level === 'fail' ? 'warning' : level
      Raven.captureException(outerError, { extra, level, environment }, function (error, eventId) {
        if (error) {
          return winston.warn('[Sentry] error while logging error to sentry')
        }
        winston.info(`[Sentry] The error was successfully logged to sentry as event with id: ${eventId}`)
        process.exit(1)
      })
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
