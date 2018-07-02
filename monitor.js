'use strict'

const pm2 = require('pm2')
const Raven = require('raven')

const RAVEN_DSN = process.env.RAVEN_DSN
const RAVEN_ENV = process.env.RAVEN_ENV

if (RAVEN_DSN) {
  Raven.config(RAVEN_DSN, {
    environment: RAVEN_ENV || 'production'
  }).install()

  /**
 * Pm2 events monitor
 */
  Raven.context(function () {
    pm2.launchBus(function (err, bus) {
      bus.on('process:exception', function (message) {
        Raven.captureMessage(message.data.message, {
          extra: message
        })
      })

      bus.on('process:event', (message) => {
        // Events: online / exit / stop
        console.log(
          `New PM2 event: ${message.event}. Executed manually: ${message.manually.toString()}`
        )
      })

      bus.on('close', function () {
        console.log('PM2 bus closed')
      })
    })
  })
} else {
  console.log('No RAVEN_DSN variable passed')
}
