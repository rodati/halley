'use strict'

const { Lock } = require('lock')

function getPromisifiedLock () {
  // eslint-disable-next-line new-cap
  const lock = Lock()

  return key => new Promise(resolve => {
    lock(key, release => {
      resolve(promisifyRelease(release))
    })
  })
}

function promisifyRelease (release) {
  return () => new Promise((resolve, reject) => {
    release(err => {
      if (err) {
        reject(err)
      } else {
        resolve()
      }
    })()
  })
}

function lockify (fn, lockInstance, lockEntityResolver = defaultLockEntityResolver) {
  return async function lockifyWrapper (...args) {
    const entity = lockEntityResolver(...args)

    if (!entity) {
      throw new Error('A locking entity is required in order to lock')
    }

    const release = await lockInstance(entity)
    let result

    try {
      result = await fn(...args)
    } finally {
      await release()
    }

    return result
  }
}

// default entity resolver uses the first parameter as the locking entity
function defaultLockEntityResolver (doc) {
  return doc
}

module.exports = {
  getPromisifiedLock,
  lockify
}
