'use strict'

/**
 * Creates an object with some keys excluded
 * Replacement for lodash.omit for performance. Does not mimic the entire lodash.omit api
 * @param {Object} originalObject: created object will be based on this object
 * @param {Array<String>} keys: an array of keys to omit from the new object
 * @returns {Object} new object with same properties as originalObject
 */
function omit(originalObject, keys) {
  // code based on babel's _objectWithoutProperties
  const newObject = {}
  for (const key in originalObject) {
    if (keys.indexOf(key) >= 0) {
      continue
    }
    if (!Object.prototype.hasOwnProperty.call(originalObject, key)) {
      continue
    }
    newObject[key] = originalObject[key]
  }
  return newObject
}

module.exports = omit
