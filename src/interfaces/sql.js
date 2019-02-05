'use strict'

function query (pgClient, query) {
  return pgClient.query(query).catch(innerErr => {
    const error = new Error(typeof query === 'string'
      ? `Error executing query "${query}"`
      : `Error executing query "${query.text}" with values (${query.values})`)
    error.name = 'PgError'
    error.innerError = innerErr
    throw error
  })
}

/**
 * @param {string} script
 */
function scriptToQueries (script) {
  // remove newlines
  return script.replace(/(\r\n|\n|\r)/gm, ' ')

    // excess white space
    .replace(/\s+/g, ' ')

    // split into statements and remove empty ones
    .split(';')
    .map(s => s.trim())
    .filter(s => s.length)
}

module.exports = {
  query,
  scriptToQueries
}
