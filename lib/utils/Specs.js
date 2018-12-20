'use strict'

const yaml = require('js-yaml')
const fs = require('fs')
const { promisify } = require('util')

const sql = require('../interfaces/sql')

const readFileAsync = promisify(fs.readFile)

async function loadFromFile (filename) {
  const config = yaml.safeLoad(await readFileAsync(filename, 'utf8'))

  const specByNs = {}
  for (const database in config) {
    if (config.hasOwnProperty(database)) {
      const collections = config[database]

      for (const collection in collections) {
        if (collections.hasOwnProperty(collection)) {
          specByNs[`${database}.${collection}`] = getCollectionSpec(collections[collection], collection, database)
        }
      }
    }
  }

  return specByNs
}

function getCollectionSpec (tableSpec, collectionName, databaseName) {
  const columns = tableSpec[':columns'].map(columnSpec => {
    const [ name ] = Object.keys(columnSpec)
    return {
      name,
      type: (columnSpec[':type'] || columnSpec[name]).toLowerCase(),
      source: columnSpec[':source'] || name,
      retainExtraProp: !!columnSpec[':retain_extra_prop']
    }
  })

  const meta = tableSpec[':meta'] || {}
  const extraProps = meta[':extra_props']

  const fields = extraProps
    ? null
    : columns.reduce((memo, column) => {
      memo[column.source] = 1
      return memo
    }, {})

  const findColumnByName = name => {
    const column = columns.find(c => c.name === name)
    if (!column) {
      throw new Error(`Could not find column ${name}`)
    }
    return column
  }

  const findColumnBySource = source => {
    const column = columns.find(c => c.source === source)
    if (!column) {
      throw new Error(`Could not find column with source ${source}`)
    }
    return column
  }

  const primaryKey = Array.isArray(meta[':composite_key'])
    ? meta[':composite_key'].map(findColumnByName)
    : [ findColumnBySource('_id') ]

  // add flag to pk columns
  for (const key of primaryKey) {
    key.isPrimaryKey = true
  }

  let deleteKey
  // TODO see issue #7 ->  https://github.com/rodati/halley/issues/7
  if (primaryKey.find((key) => key.name === 'id' && key.isPrimaryKey)) {
    deleteKey = 'id'
  }

  return {
    ns: `${databaseName}.${collectionName}`,
    source: new CollectionSpecSource({
      databaseName,
      collectionName,
      fields
    }),
    target: {
      table: meta[':table'] || collectionName,
      tableInit: meta[':table_init']
        ? sql.scriptToQueries(meta[':table_init'])
        : null,
      columns,
      extraProps
    },
    keys: {
      primaryKey,
      incrementalReplicationKey: meta[':incremental_replication_key']
        ? findColumnByName(meta[':incremental_replication_key'])
        : null,
      deleteKey
    }
  }
}

class CollectionSpecSource {
  constructor (data) {
    Object.assign(this, data)
  }

  getCollection (mongoClient) {
    return mongoClient.db(this.databaseName).collection(this.collectionName)
  }
}

module.exports = {
  loadFromFile
}
