'use strict'

const uniq = require('lodash/uniq')
const yaml = require('js-yaml')
const fs = require('fs')
const { promisify } = require('util')

const sql = require('../interfaces/sql')

const readFileAsync = promisify(fs.readFile)

async function loadFromFile(filename, { rootDatabase = null } = {}) {
  const config = yaml.safeLoad(await readFileAsync(filename, 'utf8'))

  const specByNs = {}

  if (rootDatabase) {
    reduceCollectionsSpec(specByNs, config, rootDatabase)
  } else {
    for (const database in config) {
      if (Object.prototype.hasOwnProperty.call(config, database)) {
        reduceCollectionsSpec(specByNs, config[database], database)
      }
    }
  }

  return specByNs
}

function reduceCollectionsSpec(memo, collectionsSpec, databaseName) {
  for (const collection in collectionsSpec) {
    if (Object.prototype.hasOwnProperty.call(collectionsSpec, collection)) {
      memo[`${databaseName}.${collection}`] = getCollectionSpec(collectionsSpec[collection], collection, databaseName)
    }
  }
}

function getCollectionSpec(tableSpec, collectionName, databaseName) {
  const columns = tableSpec[':columns'].map((columnSpec) => {
    const [name] = Object.keys(columnSpec)
    return {
      name,
      type: (columnSpec[':type'] || columnSpec[name]).toLowerCase(),
      source: columnSpec[':source'] || name,
      retainExtraProp: !!columnSpec[':retain_extra_prop']
    }
  })

  const meta = tableSpec[':meta'] || {}

  const findColumnByName = (name) => {
    const column = columns.find((c) => c.name === name)
    if (!column) {
      throw new Error(`Could not find column ${name}`)
    }
    return column
  }

  const findColumnBySource = (source) => {
    const column = columns.find((c) => c.source === source)
    if (!column) {
      throw new Error(`Could not find column with source ${source}`)
    }
    return column
  }

  const primaryKey = Array.isArray(meta[':composite_key'])
    ? meta[':composite_key'].map(findColumnByName)
    : [findColumnBySource('_id')]

  // add flag to pk columns
  for (const key of primaryKey) {
    key.isPrimaryKey = true
  }

  let deleteKey
  // TODO see issue #7 ->  https://github.com/rodati/halley/issues/7
  if (primaryKey.find((key) => key.name === 'id' && key.isPrimaryKey)) {
    deleteKey = 'id'
  }

  const extraProps = getExtraPropsSpec(meta[':extra_props'], columns)

  // when extra props is present, we always need to project the entire source doc
  // TODO: we could use a suppression projection for fields that are omitted in extra props and unused by any column spec
  const projection = extraProps
    ? null
    : columns.reduce((memo, column) => {
        memo[column.source] = 1
        return memo
      }, {})

  return {
    ns: `${databaseName}.${collectionName}`,
    source: new CollectionSpecSource({
      databaseName,
      collectionName,
      projection
    }),
    target: {
      table: meta[':table'] || collectionName,
      tableInit: meta[':table_init'] ? sql.scriptToQueries(meta[':table_init']) : null,
      columns,
      extraProps
    },
    keys: {
      primaryKey,
      partitioned: meta[':partitioned'] ? true : false,
      incrementalReplicationKey: meta[':incremental_replication_key']
        ? findColumnByName(meta[':incremental_replication_key'])
        : null,
      incrementalReplicationDirection: meta[':incremental_replication_direction'] === 'high_to_low' ? -1 : 1,
      incrementalReplicationLimit: meta[':incremental_replication_limit'],
      deleteKey
    }
  }
}

function getExtraPropsSpec(extraPropsSpec, columnsSpec) {
  if (!extraPropsSpec) {
    return null
  }

  let type = (extraPropsSpec[':type'] || extraPropsSpec).toLowerCase()
  if (type !== 'json' && type !== 'jsonb' && type !== 'text') {
    console.warn(`Unsupported extra props type "${type}" -- using default type "text"`)
    type = 'text'
  }

  const fieldsToOmit = [
    // omit field if it's not to be retained and it's not a deep path, since we can't omit deep fields
    ...columnsSpec.filter((c) => !c.retainExtraProp && !c.source.includes('.')).map((c) => c.source),

    // omit other specified fields
    ...(extraPropsSpec[':omit'] || [])
  ]

  return {
    type,
    omit: fieldsToOmit.length ? uniq(fieldsToOmit) : null
  }
}

class CollectionSpecSource {
  constructor(data) {
    Object.assign(this, data)
  }

  getCollection(mongoClient) {
    return mongoClient.db(this.databaseName).collection(this.collectionName)
  }
}

module.exports = {
  loadFromFile
}
