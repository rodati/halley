'use strict'

const omit = require('lodash/omit')
const get = require('lodash/get')
const { ObjectId } = require('mongodb')

function getColumnNames (spec) {
  const { columns, extraProps } = spec.target

  const names = columns.map(c => `"${c.name}"`)
  if (extraProps) {
    names.push('_extra_props')
  }

  return names
}

function getPlaceholders (spec) {
  const { columns, extraProps } = spec.target

  const placeholders = columns.map((c, i) => `$${i + 1}`)
  if (extraProps) {
    placeholders.push(`$${columns.length + 1}`)
  }

  return placeholders
}

function getTableBody (spec) {
  const tableBody = spec.target.columns.map(c => `"${c.name}" ${c.type}`)
  if (spec.target.extraProps) {
    let extraPropsType

    switch (spec.target.extraProps) {
      case 'JSON':
        extraPropsType = 'JSON'
        break
      case 'JSONB':
        extraPropsType = 'JSONB'
        break
      default:
        extraPropsType = 'TEXT'
        break
    }
    tableBody.push(`_extra_props ${extraPropsType}`)
  }
  tableBody.push(
    `PRIMARY KEY (${spec.keys.primaryKey.map(k => `"${k.name}"`).join(',')})`
  )

  return tableBody.join(',')
}

function * transformValues (spec, doc) {
  const { columns, extraProps } = spec.target

  const omittedFromExtraProps = []

  for (const column of columns) {
    const source = get(doc, column.source)

    let value

    switch (typeof source) {
      case 'object':
        if (source === null) {
          value = null
        } else if (source instanceof ObjectId) {
          value = source.toHexString()
        } else {
          value = JSON.stringify(source, transformObjectValue)
        }
        break
      case 'undefined':
        value = null
        break
      case 'number':
        if (column.type === 'smallint' || column.type === 'integer' || column.type === 'bigint') {
          value = Math.trunc(source)
        } else {
          value = source
        }
        break
      case 'string':
        value = sanitizeString(source)
        break
      default:
        value = source
    }

    yield value

    if (!column.retainExtraProp) {
      omittedFromExtraProps.push(column.source)
    }
  }

  if (extraProps) {
    yield JSON.stringify(omit(doc, omittedFromExtraProps), transformObjectValue)
  }
}

function transformObjectValue (key, jsonValue) {
  if (this[key] instanceof ObjectId) {
    return {
      // jsonValue should be a string equivalent to this[key].toHexString()
      $oid: jsonValue
    }
  } else if (typeof jsonValue === 'string') {
    return sanitizeString(jsonValue)
  }
  return jsonValue
}

/**
 * Sanitize string for PostgreSQL by removing zero code points (\u0000)
 * See: https://stackoverflow.com/a/31672314
 * @param {string} str String to sanitize
 */
function sanitizeString (str) {
  // eslint-disable-next-line no-control-regex
  return str.replace(/\u0000/g, ' ')
}

function escapeText (str) {
  return str.replace(/[\\\n\r\t]/g, '\\$&')
}

function toTextFormat (spec, doc) {
  const parts = []

  for (const value of transformValues(spec, doc)) {
    if (typeof value === 'string') {
      parts.push(escapeText(value))
    } else if (value === null) {
      parts.push('\\N')
    } else {
      parts.push(value)
    }
  }

  return `${parts.join('\t')}\n`
}

module.exports = {
  getColumnNames,
  getPlaceholders,
  getTableBody,
  transformValues,
  toTextFormat,
  escapeText
}
