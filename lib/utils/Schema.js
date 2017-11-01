'use strict'

const _ = require('lodash')
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
    const source = doc[column.source]

    let value

    if (source instanceof ObjectId) {
      value = source.toHexString()
    } else if (typeof source === 'object' && source !== null) {
      value = JSON.stringify(source)
    } else if (source === undefined) {
      value = null
    } else {
      value = source
    }

    yield value

    if (!column.retainExtraProp) {
      omittedFromExtraProps.push(column.source)
    }
  }

  if (extraProps) {
    yield JSON.stringify(_.omit(doc, omittedFromExtraProps))
  }
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
