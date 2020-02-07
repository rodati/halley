#!/usr/bin/env node
'use strict'

const argv = require('yargs')
  .env('HALLEY')

  .option('collections', {
    describe: 'Collection map YAML file',
    nargs: 1,
    required: true,
    alias: 'c'
  })

  .option('sql', {
    describe: 'SQL server to connect to',
    nargs: 1,
    default: 'postgres://postgres:postgres@localhost:5432/db'
  })

  .option('mongo', {
    describe: 'Mongo connection string',
    nargs: 1,
    default: 'mongodb://localhost/local'
  })

  .option('incremental-import', {
    describe: 'Do an incremental import (experimental)',
    boolean: true,
    default: false,
    alias: 'i'
  })

  .option('concurrency', {
    describe: 'PG client concurrency',
    number: true,
    default: 1,
    alias: 'n'
  })

  .option('delete-mode', {
    describe: 'How to handle deletes',
    choices: ['ignore', 'normal'],
    default: 'normal'
  })

  .option('db-mode', {
    describe: 'Listen to changes in a single or multiple databases',
    choices: ['multi', 'single'],
    default: 'multi'
  })

  .option('db-name', {
    describe: 'Database name. Use with db-mode single. If not set, it defaults to the database specified in the Mongo connection string.',
    string: true
  })

  .option('continue-on-error', {
    describe: 'If should continue the process on any error doing a full/incremental import',
    boolean: false,
    default: false
  })

  .option('drop-table', {
    describe: 'If should drop the table before make a full import',
    boolean: true,
    default: true
  })

  .help('h')
  .alias('help', 'h')

  .argv

require('./src/main')(argv)
