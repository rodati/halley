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
    choices: ['ignore', 'normal', 'ignore-past'],
    default: 'ignore-past'
  })

  .option('db-mode', {
    describe: 'Listen to changes in a single or multiple databases',
    choices: ['multi', 'single'],
    default: 'single'
  })

  .option('db-name', {
    describe:
      'Database name. Use with db-mode single. If not set, it defaults to the database specified in the Mongo connection string.',
    string: true
  })

  .option('copy-batch', {
    describe: 'If is false, will try to sync documents one by one instead of batch',
    boolean: true,
    default: true
  })

  .option('exit-on-error', {
    describe: 'If should stop the process on any error doing a full/incremental import',
    boolean: true,
    default: false
  })

  .option('table-init', {
    describe:
      'Making a full import: if should drop and create the postgres table where will be imported the documents and runs the table_init commands after the importation was done',
    boolean: true,
    default: true
  })

  .help('h')
  .alias('help', 'h').argv

require('./src/main')(argv)
