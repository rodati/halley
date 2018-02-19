'use strict'

const argv = require('yargs')
  .option('collections', {
    describe: 'Collection map YAML file',
    nargs: 1,
    default: 'collections-1-core.yml',
    required: true,
    alias: 'c'
  })

  .option('sql', {
    describe: 'SQL server to connect to',
    nargs: 1,
    default: 'postgres://postgres:postgres@localhost:5432/sirena-core-staging',
    required: true
  })

  .option('mongo', {
    describe: 'Mongo connection string',
    nargs: 1,
    default: 'mongodb://localhost/local',
    required: true
  })

  .option('incremental-import', {
    describe: 'Do an incremental import',
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
    choices: ['ignore', 'citus-multi-shard', 'normal'],
    default: 'normal'
  })

  .option('log', {
    describe: 'Set the log level',
    choices: ['error', 'warn', 'info', 'verbose', 'debug', 'silly'],
    default: 'info',
    alias: 'l'
  })

  .help('h')
  .alias('help', 'h')

  .argv

require('./lib/main')(argv)
