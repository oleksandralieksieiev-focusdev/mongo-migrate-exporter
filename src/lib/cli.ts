import yargs from 'yargs';
import { hideBin } from 'yargs/helpers';
import { Config, buildConfigFromEnvAndArgs } from './config';
import { createLogger } from './logger';
import { Migrator } from './migrator';

export function createCli() {
  const argv = yargs(hideBin(process.argv))
    .scriptName('mongo-migrate-exporter')
    .usage('Usage: $0 [options]')
    .option('uri', {
      type: 'string',
      description: 'MongoDB connection URI for source (overrides env MONGO_URI)'
    })
    .option('db', {
      type: 'string',
      description: 'Source database name (overrides env MONGO_DB)'
    })
    .option('dest-uri', {
      type: 'string',
      description: 'Destination MongoDB URI for import/migrate mode'
    })
    .option('dest-db', { type: 'string', description: 'Destination database name' })
    .option('out', { type: 'string', description: 'Output directory for exported files' })
    .option('mode', { type: 'string', choices: ['export', 'import', 'migrate'], default: 'export', description: 'Operation mode' })
    .option('collections', { type: 'string', description: 'Comma-separated list of collections to include (default: all)' })
    .option('exclude', { type: 'string', description: 'Comma-separated list of collections to exclude' })
    .option('gzip', { type: 'boolean', description: 'Gzip output files', default: false })
    .option('concurrency', { type: 'number', default: 4 })
    .option('batch', { type: 'number', default: 1000 })
    .option('log', { type: 'string', default: 'info' })
    .option('transform', { type: 'string', description: 'Path to a JS/TS transform module exporting "transform(doc): doc". Optional.' })
    .help()
    .alias('h', 'help')
    .version()
    .parseSync();

  let config: Config;
  try {
    config = buildConfigFromEnvAndArgs(argv);
  } catch (err) {
    // eslint-disable-next-line no-console
    console.error('Configuration error:', (err as Error).message);
    process.exit(2);
  }

  const logger = createLogger(config.logLevel);

  return {
    async run() {
      const migrator = new Migrator(config, logger);
      try {
        await migrator.run();
        logger.info('Operation completed successfully.');
        process.exit(0);
      } catch (err) {
        logger.error({ err }, 'Operation failed');
        process.exit(1);
      }
    }
  };
}
