import fs from 'fs';
import path from 'path';
import dotenv from 'dotenv';

dotenv.config();

export type Mode = 'export' | 'import' | 'migrate';

export interface Config {
  mode: Mode;
  srcUri: string;
  srcDb: string;
  destUri?: string;
  destDb?: string;
  outputDir: string;
  collectionsInclude?: string[];
  collectionsExclude?: string[];
  gzip: boolean;
  concurrency: number;
  batchSize: number;
  logLevel: string;
  transformPath?: string;
}

function coerceNumber(v: any, fallback: number) {
  const n = Number(v);
  return Number.isFinite(n) && n > 0 ? Math.floor(n) : fallback;
}

export function buildConfigFromEnvAndArgs(argv: any): Config {
  const mode = (argv.mode || process.env.MODE || 'export') as Mode;
  if (!['export', 'import', 'migrate'].includes(mode)) {
    throw new Error(`Invalid mode: ${mode}`);
  }

  const srcUri = argv.uri || process.env.MONGO_URI;
  const srcDb = argv.db || process.env.MONGO_DB;
  if (!srcUri || !srcDb) {
    throw new Error('Source Mongo URI and DB must be provided via args or environment (MONGO_URI, MONGO_DB).');
  }

  const destUri = argv['dest-uri'] || process.env.DEST_MONGO_URI;
  const destDb = argv['dest-db'] || process.env.DEST_MONGO_DB;

  if ((mode === 'import' || mode === 'migrate') && (!destUri || !destDb)) {
    throw new Error('Destination URI and DB must be provided for import/migrate modes.');
  }

  const outputDir = argv.out || process.env.OUTPUT_DIR || './backups';
  const include = argv.collections ? String(argv.collections).split(',').map(s => s.trim()).filter(Boolean) : undefined;
  const exclude = argv.exclude ? String(argv.exclude).split(',').map(s => s.trim()).filter(Boolean) : undefined;

  const gzip = typeof argv.gzip === 'boolean' ? argv.gzip : (process.env.GZIP_OUTPUT === 'true');

  const concurrency = coerceNumber(argv.concurrency || process.env.CONCURRENCY, 4);
  const batchSize = coerceNumber(argv.batch || process.env.BATCH_SIZE, 1000);

  const logLevel = argv.log || process.env.LOG_LEVEL || 'info';

  const transformPath = argv.transform || process.env.TRANSFORM_PATH;

  // Ensure output dir exists if export involved
  if (mode === 'export' || mode === 'migrate') {
    if (!fs.existsSync(outputDir)) {
      fs.mkdirSync(outputDir, { recursive: true });
    }
  }

  return {
    mode,
    srcUri,
    srcDb,
    destUri,
    destDb,
    outputDir: path.resolve(outputDir),
    collectionsInclude: include,
    collectionsExclude: exclude,
    gzip,
    concurrency,
    batchSize,
    logLevel,
    transformPath
  };
}
