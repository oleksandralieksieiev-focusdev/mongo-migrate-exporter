import { Config } from './config';
import { createLogger } from './logger';
import { connect, close, getDb } from './mongo-client';
import fs from 'fs';
import stream from 'stream';
import { collectionFilename, writeNdjsonStream, readNdjsonStream } from './fs-utils';
import ndjson from 'ndjson';
import { TransformFn, loadTransformModule } from './transform-loader';
import prettyBytes from 'pretty-bytes';

export class Migrator {
  private config: Config;
  private logger: ReturnType<typeof createLogger>;
  private transform?: TransformFn;

  constructor(config: Config, logger: ReturnType<typeof createLogger>) {
    this.config = config;
    this.logger = logger;
    if (config.transformPath) {
      try {
        this.transform = loadTransformModule(config.transformPath, logger);
      } catch (err) {
        logger.error({ err }, 'Failed to load transform module');
        throw err;
      }
    }
  }

  async run() {
    if (this.config.mode === 'export') {
      await this.exportAll();
    } else if (this.config.mode === 'import') {
      await this.importAll();
    } else if (this.config.mode === 'migrate') {
      await this.exportAll();
      await this.importAll();
    } else {
      throw new Error(`Unsupported mode: ${this.config.mode}`);
    }
  }

  private async listCollections(db: any) {
    const list = await db.listCollections().toArray();
    return list.map((c: any) => c.name);
  }

  private shouldInclude(collectionName: string) {
    const inc = this.config.collectionsInclude;
    const exc = this.config.collectionsExclude;
    if (inc && inc.length) {
      return inc.includes(collectionName);
    }
    if (exc && exc.length) {
      return !exc.includes(collectionName);
    }
    return true;
  }

  async exportAll() {
    const client = await connect(this.config.srcUri);
    try {
      const db = getDb(client, this.config.srcDb);
      const collections = await (await db).listCollections().toArray();
      const names = collections.map((c: any) => c.name).filter((n: string) => this.shouldInclude(n));
      this.logger.info({ count: names.length }, 'Collections to export');
      for (const name of names) {
        await this.exportCollection(name, db);
      }
    } finally {
      await close(client);
    }
  }

  private async exportCollection(name: string, db: any) {
    this.logger.info({ collection: name }, 'Exporting collection');
    const coll = db.collection(name);
    const cursor = coll.find({});
    // Create a readable object stream of ndjson lines
    const transformToNdjson = new stream.Transform({
      writableObjectMode: true,
      readableObjectMode: false,
      transform(doc: any, _enc, cb) {
        try {
          const json = JSON.stringify(doc);
          cb(null, json + '\n');
        } catch (unknownErr) {
          const err = unknownErr instanceof Error
              ? unknownErr
              : new Error(typeof unknownErr === 'string' ? unknownErr : 'Unknown error during JSON.stringify');
          cb(err);
        }
      }
    });

    // Optionally apply transform per document
    const applyTransform = new stream.Transform({
      writableObjectMode: true,
      readableObjectMode: true,
      transform: (doc: any, _enc, cb) => {
        try {
          const out = this.transform ? this.transform(doc) : doc;
          cb(null, out);
        } catch (err) {
          cb(err as Error);
        }
      }
    });

    // Create a readable stream that reads from cursor
    const readable = new stream.Readable({
      objectMode: true,
      read() { }
    });

    // Start reading cursor asynchronously
    (async () => {
      try {
        for await (const doc of cursor) {
          if (!readable.push(doc)) {
            // backpressure: wait a tick
            await new Promise(res => setImmediate(res));
          }
        }
        readable.push(null);
      } catch (err) {
        readable.destroy(err as Error);
      }
    })();

    const outPath = collectionFilename(this.config.outputDir, name, this.config.gzip);
    const fileSizeBefore = fs.existsSync(outPath) ? fs.statSync(outPath).size : 0;
    await writeNdjsonStream(outPath, readable.pipe(applyTransform).pipe(transformToNdjson), this.config.gzip);
    const fileSize = fs.statSync(outPath).size - fileSizeBefore;
    this.logger.info({ collection: name, bytes: prettyBytes(fileSize), path: outPath }, 'Exported collection to file');
  }

  async importAll() {
    const inputDir = this.config.outputDir;
    const files = fs.readdirSync(inputDir).filter(f => f.endsWith('.ndjson') || f.endsWith('.ndjson.gz'));
    if (files.length === 0) {
      this.logger.warn({ inputDir }, 'No backup files found for import');
      return;
    }

    const client = await connect(this.config.destUri!);
    try {
      const db = getDb(client, this.config.destDb!);
      for (const file of files) {
        const full = require('path').join(inputDir, file);
        const gzip = file.endsWith('.gz');
        const collectionName = file.replace(/\.ndjson(?:\.gz)?$/, '');
        await this.importFileToCollection(full, collectionName, db, gzip);
      }
    } finally {
      await close(client);
    }
  }

  private async importFileToCollection(filePath: string, collectionName: string, db: any, gzip: boolean) {
    this.logger.info({ file: filePath, collection: collectionName }, 'Importing file into collection');
    const parser = ndjson.parse();
    const rs: any = fs.createReadStream(filePath);
    const streamToUse = gzip ? rs.pipe(require('zlib').createGunzip()).pipe(parser) : rs.pipe(parser);
    // Collect batches and insert
    const batchSize = this.config.batchSize;
    let batch: any[] = [];
    let total = 0;
    for await (const obj of streamToUse) {
      batch.push(obj);
      if (batch.length >= batchSize) {
        await this.insertBatch(db, collectionName, batch);
        total += batch.length;
        batch = [];
      }
    }
    if (batch.length) {
      await this.insertBatch(db, collectionName, batch);
      total += batch.length;
    }
    this.logger.info({ collection: collectionName, inserted: total }, 'Imported file');
  }

  private async insertBatch(db: any, collectionName: string, docs: any[]) {
    const coll = db.collection(collectionName);
    // Try ordered:false for better throughput; handle errors
    try {
      await coll.insertMany(docs, { ordered: false });
    } catch (err: any) {
      // MongoBulkWriteError possible (some duplicate _id etc.). Log and continue.
      this.logger.warn({ err: err.message }, 'Partial failure on insertMany; some documents may have failed.');
    }
  }
}
