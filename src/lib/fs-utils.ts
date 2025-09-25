import fs from 'fs';
import path from 'path';
import { pipeline } from 'stream/promises';
import zlib from 'zlib';
import ndjson from 'ndjson';

export function collectionFilename(outputDir: string, collectionName: string, gzip: boolean) {
  const safeName = collectionName.replace(/[\/\\?%*:|"<> ]+/g, '_');
  return path.join(outputDir, `${safeName}.ndjson${gzip ? '.gz' : ''}`);
}

export async function writeNdjsonStream(outputPath: string, readable: NodeJS.ReadableStream, gzip = false) {
  if (gzip) {
    const gz = zlib.createGzip();
    const ws = fs.createWriteStream(outputPath, { flags: 'w' });
    await pipeline(readable, gz, ws);
  } else {
    const ws = fs.createWriteStream(outputPath, { flags: 'w' });
    await pipeline(readable, ws);
  }
}

export async function readNdjsonStream(inputPath: string, gzip = false):Promise<any> {
  const rs = fs.createReadStream(inputPath);
  if (gzip) {
    const gunzip = zlib.createGunzip();
    return rs.pipe(gunzip).pipe(ndjson.parse());
  } else {
    return rs.pipe(ndjson.parse());
  }
}
