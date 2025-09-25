import path from 'path';
import fs from 'fs';
import { Logger } from 'pino';

export type TransformFn = (doc: any) => any;

export function loadTransformModule(transformPath?: string, logger?: Logger): TransformFn | undefined {
  if (!transformPath) return undefined;
  const resolved = path.isAbsolute(transformPath) ? transformPath : path.resolve(process.cwd(), transformPath);
  if (!fs.existsSync(resolved)) {
    throw new Error(`Transform module not found at ${resolved}`);
  }
  // Use require to load either JS or transpiled TS. In bundle, this file can require user-provided JS at runtime.
  // eslint-disable-next-line @typescript-eslint/no-var-requires
  const mod = require(resolved);
  if (typeof mod === 'function') return mod;
  if (mod && typeof mod.transform === 'function') return mod.transform;
  logger?.info({ path: resolved }, 'Loaded transform module but did not find exported function; continuing without transform');
  return undefined;
}
