import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdir } from 'node:fs/promises';
import { join } from 'node:path';
import { root, workRoot } from './harness.mjs';

const output = join(root, 'artifacts');
await mkdir(output, { recursive: true });
const result = await promisify(execFile)('npm', ['pack', '--pack-destination', output], {
  cwd: join(root, 'packages/client'),
  env: { ...process.env, npm_config_cache: join(workRoot, 'npm-cache') },
  maxBuffer: 1024 * 1024,
});
process.stdout.write(result.stdout);
process.stderr.write(result.stderr);
