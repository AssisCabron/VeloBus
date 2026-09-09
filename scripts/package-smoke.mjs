import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { readFile, access, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { root, workRoot, startBroker, temporaryDirectory, removeTemporaryDirectory } from './harness.mjs';

const exec = promisify(execFile);
await exec(process.execPath, ['scripts/pack-client.mjs'], { cwd: root });
const { version } = JSON.parse(await readFile(join(root, 'packages/client/package.json'), 'utf8'));
const artifact = `velobus-client-${version}.tgz`;
await access(join(root, 'artifacts', artifact));
const project = await temporaryDirectory(); let server;
try {
  await writeFile(join(project, 'package.json'), JSON.stringify({ name: 'velobus-install-check', version: '1.0.0', private: true }));
  await exec('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', join(root, 'artifacts', artifact)], {
    cwd: project, env: { ...process.env, npm_config_cache: join(workRoot, 'npm-cache') },
  });
  server = await startBroker();
  const common = `const bus = await connect({port:${server.port}}); const handler = await bus.handleJSON('installed.echo', async body => body, {concurrency:2,queueLimit:4}); try { const result = await bus.requestJSON('installed.echo', {works:true}); if (result.works !== true) throw new Error('RPC package smoke failed'); await bus.ping(); } finally { await handler.close(); await bus.close(); }`;
  await exec(process.execPath, ['--input-type=module', '-e', `import {connect} from '@velobus/client'; ${common}`], { cwd: project });
  await exec(process.execPath, ['-e', `const {connect} = require('@velobus/client'); (async()=>{${common}})().catch(e=>{console.error(e);process.exitCode=1;});`], { cwd: project });
  console.log(JSON.stringify({ artifact: join(root, 'artifacts', artifact), cleanInstall: 'passed', esm: 'passed', commonjs: 'passed', serverIntegration: 'passed' }, null, 2));
} finally {
  if (server) await server.stop();
  await removeTemporaryDirectory(project);
}
