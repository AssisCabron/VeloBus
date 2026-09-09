import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { readFile, access, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { root, workRoot, startBroker, temporaryDirectory, removeTemporaryDirectory } from './harness.mjs';

const exec = promisify(execFile);
const { version } = JSON.parse(await readFile(join(root, 'packages/client/package.json'), 'utf8'));
const artifact = `nodara-${version}.tgz`;
const published = process.env.NODARA_PACKAGE_SPEC;
if (published && !/^nodara@\d+\.\d+\.\d+$/.test(published)) throw new Error('NODARA_PACKAGE_SPEC must pin nodara@x.y.z');
if (!published) {
  await exec(process.execPath, ['scripts/pack-client.mjs'], { cwd: root });
  await access(join(root, 'artifacts', artifact));
}
const source = published ?? join(root, 'artifacts', artifact);
const project = await temporaryDirectory(); const servers = [];
try {
  await writeFile(join(project, 'package.json'), JSON.stringify({ name: 'nodara-install-check', version: '1.0.0', private: true }));
  await exec('npm', ['install', '--ignore-scripts', '--no-audit', '--no-fund', '--registry=https://registry.npmjs.org/', source], {
    cwd: project, env: { ...process.env, npm_config_cache: join(workRoot, 'npm-cache') },
  });
  for (let i = 0; i < 2; i++) servers.push(await startBroker());
  const brokers = JSON.stringify(servers.map(server => ({ host: '127.0.0.1', port: server.port })));
  const common = `const bus = await connect({port:${servers[0].port}}); const handler = await bus.handleJSON('installed.echo', async body => body, {concurrency:2,queueLimit:4}); try { const result = await bus.requestJSON('installed.echo', {works:true}); if (result.works !== true) throw new Error('RPC package smoke failed'); await bus.ping(); } finally { await handler.close(); await bus.close(); }
  const cluster = await connectCluster({brokers:${brokers}}); const worker = await cluster.handleJSON('installed.cluster', async body=>body); try { if(worker.nodes().length!==2) throw new Error('Missing cluster registration'); for(let i=0;i<4;i++){ const value=await cluster.requestJSON('installed.cluster',{id:i}); if(value.id!==i)throw new Error('Cluster response mismatch'); } } finally {await worker.close(); await cluster.close();}`;
  await exec(process.execPath, ['--input-type=module', '-e', `import {connect,connectCluster} from 'nodara'; ${common}`], { cwd: project });
  await exec(process.execPath, ['-e', `const {connect,connectCluster} = require('nodara'); (async()=>{${common}})().catch(e=>{console.error(e);process.exitCode=1;});`], { cwd: project });
  for (const name of ['LLM.txt', 'llms.txt', 'LICENSE']) {
    const value = await readFile(join(project, 'node_modules/nodara', name), 'utf8');
    if (!value.length || value !== await readFile(join(root, 'packages/client', name), 'utf8')) throw new Error(`Packaged ${name} differs from source`);
  }
  await writeFile(join(project, 'contract.mts'), `import {connectCluster, type ClusterNodeStatus} from 'nodara';\nconst bus=await connectCluster({brokers:[{host:'localhost'}]});\nconst service=await bus.handleJSON<{id:string},{id:string}>('typed',async body=>body);\nconst result=await bus.requestJSON<{id:string}>('typed',{id:'x'});\nconst id:string=result.id; const nodes:ClusterNodeStatus[]=bus.nodes();\nawait service.close(); await bus.close();\n`);
  await exec(process.execPath, [join(root, 'packages/client/node_modules/typescript/bin/tsc'), '--noEmit', '--strict', '--module', 'NodeNext', '--target', 'ES2022', '--typeRoots', join(root, 'packages/client/node_modules/@types'), 'contract.mts'], { cwd: project });
  console.log(JSON.stringify({ source, cleanInstall: 'passed', esm: 'passed', commonjs: 'passed', typescript: 'passed', singleBroker: 'passed', multiBroker: 'passed', agentDocsAndLicense: 'passed' }, null, 2));
} finally {
  await Promise.allSettled(servers.map(server => server.stop()));
  await removeTemporaryDirectory(project);
}
