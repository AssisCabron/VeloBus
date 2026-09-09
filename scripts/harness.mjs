import { spawn } from 'node:child_process';
import { existsSync } from 'node:fs';
import { mkdir, mkdtemp, rm } from 'node:fs/promises';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';

export const root = resolve(dirname(fileURLToPath(import.meta.url)), '..');
export const workRoot = resolve(process.env.NODARA_WORK_DIR ?? join(root, 'work'));
export const scratch = join(workRoot, 'validation');

export async function temporaryDirectory(prefix = 'nodara-') {
  await mkdir(scratch, { recursive: true });
  return mkdtemp(join(scratch, prefix));
}

export async function startBroker({ dataDir, args = [], token, binary } = {}) {
  const executable = binary ?? process.env.NODARA_BIN ?? join(root, 'target', 'debug', 'nodara');
  if (!existsSync(executable)) throw new Error(`Compile primeiro: cargo build. Binário ausente: ${executable}`);
  const env = { ...process.env };
  delete env.NODARA_TOKEN;
  if (token !== undefined) env.NODARA_TOKEN = token;
  const child = spawn(executable, ['--listen', '127.0.0.1:0', ...(dataDir ? ['--data-dir', dataDir] : ['--memory']), ...args], {
    cwd: root, env, stdio: ['ignore', 'pipe', 'pipe'],
  });
  let stderr = '';
  let stdout = '';
  child.stderr.on('data', chunk => { stderr = (stderr + chunk).slice(-16000); });
  const exited = new Promise(resolveExit => child.once('exit', (code, signal) => resolveExit({ code, signal })));
  try {
    const ready = await new Promise((resolveReady, reject) => {
      const timer = setTimeout(() => reject(new Error(`Broker não ficou pronto: ${stderr}`)), 20000);
      const finish = (err, value) => { clearTimeout(timer); err ? reject(err) : resolveReady(value); };
      child.once('error', err => finish(err));
      child.once('exit', (code, signal) => finish(new Error(`Broker encerrou (${code ?? signal}): ${stderr}`)));
      child.stdout.on('data', chunk => {
        stdout += chunk;
        let nl;
        while ((nl = stdout.indexOf('\n')) !== -1) {
          const line = stdout.slice(0, nl); stdout = stdout.slice(nl + 1);
          try {
            const message = JSON.parse(line);
            if (message.event === 'ready') finish(null, message);
          } catch { /* only readiness JSON is part of this interface */ }
        }
      });
    });
    const port = Number(ready.address.split(':').at(-1));
    if (!Number.isInteger(port) || port <= 0) throw new Error(`Endereço inválido: ${ready.address}`);
    return {
      child, port, ready, executable,
      stderr: () => stderr,
      async stop(signal = 'SIGTERM') {
        if (child.exitCode === null && child.signalCode === null) child.kill(signal);
        let timer;
        const deadline = new Promise(resolveDeadline => {
          timer = setTimeout(() => { child.kill('SIGKILL'); resolveDeadline(); }, 4000);
        });
        await Promise.race([exited, deadline]);
        clearTimeout(timer);
        return exited;
      },
    };
  } catch (error) {
    child.kill('SIGKILL');
    await exited;
    throw error;
  }
}

export async function removeTemporaryDirectory(path) {
  // Only directories created under our dedicated scratch location may be removed.
  if (!resolve(path).startsWith(`${scratch}/nodara-`)) throw new Error('Diretório temporário fora do escopo');
  await rm(path, { recursive: true, force: true });
}
