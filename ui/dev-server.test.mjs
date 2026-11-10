import { test } from 'node:test';
import assert from 'node:assert/strict';
import { createServer as createHttpServer } from 'node:http';
import { readFile, mkdtemp, writeFile, rm, realpath } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { once } from 'node:events';
import { fileURLToPath } from 'node:url';
import { createServer } from 'vite';
import configuration, { backendTarget } from './vite.config.ts';

async function freePort() {
  const probe = createHttpServer();
  await new Promise(resolve => probe.listen(0, '127.0.0.1', resolve));
  const port = probe.address().port;
  await new Promise(resolve => probe.close(resolve));
  return port;
}

test('backend target accepts only HTTP origins', () => {
  assert.equal(backendTarget(undefined), 'http://127.0.0.1:8800');
  assert.equal(backendTarget('https://localhost:9443/'), 'https://localhost:9443');
  for (const target of ['', 'ftp://localhost', 'http://user:secret@localhost',
    'http://localhost/path', 'http://localhost?token=x', 'http://localhost#x']) {
    assert.throws(() => backendTarget(target), /UI_BACKEND_URL/);
  }
});

test('environment files load from ui and shell overrides them', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'amberlog-ui-env-'));
  const previous = process.env.UI_BACKEND_URL;
  try {
    await writeFile(join(directory, '.env.local'), 'UI_BACKEND_URL=http://127.0.0.1:12345\n');
    delete process.env.UI_BACKEND_URL;
    const fromFile = configuration({ mode: 'development', envDir: directory });
    assert.equal(fromFile.server.proxy['/api/'].target, 'http://127.0.0.1:12345');
    process.env.UI_BACKEND_URL = 'http://127.0.0.1:23456';
    assert.equal(configuration({ mode: 'development', envDir: directory }).server.proxy['/api/'].target,
      'http://127.0.0.1:23456');
  } finally {
    if (previous === undefined) delete process.env.UI_BACKEND_URL;
    else process.env.UI_BACKEND_URL = previous;
    await rm(directory, { recursive: true });
  }
});

test('development server serves source, proxies errors and never writes embedded HTML', { timeout: 30000 }, async () => {
  const artifact = new URL('../crates/server/src/ui/index.html', import.meta.url);
  const before = await readFile(artifact);
  const backend = createHttpServer(async (req, res) => {
    const chunks = [];
    for await (const chunk of req) chunks.push(chunk);
    res.setHeader('Content-Type', 'application/json');
    res.setHeader('Connection', 'close');
    res.setHeader('Retry-After', '3');
    res.statusCode = req.headers.authorization === 'Bearer test-token' ? 429 : 401;
    res.end(JSON.stringify({ method: req.method, url: req.url,
      authorization: req.headers.authorization, body: Buffer.concat(chunks).toString() }));
  });
  await new Promise(resolve => backend.listen(0, '127.0.0.1', resolve));
  const target = `http://127.0.0.1:${backend.address().port}`;
  const options = configuration({ mode: 'development' });
  for (const rule of Object.values(options.server.proxy)) rule.target = target;
  const server = await createServer({ ...options, configFile: false,
    logLevel: 'silent', optimizeDeps: { noDiscovery: true },
    server: { ...options.server, preTransformRequests: false, port: await freePort() } });
  try {
    await server.listen();
    const address = `http://127.0.0.1:${server.httpServer.address().port}`;
    for (const [path, location] of [['/ui', '/ui/'], ['/ui?screen=search', '/ui/?screen=search']]) {
      const redirect = await fetch(address + path, { redirect: 'manual' });
      assert.equal(redirect.status, 302);
      assert.equal(redirect.headers.get('Location'), location);
    }
    const page = await (await fetch(address + '/ui/')).text();
    assert.match(page, /\/ui\/src\/main.tsx/);
    assert.match(page, /@vite\/client/);
    const source = await (await fetch(address + '/ui/src/main.tsx')).text();
    assert.match(source, /App/);
    assert.doesNotMatch(source, /UI_BACKEND_URL/);
    const response = await fetch(address + '/api/default/_search?q=example', {
      method: 'POST', headers: { Authorization: 'Bearer test-token' }, body: '{"query":1}' });
    assert.equal(response.status, 429);
    assert.equal(response.headers.get('Retry-After'), '3');
    assert.deepEqual(await response.json(), { method: 'POST', url: '/api/default/_search?q=example',
      authorization: 'Bearer test-token', body: '{"query":1}' });
    for (const path of ['/api/default/_search', '/healthz', '/readyz', '/healthz?details=1', '/readyz?probe=1']) {
      const unauthorized = await fetch(address + path);
      assert.equal(unauthorized.status, 401);
      assert.equal((await unauthorized.json()).url, path);
    }
    const collision = await createServer({ ...options, configFile: false, logLevel: 'silent',
      server: { ...options.server, port: server.httpServer.address().port } });
    try { await assert.rejects(collision.listen(), /already in use/); }
    finally { await collision.close(); }
    await new Promise(resolve => backend.close(resolve));
    const unavailable = await fetch(address + '/api/default/_search', { signal: AbortSignal.timeout(5000) });
    assert.ok(unavailable.status >= 500);
    assert.doesNotMatch(await unavailable.text(), /<!doctype html>/i);
    assert.equal((await fetch(address)).status, 200);
    assert.deepEqual(await readFile(artifact), before);
  } finally {
    server.httpServer?.closeAllConnections();
    await server.close();
    if (backend.listening) await new Promise(resolve => backend.close(resolve));
  }
});

test('CSS updates in place and modules without an HMR boundary reload the page', async () => {
  const directory = await realpath(await mkdtemp(fileURLToPath(new URL('./.amberlog-ui-watch-', import.meta.url))));
  const css = join(directory, 'style.css');
  const script = join(directory, 'main.tsx');
  const plain = join(directory, 'helper.js');
  const html = await readFile(new URL('./index.html', import.meta.url), 'utf8');
  await writeFile(join(directory, 'index.html'), html.replace('/src/main.tsx', '/main.tsx'));
  await writeFile(script, 'import "./style.css"; import "./helper.js"; document.body.dataset.example = "one";');
  await writeFile(css, 'body { color: red; }');
  await writeFile(plain, 'export const value = 1;');
  const options = configuration({ mode: 'development' });
  const server = await createServer({ ...options, root: directory, configFile: false, logLevel: 'silent',
    server: { ...options.server, port: await freePort(), fs: { allow: [directory] } } });
  let socket;
  try {
    await server.listen();
    const address = `127.0.0.1:${server.httpServer.address().port}`;
    socket = new WebSocket(`ws://${address}/ui/?token=${server.config.webSocketToken}`, 'vite-hmr');
    await once(socket, 'open');
    for (const path of ['/', '/main.tsx', '/style.css', '/helper.js']) {
      const response = await fetch(`http://${address}/ui${path}`);
      const body = await response.text();
      assert.equal(response.status, 200, `${path}: ${body.slice(0, 500)}`);
    }
    const changed = async (file, content, predicate) => {
      const message = new Promise((resolve, reject) => {
        const timer = setTimeout(() => { socket.removeEventListener('message', listener); reject(new Error(`No update for ${file}`)); }, 10000);
        const listener = event => {
          const data = JSON.parse(event.data);
          if (predicate(data)) {
            clearTimeout(timer);
            socket.removeEventListener('message', listener);
            resolve(data);
          }
        };
        socket.addEventListener('message', listener);
      });
      await writeFile(file, content);
      return message;
    };
    const update = await changed(css, 'body { color: blue; }', data => data.type === 'update');
    assert.ok(update.updates.some(item => ['css-update', 'js-update'].includes(item.type) && item.path === '/style.css'));
    for (const file of [script, plain, script]) {
      const update = await changed(file, await readFile(file, 'utf8') + '\n// changed\n', data => data.type === 'full-reload');
      assert.equal(update.path, '*');
    }
    // Vite reports transformation failures and serves fixed source again.
    await writeFile(script, 'export const broken = <');
    assert.equal((await fetch(`http://${address}/ui/main.tsx`)).status, 500);
    await writeFile(script, 'document.body.dataset.example = "fixed";');
    assert.equal((await fetch(`http://${address}/ui/main.tsx`)).status, 200);
  } finally {
    if (socket) socket.close();
    server.httpServer?.closeAllConnections();
    await server.close();
    await rm(directory, { recursive: true, force: true });
  }
});
