// Vite 产物同步到 Rust 源码目录；cargo build 自身不调用 Node。
// --check 在临时目录重建、逐文件比较，不改已生成的资源。
import { build } from "vite";
import { readdir, readFile, mkdir, mkdtemp, rm, cp } from "node:fs/promises";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { tmpdir } from "node:os";
import { gzipSync } from "node:zlib";

const root = dirname(fileURLToPath(import.meta.url));
const embedded = join(root, "../crates/server/src/ui");
const checking = process.argv.includes("--check");
const temporary = checking ? await mkdtemp(join(tmpdir(), "amberlog-ui-build-")) : null;
const output = temporary ?? join(root, "dist");

async function files(dir, prefix = "") {
  const names = [];
  for (const entry of await readdir(join(dir, prefix), { withFileTypes: true })) {
    const relative = prefix ? `${prefix}/${entry.name}` : entry.name;
    if (entry.isDirectory()) names.push(...await files(dir, relative));
    else if (entry.isFile()) names.push(relative);
    else throw new Error(`不支持的资源：${relative}`);
  }
  return names.sort();
}

try {
  await build({ root, build: { outDir: output, emptyOutDir: true } });
  const runtime = (await files(output)).filter((name) => name === "index.html" || name.startsWith("assets/"));
  if (!runtime.includes("index.html") || !runtime.some((name) => name.endsWith(".js")) || !runtime.some((name) => name.endsWith(".css"))) {
    throw new Error("缺少 HTML、JS 或 CSS 产物");
  }
  // 多文件产物按完整资源集计量，不再限制为单个 200 KiB HTML。
  // 提高预算须在 PR 中说明；gzip 用于比较传输体积，服务按原始字节返回。
  let total = 0;
  let compressed = 0;
  for (const name of runtime) {
    const bytes = await readFile(join(output, name));
    total += bytes.length;
    compressed += gzipSync(bytes).length;
    console.log(`${name}: ${bytes.length} B，gzip ${gzipSync(bytes).length} B`);
  }
  if (total > 512 * 1024 || compressed > 192 * 1024) {
    throw new Error(`资源超过预算：${total} B / gzip ${compressed} B（上限 512 KiB / 192 KiB）`);
  }
  if (checking) {
    const current = await files(embedded);
    if (JSON.stringify(current) !== JSON.stringify(runtime)) throw new Error("嵌入资源清单与源码不一致，请运行 pnpm build");
    for (const name of runtime) {
      if (!(await readFile(join(output, name))).equals(await readFile(join(embedded, name)))) {
        throw new Error(`嵌入资源与源码不一致：${name}，请运行 pnpm build`);
      }
    }
  } else {
    // 只同步构建产物；过期哈希资源同时删除，避免持续增大二进制。
    await mkdir(embedded, { recursive: true });
    const obsolete = (await files(embedded)).filter((name) => !runtime.includes(name));
    for (const name of obsolete) await rm(join(embedded, name));
    for (const name of runtime) {
      await mkdir(dirname(join(embedded, name)), { recursive: true });
      await cp(join(output, name), join(embedded, name));
    }
  }
  console.log(`${checking ? "资源与源码一致" : "已同步 Rust 嵌入资源"}：${runtime.length} 个文件，共 ${total} B，gzip ${compressed} B`);
} finally {
  if (temporary !== null) await rm(temporary, { recursive: true, force: true });
}
