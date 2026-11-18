// 检索界面的构建脚本（ADR-0060 第 2、3、7 条）：Vite 把每个入口打成恰一个 JS 与至多一个 CSS，内联进该入口的 HTML 模板，
// 写到 crates/server/src/ui/ 下（由 amberlog-server 以 include_str! 编入）。不分块、不出资源文件、不加 /ui/assets/*。
// 两个入口：外壳 src/main.tsx → index.html；回调页 src/callback/main.ts → callback.html（REQ-AMB-UI-CONSOLE-V4-CALLBACK-PAGE）。
// 模板就是开发服务用的 HTML：其中 `<script type="module" src="…">` 一行换成内联的样式与脚本。
//   node build.mjs          构建并写入产物
//   node build.mjs --check  构建后与已提交的产物逐字节比对，不一致即失败（CI 的 ui 作业用它核对产物）
// 产物超过 SIZE_LIMIT 即失败（每个产物分别计）；提高门限要在提交的改动说明里写理由。
// 本脚本只写下面列出的产物文件，不删 crates/server/src/ui/ 下的其他文件（那里还有 Rust 源码）。

import { build } from "vite";
import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const SIZE_LIMIT = 200 * 1024;
const here = (p) => fileURLToPath(new URL(p, import.meta.url));
const ENTRIES = [
  { entry: "src/main.tsx", template: "index.html", out: "index.html" },
  { entry: "src/callback/main.ts", template: "src/callback/index.html", out: "callback.html" },
];

async function page({ entry, template }) {
  const result = await build({
    configFile: here("vite.config.ts"),
    mode: "production",
    logLevel: "warn",
    build: {
      write: false,
      emptyOutDir: false,
      rollupOptions: { input: here(entry) },
    },
  });
  const outputs = (Array.isArray(result) ? result : [result]).flatMap((r) => r.output);
  const chunks = outputs.filter((o) => o.type === "chunk");
  const assets = outputs.filter((o) => o.type === "asset");
  if (chunks.length !== 1) throw new Error(`${entry}：应恰好输出一个 JS，实际 ${chunks.length} 个（不允许分块）`);
  const others = assets.filter((a) => !a.fileName.endsWith(".css"));
  if (others.length > 0 || assets.length > 1) {
    throw new Error(`${entry}：除至多一个 CSS 外不允许别的产物文件：${assets.map((a) => a.fileName).join("、")}`);
  }
  const text = (a) => (typeof a.source === "string" ? a.source : Buffer.from(a.source).toString("utf8")).trim();
  // 内联后 JS 与 CSS 里不能出现结束标签
  const js = chunks[0].code.trim().replaceAll("</script", "<\\/script");
  const css = assets.length ? text(assets[0]).replaceAll("</style", "<\\/style") : "";
  const html = await readFile(here(template), "utf8");
  const tag = new RegExp(`<script type="module" src="/${entry.replaceAll(".", "\\.")}"></script>`);
  if (!tag.test(html)) throw new Error(`${template}：找不到引用 /${entry} 的 <script type="module" src> 一行`);
  if (!html.includes("</head>")) throw new Error(`${template}：没有 </head>`);
  const out = html
    .replace("</head>", () => (css ? `<style>${css}</style>\n</head>` : "</head>"))
    .replace(tag, () => `<script type="module">${js}</script>`);
  // 产物里不得引用别的文件（ADR-0060 第 2 条）
  if (/<script[^>]*\ssrc=|<link[^>]*\s(rel="(modulepreload|stylesheet)")/.test(out)) {
    throw new Error(`${entry}：产物引用了别的文件`);
  }
  return out;
}

let failed = false;
for (const e of ENTRIES) {
  const out = here("../crates/server/src/ui/" + e.out);
  const name = "crates/server/src/ui/" + e.out;
  const html = await page(e);
  const size = Buffer.byteLength(html);
  if (size > SIZE_LIMIT) {
    console.error(`${name} ${size} 字节，超过门限 ${SIZE_LIMIT} 字节`);
    failed = true;
  } else if (process.argv.includes("--check")) {
    const committed = await readFile(out, "utf8").catch(() => null);
    if (committed !== html) {
      console.error(`${name} 与 ui/ 源码重新构建的结果不一致：在 ui/ 下执行 pnpm build 并提交产物`);
      failed = true;
    } else {
      console.log(`${name} 与源码一致（${size} 字节）`);
    }
  } else {
    await writeFile(out, html);
    console.log(`已写入 ${name}（${size} 字节）`);
  }
}
if (failed) process.exit(1);
