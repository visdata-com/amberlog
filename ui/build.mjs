// 检索界面的构建脚本（ADR-0055 第 2、3、5 条）：esbuild 把 src/main.tsx 打成一个 JS 与一个 CSS，内联进 src/index.html，
// 写到 crates/server/src/ui/index.html（由 amberlog-server 以 include_str! 编入）。
//   node build.mjs          构建并写入产物
//   node build.mjs --check  构建后与已提交的产物逐字节比对，不一致即失败（维护者检查改动时用它核对产物）
// 产物超过 SIZE_LIMIT 即失败；提高门限要在提交的改动说明里写理由。

import { build } from "esbuild";
import { readFile, writeFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const SIZE_LIMIT = 200 * 1024;
const here = (p) => fileURLToPath(new URL(p, import.meta.url));
const OUT = here("../crates/server/src/ui/index.html");

const result = await build({
  entryPoints: [here("src/main.tsx")],
  bundle: true,
  minify: true,
  format: "esm",
  target: "es2022",
  jsx: "automatic",
  jsxImportSource: "preact",
  legalComments: "none",
  charset: "utf8",
  outdir: "out",
  write: false,
  logLevel: "warning",
});
const pick = (ext) => {
  const files = result.outputFiles.filter((f) => f.path.endsWith(ext));
  if (files.length !== 1) throw new Error(`esbuild 应恰好输出一个 ${ext}，实际 ${files.length} 个`);
  return files[0].text.trim();
};
// 内联后 JS 与 CSS 里不能出现结束标签
const js = pick(".js").replaceAll("</script", "<\\/script");
const css = pick(".css").replaceAll("</style", "<\\/style");
const template = await readFile(here("src/index.html"), "utf8");
const page = template.replace("/*CSS*/", () => css).replace("/*JS*/", () => js);

const size = Buffer.byteLength(page);
if (size > SIZE_LIMIT) {
  console.error(`产物 ${size} 字节，超过门限 ${SIZE_LIMIT} 字节`);
  process.exit(1);
}
if (process.argv.includes("--check")) {
  const committed = await readFile(OUT, "utf8").catch(() => null);
  if (committed !== page) {
    console.error("crates/server/src/ui/index.html 与 ui/src 重新构建的结果不一致：在 ui/ 下执行 pnpm build 并提交产物");
    process.exit(1);
  }
  console.log(`产物与源码一致（${size} 字节）`);
} else {
  await writeFile(OUT, page);
  console.log(`已写入 crates/server/src/ui/index.html（${size} 字节）`);
}
