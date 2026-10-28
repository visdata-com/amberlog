// 检索界面前端依赖的许可与来源检查（ADR-0055 第 6 条；与 Rust 依赖的 deny.toml 同一套规则）。
//   node tools/ui-deps-check.mjs        ui/ 下须已 pnpm install --frozen-lockfile
// 检查三件事，任一不合即以非零退出：
//   1. 来源：ui/pnpm-lock.yaml 中每个包都取自 npm 注册表（只有完整性校验值，没有 git 仓库、tarball 地址或本地路径）；
//   2. 许可：已安装的全部包（运行时与构建期都算，构建期工具的输出进了产物）的许可都在 deny.toml 的 [licenses] allow 里；
//   3. 声明：运行时依赖（package.json 的 dependencies）在 third-party-notices-bundled.txt 里照录了与锁文件同版本的许可原文。

import { execFileSync } from "node:child_process";
import { existsSync, readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

const here = (p) => fileURLToPath(new URL(p, import.meta.url));
// 仓库根目录：从本脚本所在目录向上找含 ui/pnpm-lock.yaml 的目录，这样脚本放在哪一层都能用。
let root = here("./");
while (!existsSync(root + "ui/pnpm-lock.yaml")) {
  const up = fileURLToPath(new URL("../", "file://" + root));
  if (up === root) throw new Error("找不到含 ui/pnpm-lock.yaml 的仓库根目录");
  root = up;
}
const problems = [];

// 1. 来源
const lock = readFileSync(root + "ui/pnpm-lock.yaml", "utf8");
for (const line of lock.split("\n")) {
  if (/^\s+resolution:/.test(line) && !/^\s+resolution: \{integrity: sha512-[A-Za-z0-9+/=]+\}$/.test(line)) {
    problems.push(`来源不是 npm 注册表：${line.trim()}`);
  }
}

// 2. 许可：白名单取 deny.toml 的 [licenses] allow
const deny = readFileSync(here("../deny.toml"), "utf8");
const allowBlock = /\[licenses\][\s\S]*?\nallow = \[([\s\S]*?)\n\]/.exec(deny);
if (allowBlock === null) throw new Error("deny.toml 里找不到 [licenses] allow");
const allow = new Set([...allowBlock[1].matchAll(/^\s*"([^"]+)",/gm)].map((m) => m[1]));
const listed = JSON.parse(execFileSync("pnpm", ["licenses", "list", "--json"], { cwd: root + "ui", encoding: "utf8" }));
const permitted = (expr) => expr.replace(/[()]/g, "").split(/\s+OR\s+/).some((l) => allow.has(l.trim()));
let count = 0;
for (const [license, pkgs] of Object.entries(listed)) {
  for (const p of pkgs) {
    count++;
    if (!permitted(license)) problems.push(`许可不在白名单：${p.name}@${p.versions.join(",")}（${license}）`);
  }
}

// 3. 运行时依赖的许可声明
const pkg = JSON.parse(readFileSync(root + "ui/package.json", "utf8"));
const notices = readFileSync(here("third-party-notices-bundled.txt"), "utf8");
for (const name of Object.keys(pkg.dependencies ?? {})) {
  const m = new RegExp(`^  ${name.replace(/[.*+?^${}()|[\]\\/]/g, "\\$&")}@([^:\\s]+):`, "m").exec(lock);
  const version = m?.[1];
  if (version === undefined) problems.push(`锁文件里找不到运行时依赖 ${name}`);
  else if (!notices.includes(`${name} ${version} (`)) {
    problems.push(`third-party-notices-bundled.txt 没有照录 ${name} ${version} 的许可原文`);
  }
}

if (problems.length > 0) {
  for (const p of problems) console.error("ui-deps-check: " + p);
  process.exit(1);
}
console.log(`ui-deps-check: ${count} 个包的许可都在白名单，来源都是 npm 注册表，运行时依赖的许可声明齐全`);
