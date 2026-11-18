// 检索界面前端依赖的许可与来源检查（ADR-0055 第 6 条；与 Rust 依赖的 deny.toml 同一套规则）。
//   node tools/ui-deps-check.mjs        ui/ 下须已 pnpm install --frozen-lockfile
// 检查三件事，任一不合即以非零退出：
//   1. 来源：ui/pnpm-lock.yaml 中每个包都取自 npm 注册表（只有完整性校验值，没有 git 仓库、tarball 地址或本地路径）；
//   2. 许可：已安装的全部包（运行时与构建期都算，构建期工具的输出进了产物）的许可都在 deny.toml 的 [licenses] allow 里；
//      例外只给下面 BUILD_ONLY 按包名列出的构建期工具（ADR-0060 第 6 条）：它们不在生产依赖树里、不进产物；
//      在生产依赖树里即失败。白名单本身不为它们放宽（它也管 Rust 依赖）；
//   3. 声明：生产依赖树（包括间接依赖）在 third-party-notices-bundled.txt 里照录了与锁文件同版本的许可原文。

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
const runtime = JSON.parse(execFileSync("pnpm", ["licenses", "list", "--prod", "--json"], { cwd: root + "ui", encoding: "utf8" }));
// 构建期专用的许可例外（ADR-0060 第 6 条）。每项：包名（平台二进制包按名字族列出）、许可、理由。
// 构建工具升级引入新的同类包时逐个加，在合并请求里写理由。
const BUILD_ONLY = [
  {
    name: /^lightningcss(-(darwin|linux|win32|freebsd|android)-[a-z0-9-]+)?$/,
    license: "MPL-2.0",
    why: "Vite 内置的 CSS 转换器及其各平台二进制，只在构建时运行，输出的 CSS 不含它的代码",
  },
  {
    name: /^caniuse-lite$/,
    license: "CC-BY-4.0",
    why: "浏览器支持数据，只供构建时决定语法降级，不进产物",
  },
];
const buildOnly = (p, license) => BUILD_ONLY.find((e) => e.name.test(p.name) && e.license === license);
for (const pkgs of Object.values(runtime)) {
  for (const p of pkgs) {
    if (BUILD_ONLY.some((e) => e.name.test(p.name))) problems.push(`构建期例外的包在生产依赖树里：${p.name}@${p.versions.join(",")}`);
  }
}
const permitted = (expr) => expr.replace(/[()]/g, "").split(/\s+OR\s+/).some((l) => allow.has(l.trim()));
let count = 0;
for (const [license, pkgs] of Object.entries(listed)) {
  for (const p of pkgs) {
    count++;
    if (!permitted(license) && !buildOnly(p, license)) {
      problems.push(`许可不在白名单：${p.name}@${p.versions.join(",")}（${license}）`);
    }
  }
}

// 3. 运行时依赖的许可声明
const notices = readFileSync(here("third-party-notices-bundled.txt"), "utf8");
for (const pkgs of Object.values(runtime)) {
  for (const p of pkgs) {
    for (const version of p.versions) {
      if (!notices.includes(`${p.name} ${version} (`)) {
        problems.push(`third-party-notices-bundled.txt 没有照录 ${p.name} ${version} 的许可原文`);
      }
    }
  }
}

if (problems.length > 0) {
  for (const p of problems) console.error("ui-deps-check: " + p);
  process.exit(1);
}
console.log(`ui-deps-check: ${count} 个包的许可通过检查，来源都是 npm 注册表，运行时依赖的许可声明齐全`);
