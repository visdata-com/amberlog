//! 构建时注入提交号，供 `amberlog --version` 与 `starting`/`ready` 日志使用（REQ-AMB-CFG-STARTUP-V2）。
//! 取不到 git 信息时（如从源码包构建）用全零占位。

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

fn git(args: &[&str]) -> Option<String> {
    let out = Command::new("git").args(args).output().ok()?;
    if !out.status.success() {
        return None;
    }
    let s = String::from_utf8(out.stdout).ok()?.trim().to_owned();
    (!s.is_empty()).then_some(s)
}

fn main() {
    embed_ui();
    let commit = git(&["rev-parse", "--short=12", "HEAD"])
        .filter(|s| s.len() >= 7 && s.bytes().all(|b| b.is_ascii_hexdigit()))
        .unwrap_or_else(|| "0000000".to_owned());
    println!("cargo:rustc-env=BUILD_GIT_COMMIT={commit}");

    // HEAD 移动或所指分支前进时重跑；声明了 rerun-if-changed 后 cargo 不再因本包源码变化重跑本脚本。
    // 只声明存在的路径：cargo 把不存在的路径一律判为已变、每次构建都重跑本脚本并重编本包（CI 的检出里常没有
    // packed-refs，逐个测试单独调用 cargo test 时每次都要重编重链约 1 分钟）。
    // - packed-refs 不存在时不监视：它只由 pack-refs 生成，而 pack-refs 同时删去分支的单独 ref 文件，那一变化已在监视之内。
    // - 分支 ref 已打包、没有单独文件时，监视它最近的现存上级目录：下次提交会新建该文件，cargo 扫描目录时能察觉。
    println!("cargo:rerun-if-changed=build.rs");
    let watch = |p: &Path| println!("cargo:rerun-if-changed={}", p.display());
    if let Some(head) = git(&["rev-parse", "--git-path", "HEAD"]) {
        watch(Path::new(&head));
    }
    if let Some(branch) = git(&["symbolic-ref", "-q", "HEAD"])
        && let Some(r) = git(&["rev-parse", "--git-path", &branch])
        && let Some(existing) = Path::new(&r).ancestors().find(|p| p.exists())
    {
        watch(existing);
    }
    if let Some(packed) = git(&["rev-parse", "--git-path", "packed-refs"])
        && Path::new(&packed).exists()
    {
        watch(Path::new(&packed));
    }
}

// 前端的资源清单在编译时生成；运行时不访问前端源码或构建目录。
// 只嵌入 index.html 与 assets/ 下的已知资源类型，不暴露 manifest、源码或任意磁盘路径。
fn embed_ui() {
    let root = PathBuf::from(std::env::var_os("CARGO_MANIFEST_DIR").unwrap()).join("src/ui");
    let mut resources = Vec::new();
    ui_files(&root.join("assets"), &mut resources);
    resources.sort();
    assert!(
        !resources.is_empty(),
        "缺少前端页面资源，请先在 ui/ 运行 pnpm build"
    );
    let mut generated = String::from("const ASSETS: &[Asset] = &[\n");
    for file in resources {
        let relative = file.strip_prefix(&root).unwrap().to_str().unwrap();
        assert!(
            relative
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"/_-.".contains(&b)),
            "非法资源路径：{relative}"
        );
        let content_type = match file.extension().and_then(|s| s.to_str()) {
            Some("js") => "text/javascript; charset=utf-8",
            Some("css") => "text/css; charset=utf-8",
            Some("svg") => "image/svg+xml",
            Some("png") => "image/png",
            Some("woff2") => "font/woff2",
            _ => panic!("不支持的前端资源类型：{relative}"),
        };
        generated.push_str(&format!(
            "    Asset {{ path: {:?}, content_type: {:?}, bytes: include_bytes!({:?}) }},\n",
            format!("/ui/{relative}"),
            content_type,
            file.to_str().unwrap()
        ));
    }
    generated.push_str("];\n");
    let output = PathBuf::from(std::env::var_os("OUT_DIR").unwrap());
    fs::write(output.join("ui_assets.rs"), generated).unwrap();
    println!(
        "cargo:rerun-if-changed={}",
        root.join("index.html").display()
    );
}

fn ui_files(dir: &Path, files: &mut Vec<PathBuf>) {
    println!("cargo:rerun-if-changed={}", dir.display());
    for entry in fs::read_dir(dir).expect("缺少前端资源目录，请先在 ui/ 运行 pnpm build")
    {
        let entry = entry.unwrap();
        let kind = entry.file_type().unwrap();
        if kind.is_dir() {
            ui_files(&entry.path(), files);
        } else {
            assert!(kind.is_file(), "前端资源不允许符号链接");
            files.push(entry.path());
        }
    }
}
