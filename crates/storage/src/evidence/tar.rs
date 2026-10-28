//! 证据包的 tar 读写（REQ-AMB-STO-EVIDENCE-EXPORT-TAR）：不压缩的 POSIX tar，每个成员以 ustar 头开始，成员名超出
//! ustar 头的长度限制时先写 pax 扩展头（typeflag `x`）记全名，不写 GNU 的长名扩展（typeflag `L`、`K`）；只写普通文件成员。
//!
//! 自写而不用常见的 `tar` crate：后者的 `Builder` 遇长名写 GNU 长名头，而本格式要求写 pax 扩展头。
//! 读取一侧（查验）收 ustar 与 pax 写法，也认 GNU 长名头，只取普通文件成员。

use std::io::{self, Write};

const BLOCK: usize = 512;

/// ustar 头里 `size` 字段（11 位八进制）能表示的上限；更大的成员另在 pax 扩展头里记 `size`。
const USTAR_MAX_SIZE: u64 = 0o777_7777_7777;

/// 依次写出成员的 tar 写者。写完全部成员后调用 [`TarWriter::finish`] 写出结尾的两个空块。
pub struct TarWriter<W: Write> {
    out: W,
}

impl<W: Write> TarWriter<W> {
    pub fn new(out: W) -> Self {
        TarWriter { out }
    }

    /// 写出一个普通文件成员 `name`（相对路径，调用方保证不以 `/` 开头、不含 `.` 与 `..` 段）。
    pub fn member(&mut self, name: &str, data: &[u8]) -> io::Result<()> {
        let size = data.len() as u64;
        let split = split_name(name);
        if split.is_none() || size > USTAR_MAX_SIZE {
            let mut records = Vec::new();
            if split.is_none() {
                records.extend(pax_record("path", name));
            }
            if size > USTAR_MAX_SIZE {
                records.extend(pax_record("size", &size.to_string()));
            }
            let pax_name = format!("PaxHeaders/{}", short_name(name));
            let header = header(short_name(&pax_name), "", records.len() as u64, b'x');
            self.out.write_all(&header)?;
            self.write_padded(&records)?;
        }
        let (prefix, short) = split.unwrap_or(("", short_name(name)));
        let header = header(short, prefix, size.min(USTAR_MAX_SIZE), b'0');
        self.out.write_all(&header)?;
        self.write_padded(data)
    }

    /// 把已写出的内容推给底层写者。
    pub fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }

    /// 写出结尾的两个空块，交回底层写者。
    pub fn finish(mut self) -> io::Result<W> {
        self.out.write_all(&[0u8; 2 * BLOCK])?;
        Ok(self.out)
    }

    fn write_padded(&mut self, data: &[u8]) -> io::Result<()> {
        self.out.write_all(data)?;
        let pad = (BLOCK - data.len() % BLOCK) % BLOCK;
        self.out.write_all(&vec![0u8; pad])
    }
}

/// 名字能放进 ustar 头时返回（prefix, name）：整名不超过 100 字节，或在某个 `/` 处拆成不超过 155 与 100 字节的两段。
fn split_name(name: &str) -> Option<(&str, &str)> {
    if name.len() <= 100 {
        return Some(("", name));
    }
    name.char_indices()
        .filter(|(_, c)| *c == '/')
        .map(|(i, _)| (&name[..i], &name[i + 1..]))
        .find(|(p, n)| p.len() <= 155 && !n.is_empty() && n.len() <= 100)
}

/// 放不下时 ustar 头里留的名字：取末尾不超过 100 字节、落在字符边界上的部分（读者按 pax 头的 `path` 取全名）。
fn short_name(name: &str) -> &str {
    let mut start = name.len().saturating_sub(100);
    while !name.is_char_boundary(start) {
        start += 1;
    }
    &name[start..]
}

/// 一条 pax 扩展记录 `"<长度> <键>=<值>\n"`，长度含它自己的位数。
fn pax_record(key: &str, value: &str) -> Vec<u8> {
    let body = key.len() + value.len() + 3; // 空格、`=`、换行
    let mut len = body + 1;
    while len != body + len.to_string().len() {
        len = body + len.to_string().len();
    }
    format!("{len} {key}={value}\n").into_bytes()
}

fn header(name: &str, prefix: &str, size: u64, typeflag: u8) -> [u8; BLOCK] {
    let mut h = [0u8; BLOCK];
    h[..name.len()].copy_from_slice(name.as_bytes());
    octal(&mut h[100..108], 0o644);
    octal(&mut h[108..116], 0);
    octal(&mut h[116..124], 0);
    octal(&mut h[124..136], size);
    octal(&mut h[136..148], 0);
    h[156] = typeflag;
    h[257..263].copy_from_slice(b"ustar\0");
    h[263..265].copy_from_slice(b"00");
    octal(&mut h[329..337], 0);
    octal(&mut h[337..345], 0);
    h[345..345 + prefix.len()].copy_from_slice(prefix.as_bytes());
    // 校验和：校验和字段按 8 个空格计，写作 6 位八进制、NUL、空格
    h[148..156].copy_from_slice(b"        ");
    let sum: u32 = h.iter().map(|&b| u32::from(b)).sum();
    let digits = format!("{sum:06o}\0 ");
    h[148..156].copy_from_slice(digits.as_bytes());
    h
}

/// 以 NUL 结尾、前补 0 的八进制数字填满 `field`。
fn octal(field: &mut [u8], value: u64) {
    let width = field.len() - 1;
    let s = format!("{value:0width$o}");
    field[..width].copy_from_slice(&s.as_bytes()[s.len() - width..]);
    field[width] = 0;
}

/// 读出的一个普通文件成员。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Member {
    pub name: String,
    pub data: Vec<u8>,
}

/// 文件不是 tar 文件（头的校验和或 magic 不对、成员被截断等）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NotTar(pub String);

/// 读出 `bytes` 中的全部普通文件成员（按出现的先后）。其他种类的成员（目录、链接等）跳过。
pub fn read(bytes: &[u8]) -> Result<Vec<Member>, NotTar> {
    let bad = |why: &str| NotTar(why.to_owned());
    let mut out = Vec::new();
    let mut pos = 0;
    let mut long_name: Option<String> = None;
    let mut long_size: Option<u64> = None;
    loop {
        let Some(h) = bytes.get(pos..pos + BLOCK) else {
            // 没有结尾空块：至少要读到过一个头才算 tar
            return if pos > 0 && pos == bytes.len() {
                Ok(out)
            } else {
                Err(bad("truncated header"))
            };
        };
        if h.iter().all(|&b| b == 0) {
            if pos == 0 {
                return Err(bad("empty archive"));
            }
            return Ok(out);
        }
        if &h[257..262] != b"ustar" {
            return Err(bad("not a ustar header"));
        }
        let want = parse_octal(&h[148..156]).ok_or_else(|| bad("bad checksum field"))?;
        let sum: u64 = h
            .iter()
            .enumerate()
            .map(|(i, &b)| u64::from(if (148..156).contains(&i) { b' ' } else { b }))
            .sum();
        if sum != want {
            return Err(bad("header checksum mismatch"));
        }
        let mut size = parse_octal(&h[124..136]).ok_or_else(|| bad("bad size field"))?;
        if let Some(s) = long_size.take() {
            size = s;
        }
        let start = pos + BLOCK;
        let len = usize::try_from(size).map_err(|_| bad("member too large"))?;
        let data = bytes
            .get(
                start
                    ..start
                        .checked_add(len)
                        .ok_or_else(|| bad("member too large"))?,
            )
            .ok_or_else(|| bad("truncated member"))?;
        pos = start + len.div_ceil(BLOCK) * BLOCK;
        match h[156] {
            b'x' => {
                for (k, v) in parse_pax(data).ok_or_else(|| bad("bad pax header"))? {
                    match k.as_str() {
                        "path" => long_name = Some(v),
                        "size" => long_size = Some(v.parse().map_err(|_| bad("bad pax size"))?),
                        _ => {}
                    }
                }
            }
            b'L' => {
                let n = data.split(|&b| b == 0).next().unwrap_or_default();
                long_name = Some(String::from_utf8(n.to_vec()).map_err(|_| bad("bad name"))?);
            }
            b'g' => {}
            b'0' | 0 => {
                let name = match long_name.take() {
                    Some(n) => n,
                    None => {
                        let name = cstr(&h[0..100]);
                        let prefix = cstr(&h[345..500]);
                        if prefix.is_empty() {
                            name
                        } else {
                            format!("{prefix}/{name}")
                        }
                    }
                };
                out.push(Member {
                    name,
                    data: data.to_vec(),
                });
            }
            _ => {
                long_name = None;
            }
        }
    }
}

fn cstr(field: &[u8]) -> String {
    let end = field.iter().position(|&b| b == 0).unwrap_or(field.len());
    String::from_utf8_lossy(&field[..end]).into_owned()
}

fn parse_octal(field: &[u8]) -> Option<u64> {
    let s = std::str::from_utf8(field).ok()?;
    let s = s.trim_matches(|c: char| c == '\0' || c == ' ');
    if s.is_empty() {
        return Some(0);
    }
    u64::from_str_radix(s, 8).ok()
}

fn parse_pax(mut data: &[u8]) -> Option<Vec<(String, String)>> {
    let mut out = Vec::new();
    while !data.is_empty() {
        let sp = data.iter().position(|&b| b == b' ')?;
        let len: usize = std::str::from_utf8(&data[..sp]).ok()?.parse().ok()?;
        let rec = data.get(..len)?;
        let body = rec.get(sp + 1..len - 1)?;
        let eq = body.iter().position(|&b| b == b'=')?;
        out.push((
            String::from_utf8(body[..eq].to_vec()).ok()?,
            String::from_utf8(body[eq + 1..].to_vec()).ok()?,
        ));
        data = &data[len..];
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn pack(members: &[(&str, &[u8])]) -> Vec<u8> {
        let mut w = TarWriter::new(Vec::new());
        for (n, d) in members {
            w.member(n, d).unwrap();
        }
        w.finish().unwrap()
    }

    #[test]
    fn round_trip_short_and_long_names() {
        let long = format!(
            "objects/default/app/manifest/buckets/{}.json",
            "x".repeat(200)
        );
        let mid = format!("{}/{}", "p".repeat(120), "n".repeat(90));
        let bytes = pack(&[("evidence.json", b"{}"), (&long, b"abc"), (&mid, b"")]);
        assert_eq!(bytes.len() % BLOCK, 0);
        let got = read(&bytes).unwrap();
        let names: Vec<_> = got.iter().map(|m| m.name.as_str()).collect();
        assert_eq!(names, ["evidence.json", long.as_str(), mid.as_str()]);
        assert_eq!(got[1].data, b"abc");
    }

    #[test]
    fn headers_are_ustar_without_gnu_long_names() {
        let long = "y".repeat(300);
        let bytes = pack(&[("a", b"1"), (&long, b"2")]);
        let mut pos = 0;
        let mut flags = Vec::new();
        while bytes[pos..pos + BLOCK].iter().any(|&b| b != 0) {
            let h = &bytes[pos..pos + BLOCK];
            assert_eq!(&h[257..263], b"ustar\0");
            assert_eq!(&h[263..265], b"00");
            flags.push(h[156]);
            let size = parse_octal(&h[124..136]).unwrap() as usize;
            pos += BLOCK + size.div_ceil(BLOCK) * BLOCK;
        }
        assert_eq!(flags, [b'0', b'x', b'0']);
    }

    #[test]
    fn pax_record_length_counts_itself() {
        for v in ["", "a", &"z".repeat(95), &"z".repeat(996)] {
            let r = pax_record("path", v);
            let (n, _) = std::str::from_utf8(&r).unwrap().split_once(' ').unwrap();
            assert_eq!(n.parse::<usize>().unwrap(), r.len());
        }
    }

    #[test]
    fn rejects_non_tar() {
        assert!(read(b"hello").is_err());
        assert!(read(&[0u8; 1024]).is_err());
        let mut bytes = pack(&[("a", b"1")]);
        bytes[0] = b'b';
        assert!(read(&bytes).is_err());
    }
}
