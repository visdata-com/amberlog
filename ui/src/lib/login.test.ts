import { test } from "node:test";
import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { authorizeUrl, base64url, codeChallenge, parseLoginConfig, randomString } from "./login.ts";
import { sha256 } from "./sha256.ts";

const hex = (b: Uint8Array): string => Buffer.from(b).toString("hex");

test("SHA-256 与 node:crypto 一致（含补位跨块的长度）", () => {
  for (const n of [0, 1, 3, 55, 56, 63, 64, 65, 119, 120, 1000]) {
    const data = new Uint8Array(n).map((_, i) => (i * 31 + 7) & 0xff);
    assert.equal(hex(sha256(data)), createHash("sha256").update(data).digest("hex"), String(n));
  }
});

test("RFC 7636 附录 B 的 code_challenge", async () => {
  assert.equal(
    await codeChallenge("dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"),
    "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM",
  );
});

test("base64url 无填充，随机量的长度", () => {
  assert.equal(base64url(new Uint8Array([0xfb, 0xff])), "-_8");
  assert.match(randomString(32), /^[A-Za-z0-9_-]{43}$/);
  assert.match(randomString(16), /^[A-Za-z0-9_-]{22}$/);
});

test("登录配置的形状", () => {
  const ok = { authorization_endpoint: "https://idp/a", client_id: "c", scope: "openid" };
  assert.deepEqual(parseLoginConfig(ok), ok);
  assert.equal(parseLoginConfig({ ...ok, scope: 1 }), null);
  assert.equal(parseLoginConfig({ authorization_endpoint: "x", client_id: "c" }), null);
  assert.equal(parseLoginConfig([ok]), null);
  assert.equal(parseLoginConfig(null), null);
  assert.equal(parseLoginConfig("x"), null);
});

test("授权地址保留原有查询参数", () => {
  const u = new URL(
    authorizeUrl(
      { authorization_endpoint: "https://idp.example/auth?tenant=t1", client_id: "amb", scope: "openid profile" },
      "http://127.0.0.1:8080",
      "S",
      "C",
    ),
  );
  assert.equal(u.origin + u.pathname, "https://idp.example/auth");
  assert.deepEqual(Object.fromEntries(u.searchParams), {
    tenant: "t1",
    response_type: "code",
    client_id: "amb",
    redirect_uri: "http://127.0.0.1:8080/ui/callback",
    scope: "openid profile",
    state: "S",
    code_challenge: "C",
    code_challenge_method: "S256",
  });
});
