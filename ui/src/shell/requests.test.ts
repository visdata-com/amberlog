import { strict as assert } from "node:assert";
import { test } from "node:test";
import { ResultRequest } from "./requests.ts";

type Done = { kind: "ok"; n: number } | { kind: "aborted" };

// 一个挂起的附属请求：记下它收到的 signal，由测试决定何时得到响应
function pending(): { run: (s: AbortSignal) => Promise<Done>; signal: () => AbortSignal; answer: (n: number) => void } {
  let sig: AbortSignal | undefined;
  let resolve: (d: Done) => void = () => {};
  return {
    run: (s) =>
      new Promise<Done>((r) => {
        sig = s;
        resolve = r;
        s.addEventListener("abort", () => r({ kind: "aborted" }));
      }),
    signal: () => sig as AbortSignal,
    answer: (n) => resolve({ kind: "ok", n }),
  };
}

test("同槽更晚的请求中止并取代先发的", async () => {
  const req = new ResultRequest();
  const a = pending();
  const b = pending();
  const pa = req.inSlot("v", a.run);
  const pb = req.inSlot("v", b.run);
  assert.equal(a.signal().aborted, true);
  b.answer(2);
  assert.equal(await pa, null);
  assert.deepEqual(await pb, { kind: "ok", n: 2 });
});

test("dropSlot 中止在途请求并丢弃其响应，不影响别的槽", async () => {
  const req = new ResultRequest();
  const a = pending();
  const b = pending();
  const pa = req.inSlot("v", a.run);
  const pb = req.inSlot("w", b.run);
  req.dropSlot("v");
  assert.equal(a.signal().aborted, true);
  assert.equal(b.signal().aborted, false);
  b.answer(1);
  assert.equal(await pa, null);
  assert.deepEqual(await pb, { kind: "ok", n: 1 });
  const c = pending();
  const pc = req.inSlot("v", c.run);
  assert.equal(c.signal().aborted, false);
  c.answer(3);
  assert.deepEqual(await pc, { kind: "ok", n: 3 });
});

test("迟到而未被中止的响应也丢弃", async () => {
  const req = new ResultRequest();
  let resolve: (d: Done) => void = () => {};
  const pa = req.inSlot("v", () => new Promise<Done>((r) => (resolve = r)));
  req.dropSlot("v");
  resolve({ kind: "ok", n: 1 });
  assert.equal(await pa, null);
});

test("取消中止全部槽的在途请求", async () => {
  const req = new ResultRequest();
  const a = pending();
  const b = pending();
  const pa = req.inSlot("v", a.run);
  const pb = req.inSlot("w", b.run);
  req.cancel();
  assert.equal(a.signal().aborted, true);
  assert.equal(b.signal().aborted, true);
  assert.equal(await pa, null);
  assert.equal(await pb, null);
});
