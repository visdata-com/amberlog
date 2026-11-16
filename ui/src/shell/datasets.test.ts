import assert from "node:assert/strict";
import test from "node:test";
import { datasetOf, streamOf } from "./datasets.ts";

test("数据集界面名称与对账查询名称互相转换", () => {
  assert.equal(streamOf("default/app"), "app");
  assert.equal(datasetOf("app"), "default/app");
  assert.equal(datasetOf("default/app"), "default/app");
});
