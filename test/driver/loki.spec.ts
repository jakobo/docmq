import anytest, { TestFn } from "ava";
import { v4 } from "uuid";
import { fromLoki, toLoki } from "../../src/driver/loki.js";
import { LokiDriver } from "../../src/index.js";
import { suites } from "./driver.suite.js";
import { Context } from "./driver.types.js";

const test = anytest as TestFn<Context<LokiDriver>>;

test.beforeEach(async (t) => {
  t.context.driver = new LokiDriver(v4());

  t.context.insert = async (doc) => {
    if (t.context.driver instanceof LokiDriver) {
      const col = await t.context.driver.getTable();
      col.insertOne(toLoki(doc));
    } else {
      throw new TypeError("Incorrect driver in context");
    }
  };

  t.context.dump = async () => {
    if (t.context.driver instanceof LokiDriver) {
      const col = await t.context.driver.getTable();
      return col.chain().find().data().map(fromLoki);
    } else {
      throw new TypeError("Incorrect driver in context");
    }
  };

  await t.context.driver.ready();
});

for (const s of suites) {
  // example of skipping an optional driver feature
  // if (s.optionalFeatures?.listen) {
  //   test.skip(title)
  // }
  test(s.title, s.test);
}
