import anytest, { TestFn } from "ava";
import { v4 } from "uuid";
import { fromLoki, toLoki, LokiDriver } from "../../src/driver/loki.js";
import { suites } from "./driver.suite.js";
import { Context } from "./driver.types.js";

const test = anytest as TestFn<Context<LokiDriver>>;

test.before((t) => {
  t.context.createDriver = async (options) => {
    return Promise.resolve(
      new LokiDriver(v4(), {
        ...(options ?? {}),
      })
    );
  };
});

test.beforeEach(async (t) => {
  t.context.setDriver = async (d) => {
    t.context.insert = async (doc) => {
      const col = await d.getTable();
      col.insertOne(toLoki(doc));
    };
    t.context.dump = async () => {
      const col = await t.context.driver.getTable();
      return col.chain().find().data().map(fromLoki);
    };
    t.context.driver = d;
    await d.ready();
  };

  const driver = await t.context.createDriver();
  await t.context.setDriver(driver);
});

for (const s of suites<LokiDriver>()) {
  // example of skipping an optional driver feature
  // if (s.optionalFeatures?.listen) {
  //   test.skip(title)
  // }
  test(s.title, s.test);
}
