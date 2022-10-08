import anytest, { TestFn } from "ava";
import { v4 } from "uuid";
import { suites } from "./driver.suite.js";
import { Context } from "./driver.types.js";
import { fromPg, PgDriver, QUERIES } from "../../src/driver/postgres.js";
import pg from "pg";

const ENABLED = typeof process.env.POSTGRES_URL === "string";

const test = anytest as TestFn<Context<PgDriver>>;

(ENABLED ? test.before : test.before.skip)(async (t) => {
  const pool = new pg.Pool({
    connectionString: process.env.POSTGRES_URL,
    min: 1,
    max: 20,
  });

  await pool.query(`DROP SCHEMA IF EXISTS "test" CASCADE`);

  // for testing in mass-parallel, create the schema first to avoid
  // deadlocking the pg_namespace_nspname_index table
  const tmp = new PgDriver(pool, {
    schema: "test",
    table: v4(),
  });
  await tmp.ready();

  t.context.createDriver = async () => {
    const driver = new PgDriver(pool, {
      schema: "test",
      table: v4(),
    });
    driver.events.on("error", (e) => {
      // log driver errors for sanity
      t.log(e);
      if (e.original) {
        t.log(e.original);
      }
      t.fail();
    });
    return Promise.resolve(driver);
  };

  return Promise.resolve();
});

test.beforeEach(async (t) => {
  t.context.driver = await t.context.createDriver();

  await t.context.driver.ready();

  t.context.insert = async (doc) => {
    if (t.context.driver instanceof PgDriver) {
      const p = t.context.driver.getPool();
      const tn = t.context.driver.getQueryObjects();
      await p.query(
        QUERIES.insertOne.query(tn),
        QUERIES.insertOne.variables(doc)
      );
    } else {
      throw new TypeError("Incorrect driver in context");
    }
  };

  t.context.dump = async () => {
    if (t.context.driver instanceof PgDriver) {
      const p = t.context.driver.getPool();
      const tn = t.context.driver.getQueryObjects();

      const res = await p.query(`
        SELECT * FROM ${tn.table}
      `);

      // console.log(res.rows);

      return res.rows.map(fromPg);
    } else {
      throw new TypeError("Incorrect driver in context");
    }
  };
});

for (const s of suites) {
  (ENABLED ? test : test.skip)(s.title, s.test);
}
