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

  t.context.createDriver = async (options) => {
    const driver = new PgDriver(pool, {
      schema: "test",
      table: v4(),
      ...(options ?? {}),
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
  t.context.setDriver = async (d) => {
    t.context.insert = async (doc) => {
      const p = d.getPool();
      const tn = d.getQueryObjects();
      await p.query(
        QUERIES.insertOne.query(tn),
        QUERIES.insertOne.variables(doc)
      );
    };

    t.context.dump = async () => {
      const p = d.getPool();
      const tn = d.getQueryObjects();

      const res = await p.query(`
        SELECT * FROM ${tn.table}
      `);

      return res.rows.map(fromPg);
    };

    t.context.driver = d;
    await d.ready();
  };

  const driver = await t.context.createDriver();
  await t.context.setDriver(driver);
});

for (const s of suites<PgDriver>()) {
  (ENABLED ? test : test.skip)(s.title, s.test);
}
