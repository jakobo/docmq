import anytest, { TestFn } from "ava";
import { v4 } from "uuid";
import { suites } from "./driver.suite.js";
import { Context } from "./driver.types.js";
import { PgDriver, SQL_COLUMNS, toDoc } from "../../src/driver/postrgres.js";
import pg from "pg";

const test = anytest as TestFn<Context>;
const enabled = typeof process.env.AVA_POSTGRES_URL === "string";

test.before(async (t) => {
  const pool =
    typeof process.env.AVA_POSTGRES_URL === "string"
      ? new pg.Pool({
          connectionString: process.env.AVA_POSTGRES_URL,
          min: 1,
          max: 20,
        })
      : null;

  t.context.createDriver = async () => {
    if (pool === null) {
      return Promise.reject("No driver available");
    }

    const driver = new PgDriver(pool, {
      schema: "test",
      table: v4(),
    });
    return Promise.resolve(driver);
  };

  t.context.end = async () => {
    if (pool !== null) {
      // await pool.query(`DROP SCHEMA IF EXISTS "test" CASCADE`);
    }
    return Promise.resolve();
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
        `
        INSERT INTO ${tn.table} ${SQL_COLUMNS}
        VALUES ($1::uuid, $2::timestamptz, null, null, false, null, $3::text, 0, $4::integer, $5::text, 0, null, $6::text)
      `,
        [
          doc.ref,
          doc.visible,
          doc.payload,
          doc.attempts.max,
          JSON.stringify(doc.attempts.retryStrategy),
          doc.repeat.every ? JSON.stringify(doc.repeat.every) : null,
        ]
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

      return res.rows.map(toDoc);
    } else {
      throw new TypeError("Incorrect driver in context");
    }
  };
});

test.after(async (t) => {
  await t.context.end();
});

for (const s of suites) {
  // example of skipping an optional driver feature
  if (s.optionalFeatures?.listen) {
    test.skip(s.title, s.test);
  } else if (!enabled) {
    test.skip(s.title, s.test);
  } else {
    test(s.title, s.test);
  }
}
