import anytest, { TestFn } from "ava";
import pg from "pg";
import { v4 } from "uuid";
import { PgDriver } from "../../src/driver/postrgres.js";
import { Queue } from "../../src/queue.js";

interface Context {
  pool: pg.Pool;
}

interface SimpleJob {
  success: boolean;
}

const test = anytest as TestFn<Context>;

// create a clean db pool for suite
test.before((t) => {
  const p = new pg.Pool({
    connectionString: process.env.AVA_POSTGRES_URL,
  });
  t.context.pool = p;
});

// shut down after test
test.after(async (t) => {
  await t.context.pool.end();
});

test("Connects", async (t) => {
  const client = await t.context.pool.connect();
  client.release();
  t.pass();
});

test("Creates a queue, adds an item, and sees the result in a processor", async (t) => {
  t.timeout(5000, "Max wait time exceeded");

  const queue = new Queue<SimpleJob>(
    new PgDriver(t.context.pool, { schema: "docmq", table: "jobs" }),
    v4()
  );

  const p = new Promise<void>((resolve) => {
    queue.process(
      async (job, api) => {
        t.true(job.success);
        await api.ack();
        t.pass();
        resolve();
      },
      {
        pollInterval: 0.1,
      }
    );
  });

  // add job
  await queue.enqueue({
    payload: {
      success: true,
    },
  });
  await p; // wait for finish
});
