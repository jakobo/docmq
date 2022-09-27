import anytest, { TestFn } from "ava";
import { DateTime } from "luxon";
import { Collection } from "mongodb";
import { v4 } from "uuid";
import { MongoDriver, Queue, QueueDoc } from "../../src/index.js";
import { suites } from "./driver.suite.js";
import { Context } from "./driver.types.js";
import { Worker } from "../../src/worker.js";

// test suite only runs if a mongo uri is set
const ENABLED = process.env.MONGO_URI ? true : false;

// localize test fn w/ type
const test = anytest as TestFn<Context>;

// interface for additional mongo jobs
type StringJob = string;

/**
 * Set up the Mongo Driver and connect, making it available for all tests
 * before() manages the connection, while beforeEach() creates a unique driver
 * and sets up any additional testing infrastructure such as t.context.insert()
 * and t.context.dump() for analysis
 *
 * The before() should set up createDriver() and end()
 */
(ENABLED ? test.before : test.before.skip)((t) => {
  t.context.createDriver = async () => {
    return Promise.resolve(
      new MongoDriver(process.env.MONGO_URI, {
        schema: "test",
        table: v4(),
      })
    );
  };

  t.context.end = async () => {
    return Promise.resolve();
  };
});

/** Before every test, set up the driver instance and insert/dump methods */
(ENABLED ? test.beforeEach : test.beforeEach.skip)(async (t) => {
  t.context.driver = await t.context.createDriver();

  t.context.insert = async (doc) => {
    if (t.context.driver instanceof MongoDriver) {
      const col = await t.context.driver.getTable();
      await col.insertOne(doc);
    } else {
      throw new TypeError("Incorrect driver in context");
    }
  };

  t.context.dump = async () => {
    if (t.context.driver instanceof MongoDriver) {
      const col = await t.context.driver.getTable();
      return await col.find().toArray();
    } else {
      throw new TypeError("Incorrect driver in context");
    }
  };

  await t.context.driver.ready();
});

/** After all tests run, perform this cleanup */
(ENABLED ? test.after : test.after.skip)(async (t) => {
  await t.context.end();
});

/** BEGIN TEST SUITE */
for (const s of suites) {
  (ENABLED ? test : test.skip)(s.title, s.test);
}
/** END TEST SUITE */

/*
 * Test multiple job variants against an index that could be null/undefined
 * expected to fail for B, C, D on duplicate key
 * ensures that any unique indexes are non-sparse
 * ref: https://www.mongodb.com/docs/manual/core/index-sparse/#sparse-and-unique-properties
 * "An index that is both sparse and unique prevents collection from having
 * documents with duplicate values for a field but allows multiple documents
 * that omit the key."
 */
(ENABLED ? test : test.skip)(
  "mongo - deleted:undefined is indexed",
  async (t) => {
    const ref = v4();

    const jobA: QueueDoc = {
      ref,
      ack: null,
      // deleted: omitted field, meaning sparse would allow multiple ommitted
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
      attempts: {
        tries: 4,
        max: 3,
        retryStrategy: {
          type: "fixed",
          amount: 5,
        },
      },
      repeat: {
        count: 0,
        last: DateTime.now().minus({ days: 1 }).toJSDate(),
        every: {
          type: "duration",
          value: "P1D",
        },
      },
      payload: Queue.encodePayload("ack-job-dlq"),
    };

    const jobB: QueueDoc = {
      ref,
      ack: null,
      // deleted: omited. Should still be caught as a duplicate
      visible: DateTime.now().plus({ hours: 1 }).toJSDate(),
      attempts: {
        tries: 0,
        max: 3,
        retryStrategy: {
          type: "fixed",
          amount: 5,
        },
      },
      repeat: {
        count: 0,
        last: DateTime.now().plus({ hours: 1 }).toJSDate(),
        every: {
          type: "duration",
          value: "P1D",
        },
      },
      payload: Queue.encodePayload("new-value"),
    };

    const jobC: QueueDoc = {
      ref,
      ack: null,
      deleted: null, // Should still be caught as a duplicate
      visible: DateTime.now().plus({ hours: 1 }).toJSDate(),
      attempts: {
        tries: 0,
        max: 3,
        retryStrategy: {
          type: "fixed",
          amount: 5,
        },
      },
      repeat: {
        count: 0,
        last: DateTime.now().plus({ hours: 1 }).toJSDate(),
        every: {
          type: "duration",
          value: "P1D",
        },
      },
      payload: Queue.encodePayload("new-value"),
    };

    const jobD: QueueDoc = {
      ref,
      ack: null,
      deleted: undefined, // Should still be caught as a duplicate
      visible: DateTime.now().plus({ hours: 1 }).toJSDate(),
      attempts: {
        tries: 0,
        max: 3,
        retryStrategy: {
          type: "fixed",
          amount: 5,
        },
      },
      repeat: {
        count: 0,
        last: DateTime.now().plus({ hours: 1 }).toJSDate(),
        every: {
          type: "duration",
          value: "P1D",
        },
      },
      payload: Queue.encodePayload("new-value"),
    };

    const col = (await t.context.driver.getTable()) as Collection<QueueDoc>;

    await col.insertOne(jobA);
    await t.throwsAsync(col.insertOne(jobB));
    await t.throwsAsync(col.insertOne(jobC));
    await t.throwsAsync(col.insertOne(jobD));

    // after process, there should be only one job with ref in the future
    const docs = await col
      .find({
        ref,
      })
      .toArray();

    t.is(docs.length, 1);
  }
);

// Makes sure the oplog is used to minimize polling load
// all other times, we'll a 1ms poll to move through tests as fast as possible
(ENABLED ? test : test.skip)(
  "Leverages the oplog to minimize polling",
  async (t) => {
    const queue = new Queue<StringJob>(t.context.driver, v4());

    const p = new Promise<void>((resolve) => {
      queue.process(
        async (job, api) => {
          t.is(job, "oplog");
          await api.ack();
          t.pass();
          resolve();
        },
        {
          concurrency: 1,
          pollInterval: 40,
        }
      );
    });

    t.timeout(15000, "Max wait time exceeded");
    await queue.enqueue({
      payload: "oplog",
    });
    await p; // wait for finish
  }
);

(ENABLED ? test : test.skip)(
  "Enqueueing an existing ref replaces it",
  async (t) => {
    const driver = (await t.context.createDriver()) as MongoDriver;
    const queue = new Queue<StringJob>(driver, v4());
    const ref = v4();
    const col = await driver.getTable();

    // place an existing value into the collection
    // expiring some time in the future
    await col.insertOne({
      ref,
      visible: DateTime.now().plus({ seconds: 999 }).toJSDate(),
      ack: undefined,
      attempts: {
        tries: 0,
        max: 999,
        retryStrategy: {
          type: "fixed",
          amount: 5,
        },
      },
      repeat: {
        count: 0,
      },
      payload: Queue.encodePayload("old-value"),
    });

    // add job which should replace the existing job with its new value by ref
    await queue.enqueue({ ref, payload: "new-value" });

    // query and confirm
    const doc = await col.findOne({ ref });
    t.true(doc?.payload === Queue.encodePayload("new-value"));
  }
);

(ENABLED ? test : test.skip)(
  "Creating a 'next' job fails quietly if a future job exists",
  async (t) => {
    const driver = (await t.context.createDriver()) as MongoDriver;
    const queue = new Queue<StringJob>(driver, v4());
    const ref = v4();
    const col = await driver.getTable();

    // all indexes must be loaded for test
    await queue.ready();

    // place an existing value into the collection
    // expiring some time in the future
    await col.insertOne({
      ref,
      visible: DateTime.now().plus({ months: 3 }).toJSDate(),
      ack: undefined,
      attempts: {
        tries: 0,
        max: 999,
        retryStrategy: {
          type: "fixed",
          amount: 5,
        },
      },
      repeat: {
        count: 0,
      },
      payload: Queue.encodePayload("new-value"),
    });

    // perform a "createNext" operation which should fail silently
    await driver.createNext({
      ref,
      visible: DateTime.now().minus({ seconds: 100 }).toJSDate(),
      ack: v4(),
      attempts: {
        tries: 0,
        max: 999,
        retryStrategy: {
          type: "fixed",
          amount: 5,
        },
      },
      repeat: {
        count: 4,
        last: DateTime.now().toJSDate(),
        every: {
          type: "duration",
          value: "PT1M",
        },
      },
      payload: Queue.encodePayload("recur-value"),
    });

    // check result
    const docs = await col
      .find({
        ref,
        deleted: null,
        visible: {
          $gte: new Date(),
        },
      })
      .toArray();

    t.is(docs.length, 1);
    t.is(docs[0].payload, Queue.encodePayload("new-value"));
  }
);

// job A - ack + destined for dead letter queue, visible now
// job B - added fresh via external interface
//
// expected: When job A is handled by the worker, it opts for
// the dead letter queue. createNext failing with a duplicate
// conflict does not abort the entire transaction
(ENABLED ? test : test.skip)(
  "job A in ack + DLQ, job B added fresh",
  async (t) => {
    const ref = v4();

    const jobA: QueueDoc = {
      ref,
      ack: v4(),
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
      attempts: {
        tries: 4,
        max: 3,
        retryStrategy: {
          type: "fixed",
          amount: 5,
        },
      },
      repeat: {
        count: 0,
        last: DateTime.now().minus({ days: 1 }).toJSDate(),
        every: {
          type: "duration",
          value: "P1D",
        },
      },
      payload: Queue.encodePayload("ack-job-dlq"),
    };

    const jobB: QueueDoc = {
      ref,
      ack: undefined,
      visible: DateTime.now().plus({ hours: 1 }).toJSDate(),
      attempts: {
        tries: 0,
        max: 3,
        retryStrategy: {
          type: "fixed",
          amount: 5,
        },
      },
      repeat: {
        count: 0,
        last: DateTime.now().plus({ hours: 1 }).toJSDate(),
        every: {
          type: "duration",
          value: "P1D",
        },
      },
      payload: Queue.encodePayload("new-value"),
    };

    const name = v4();
    const driver = (await t.context.createDriver()) as MongoDriver;
    const queue = new Queue<StringJob>(driver, name);
    const col = await driver.getTable();

    await col.insertOne(jobA);
    await col.insertOne(jobB);

    const w = new Worker<StringJob>({
      doc: {
        ...jobA,
      },
      driver,
      name,
      payload: Queue.decodePayload(jobA.payload),
      handler: async (job, api) => {
        await api.ack();
      },
      emitter: queue.events,
      visibility: 30,
    });

    // make sure indexes are built, then test
    await queue.ready();
    await w.processOne();

    // after process, there should be only one job with ref in the future
    const docs = await col
      .find({
        ref,
        deleted: null,
        visible: {
          $gte: new Date(),
        },
      })
      .toArray();

    t.is(docs.length, 1);
    t.is(docs[0].payload, Queue.encodePayload("new-value"));
  }
);
