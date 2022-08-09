import anytest, { TestFn } from "ava";
import { DateTime } from "luxon";
import { Collection } from "mongodb";
import { MongoMemoryReplSet } from "mongodb-memory-server";
import { v4 } from "uuid";
import { MongoDriver, Queue, QueueDoc } from "../../src/index.js";
import { suites } from "./driver.suite.js";
import { Context } from "./driver.types.js";
const test = anytest as TestFn<Context>;

test.before(async (t) => {
  const rs = await MongoMemoryReplSet.create({
    replSet: { count: 1, name: v4(), storageEngine: "wiredTiger" },
  });

  t.context.createDriver = async () => {
    return Promise.resolve(
      new MongoDriver(rs.getUri(), {
        schema: "test",
        table: v4(),
      })
    );
  };

  t.context.end = async () => {
    await rs.stop();
  };
});

test.beforeEach(async (t) => {
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

test.after(async (t) => {
  await t.context.end();
});

for (const s of suites) {
  const title = `mongo - ${s.title}`;
  // example of skipping an optional driver feature
  // if (s.optionalFeatures?.listen) {
  //   test.skip(title)
  // }
  test(title, s.test);
}

/*
 * Test multiple job variants against an index that could be null/undefined
 * expected to fail for B, C, D on duplicate key
 * ensures that any unique indexes are non-sparse
 * ref: https://www.mongodb.com/docs/manual/core/index-sparse/#sparse-and-unique-properties
 * "An index that is both sparse and unique prevents collection from having
 * documents with duplicate values for a field but allows multiple documents
 * that omit the key."
 */
test("mongo - deleted:undefined is indexed", async (t) => {
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
});
