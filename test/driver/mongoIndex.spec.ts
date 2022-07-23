import anytest, { TestFn } from "ava";
import { DateTime } from "luxon";
import { MongoMemoryReplSet } from "mongodb-memory-server";
import { v4 } from "uuid";
import { MongoDriver } from "../../src/driver/mongo.js";
import { Queue } from "../../src/queue.js";
import { QueueDoc } from "../../src/types.js";

interface Context {
  mongo: MongoMemoryReplSet;
}

const test = anytest as TestFn<Context>;

test.before(async (t) => {
  const rs = await MongoMemoryReplSet.create({
    replSet: { count: 1, name: v4(), storageEngine: "wiredTiger" },
  });
  t.context.mongo = rs;
});

test.after(async (t) => {
  await t.context.mongo.stop();
});

/*
 * Test multiple job variants against an index that could be null/undefined
 * expected to fail for B, C, D on duplicate key
 * ensures that any unique indexes are non-sparse
 * ref: https://www.mongodb.com/docs/manual/core/index-sparse/#sparse-and-unique-properties
 * "An index that is both sparse and unique prevents collection from having
 * documents with duplicate values for a field but allows multiple documents
 * that omit the key.""
 */
test("job A with deleted:undefined is properly indexed", async (t) => {
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

  const driver = new MongoDriver(t.context.mongo.getUri());
  const col = await driver.getTable();

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
