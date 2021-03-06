import anytest, { TestFn } from "ava";
import { DateTime } from "luxon";
import { MongoMemoryReplSet } from "mongodb-memory-server";
import { v4 } from "uuid";
import { MongoDriver } from "../src/driver/mongo.js";
import { Queue } from "../src/queue.js";
import { QueueDoc } from "../src/types.js";
import { Worker } from "../src/worker.js";

interface Context {
  mongo: MongoMemoryReplSet;
}

type StringJob = string;

const test = anytest as TestFn<Context>;

// create a clean mongo in-memory for every test
test.before(async (t) => {
  const rs = await MongoMemoryReplSet.create({
    replSet: { count: 1, name: v4(), storageEngine: "wiredTiger" },
  });
  t.context.mongo = rs;
});

// shut down replset after test
test.after(async (t) => {
  await t.context.mongo.stop();
});

test("Enqueueing an existing ref replaces it", async (t) => {
  const driver = new MongoDriver(t.context.mongo.getUri());
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
});

test("Creating a 'next' job fails quietly if a future job exists", async (t) => {
  const driver = new MongoDriver(t.context.mongo.getUri());
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
});

// job A - ack + destined for dead letter queue, visible now
// job B - added fresh via external interface
//
// expected: When job A is handled by the worker, it opts for
// the dead letter queue. createNext failing with a duplicate
// conflict does not abort the entire transaction
test("job A in ack + DLQ, job B added fresh", async (t) => {
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
  const driver = new MongoDriver(t.context.mongo.getUri());
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
});
