import anytest, { TestFn } from "ava";
import { MongoClient } from "mongodb";
import { MongoMemoryReplSet } from "mongodb-memory-server";
import { v4 } from "uuid";
import { Queue } from "../src/queue.js";

interface Context {
  mongo: MongoMemoryReplSet;
}

interface SimpleJob {
  success: boolean;
}

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

test("Sanity - Can connect to local mongo", async (t) => {
  const client = new MongoClient(t.context.mongo.getUri());
  await client.connect();
  await client.db("sanity").collection("test").insertOne({
    valid: true,
  });
  t.pass();
});

test("Creates a queue, adds an item, and sees the result in a processor", async (t) => {
  t.timeout(5000, "Max wait time exceeded");

  const queue = new Queue<SimpleJob>(t.context.mongo.getUri(), v4());

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
    success: true,
  });
  await p; // wait for finish
});
