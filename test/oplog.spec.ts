import anytest, { TestFn } from "ava";
import { MongoMemoryReplSet, MongoMemoryServer } from "mongodb-memory-server";
import { MongoDriver } from "../src/driver/mongo.js";
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

// Makes sure the oplog is used to minimize polling load
// all other times, we'll a 1ms poll to move through tests as fast as possible
test("Leverages the oplog to minimize polling", async (t) => {
  const queue = new Queue<SimpleJob>(
    new MongoDriver(t.context.mongo.getUri()),
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
        concurrency: 1,
        pollInterval: 40,
      }
    );
  });

  t.timeout(15000, "Max wait time exceeded");
  await queue.enqueue({
    payload: {
      success: true,
    },
  });
  await p; // wait for finish
});

test("Won't run without a replica set", async (t) => {
  const mms = await MongoMemoryServer.create({
    instance: {
      storageEngine: "wiredTiger",
    },
  });

  const queue = new Queue<SimpleJob>(new MongoDriver(mms.getUri()), v4());

  const p = new Promise<void>((resolve, reject) => {
    queue.process(
      async () => {
        await new Promise((x) => setTimeout(x, 1));
        t.fail(); // we should never reach this
        resolve();
      },
      {
        pollInterval: 0.1,
      }
    );
    queue.events.on("error", (err) => {
      reject(err);
    });
  });

  await t.throwsAsync(
    queue.enqueue({
      payload: {
        success: true,
      },
    })
  );

  await t.throwsAsync(p); // wait for finish
  t.pass();
});
