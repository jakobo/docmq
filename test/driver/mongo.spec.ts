import anytest, { TestFn } from "ava";
import { MongoMemoryReplSet } from "mongodb-memory-server";
import { MongoDriver } from "../../src/driver/mongo.js";
import { v4 } from "uuid";
import { Queue } from "../../src/queue.js";

interface Context {
  mongo: MongoMemoryReplSet;
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

test("Allows an external transaction", async (t) => {
  const name = v4();
  const ref = v4();
  const driver = new MongoDriver(t.context.mongo.getUri());
  const queue = new Queue<string>(driver, name);

  // make sure errors in a transaction throw
  await t.throwsAsync(async () => {
    // clone and perform actions against the transaction isolated queue
    await queue.transaction(async (q) => {
      // enqueue should use the session object
      await q.enqueue({
        ref,
        payload: "",
      });

      // forced exception
      // this should roll back the q.enqueue() operation
      throw new Error("abort by throw");
    });
  });

  // check DB wasn't written to
  const col = await driver.getTable();
  const result = await col.findOne({ ref });
  t.not(result?.ref, ref);
});
