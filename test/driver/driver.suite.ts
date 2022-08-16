import { ExecutionContext } from "ava";
import { DateTime } from "luxon";
import { v4 } from "uuid";
import { Queue, QueueDoc } from "../../src/index.js";
import { Context } from "./driver.types.js";

type StringJob = string;

const genericJob = (ref: string, payload: string): QueueDoc => ({
  ref,
  visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
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
  payload: Queue.encodePayload(payload),
});

export const suites: {
  title: string;
  optionalFeatures?: {
    listen?: boolean;
  };
  test: (t: ExecutionContext<Context>) => void | Promise<void>;
}[] = [];

// basic end to end using the queue
suites.push({
  title: "end to end",
  test: async (t) => {
    t.timeout(5000, "Max wait time exceeded");
    const queue = new Queue<StringJob>(t.context.driver, v4());

    const p = new Promise<void>((resolve) => {
      queue.process(
        async (job, api) => {
          t.true(job === "ok");
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
      payload: "ok",
    });

    await p; // wait for finish
  },
});

suites.push({
  title: "replaceUpcoming - replaces future jobs",
  test: async (t) => {
    const ref = v4();
    await t.context.insert({
      ...genericJob(ref, "old-value"),
    });

    await t.context.driver.replaceUpcoming({
      ...genericJob(ref, "new-value"),
    });

    // check
    const results = await t.context.dump();
    t.is(results.length, 1, "has only one message");
    t.is(
      results[0].payload,
      Queue.encodePayload("new-value"),
      "updated to new value"
    );
  },
});

suites.push({
  title: "createNext - fails silently if new future job exists",
  test: async (t) => {
    const ref = v4();
    await t.context.insert({
      ...genericJob(ref, "new-value"),
    });

    await t.context.driver.createNext({
      ...genericJob(ref, "recur-value"),
      // change to "done" job
      visible: DateTime.now().minus({ seconds: 100 }).toJSDate(),
      ack: v4(),
      repeat: {
        count: 4,
        last: DateTime.now().toJSDate(),
        every: {
          type: "duration",
          value: "PT1M",
        },
      },
    });

    // check
    const results = await t.context.dump();
    t.is(results.length, 1, "has only one message");
    t.is(
      results[0].payload,
      Queue.encodePayload("new-value"),
      "did not switch back to recur-value"
    );
  },
});

suites.push({
  title: "take - marks jobs for processing",
  test: async (t) => {
    const refA = v4();
    const refB = v4();
    const refC = v4();
    const now = DateTime.now().toJSDate();
    await t.context.insert({
      ...genericJob(refA, "job-a"),
      visible: DateTime.now().minus({ seconds: 100 }).toJSDate(),
    });
    await t.context.insert({
      ...genericJob(refB, "job-b"),
      visible: DateTime.now().minus({ seconds: 200 }).toJSDate(),
    });
    await t.context.insert({
      ...genericJob(refC, "job-c"),
      visible: DateTime.now().minus({ seconds: 300 }).toJSDate(),
    });

    const docs = await t.context.driver.take(30, 2);
    const db = await t.context.dump();

    // check
    // 2 items in DB w/ visibility in future
    // db matches take result
    t.is(
      db.filter((doc) => doc.visible >= now && doc.ack).length,
      2,
      "two items in the future"
    );
    t.deepEqual(
      db
        .filter((doc) => doc.visible >= now && doc.ack)
        .map((doc) => doc.ref)
        .sort(),
      docs.map((doc) => doc.ref).sort()
    );
  },
});

suites.push({
  title: "ack - flags job as completed",
  test: async (t) => {
    const ref = v4();
    const ack = v4();
    await t.context.insert({
      ...genericJob(ref, "job-a"),
      visible: DateTime.now().plus({ seconds: 100 }).toJSDate(),
      ack,
    });

    await t.context.driver.ack(ack);

    const db = await t.context.dump();
    t.truthy(db[0].deleted, "was never acked");
  },
});

suites.push({
  title: "ack - cannot ack an expired message",
  test: async (t) => {
    const ref = v4();
    const ack = v4();
    await t.context.insert({
      ...genericJob(ref, "job-a"),
      visible: DateTime.now().minus({ seconds: 100 }).toJSDate(),
      ack,
    });

    await t.throwsAsync(() => t.context.driver.ack(ack));

    const db = await t.context.dump();
    t.falsy(db[0].deleted, "was acked when it shouldn't be"); // never acked
  },
});

suites.push({
  title: "fail - updates job to next attempt",
  test: async (t) => {
    const ref = v4();
    const ack = v4();
    await t.context.insert({
      ...genericJob(ref, "job-a"),
      visible: DateTime.now().plus({ seconds: 100 }).toJSDate(),
      ack,
    });

    await t.context.driver.fail(ack, 3000, 9);

    const db = await t.context.dump();
    t.is(db[0].attempts.tries, 9, "sets attempts.tries");
    t.true(
      db[0].visible > DateTime.now().plus({ seconds: 300 }).toJSDate(),
      "visible was updated"
    );
  },
});

suites.push({
  title: "fail - cannot fail an expired job",
  test: async (t) => {
    const ref = v4();
    const ack = v4();
    await t.context.insert({
      ...genericJob(ref, "job-a"),
      visible: DateTime.now().minus({ seconds: 100 }).toJSDate(),
      ack,
    });

    await t.throwsAsync(() => t.context.driver.fail(ack, 3000, 9));
  },
});

suites.push({
  title: "dead - moves job to dead letter queue",
  test: async (t) => {
    const ref = v4();
    const ack = v4();

    const doc: QueueDoc = {
      ...genericJob(ref, "job-a"),
      visible: DateTime.now().plus({ seconds: 100 }).toJSDate(),
      ack,
      attempts: {
        tries: 2,
        max: 1,
        retryStrategy: {
          type: "fixed",
          amount: 5,
        },
      },
    };

    await t.context.insert(doc);
    await t.context.driver.dead(doc);

    const db = await t.context.dump();
    t.true(db[0].dead, "marked as dead");
    t.truthy(db[0].deleted, "marked as processed");
    t.truthy(db[0].error, "has an error set");
  },
});

suites.push({
  title: "dead - cannot dead-letter an expired item",
  test: async (t) => {
    const ref = v4();
    const ack = v4();

    const doc: QueueDoc = {
      ...genericJob(ref, "job-a"),
      visible: DateTime.now().minus({ seconds: 100 }).toJSDate(),
      ack,
      attempts: {
        tries: 2,
        max: 1,
        retryStrategy: {
          type: "fixed",
          amount: 5,
        },
      },
    };

    await t.context.insert(doc);

    await t.throwsAsync(
      () => t.context.driver.dead(doc),
      undefined,
      "throws an error"
    );
  },
});

suites.push({
  title: "ping - pushes a job's visibility out",
  test: async (t) => {
    const ref = v4();
    const ack = v4();

    await t.context.insert({
      ...genericJob(ref, "job-a"),
      ack,
    });

    await t.context.driver.ping(ack, 60);

    const db = await t.context.dump();

    t.true(
      DateTime.fromJSDate(db[0].visible).diffNow().as("seconds") > 31,
      "extends time"
    );
  },
});

suites.push({
  title: "ping - cannot extend an expired value",
  test: async (t) => {
    const ref = v4();
    const ack = v4();

    await t.context.insert({
      ...genericJob(ref, "job-a"),
      visible: DateTime.now().minus({ seconds: 1 }).toJSDate(),
      ack,
    });

    await t.throwsAsync(
      () => t.context.driver.ping(ack, 60),
      undefined,
      "throws an error"
    );
  },
});

suites.push({
  title: "promote - move a future job's run to now",
  test: async (t) => {
    const ref = v4();

    await t.context.insert({
      ...genericJob(ref, "job-a"),
      visible: DateTime.now().plus({ days: 1 }).toJSDate(),
    });

    await t.context.driver.promote(ref);
    const db = await t.context.dump();

    t.true(
      DateTime.fromJSDate(db[0].visible).diffNow().as("seconds") < 1,
      "moved to immediate"
    );
  },
});

suites.push({
  title: "promote - cannot promote an expired job",
  test: async (t) => {
    const ref = v4();

    await t.context.insert({
      ...genericJob(ref, "job-a"),
      visible: DateTime.now().minus({ days: 1 }).toJSDate(),
    });

    await t.throwsAsync(
      () => t.context.driver.promote(ref),
      undefined,
      "throws an error"
    );
  },
});

suites.push({
  title: "delay - push a job out by a specific amount of time",
  test: async (t) => {
    const ref = v4();

    await t.context.insert({
      ...genericJob(ref, "job-a"),
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
    });

    await t.context.driver.delay(ref, 30);
    const db = await t.context.dump();

    t.true(
      DateTime.fromJSDate(db[0].visible).diffNow().as("seconds") > 30,
      "moved to immediate"
    );
  },
});

suites.push({
  title: "delay - cannot delay an expired job",
  test: async (t) => {
    const ref = v4();

    await t.context.insert({
      ...genericJob(ref, "job-a"),
      visible: DateTime.now().minus({ seconds: 30 }).toJSDate(),
    });

    await t.throwsAsync(() => t.context.driver.delay(ref, 30));
  },
});

suites.push({
  title: "replay - insert a new job, cloning the last completed",
  test: async (t) => {
    const ref = v4();
    const now = DateTime.now();

    await t.context.insert({
      ...genericJob(ref, "job-a"),
      visible: now.minus({ seconds: 30 }).toJSDate(),
      ack: v4(),
      deleted: now.minus({ seconds: 30 }).toJSDate(),
    });

    await t.context.driver.replay(ref);
    const db = await t.context.dump();

    t.is(db.length, 2, "has two records");
    t.is(db[0].ref, db[1].ref, "duplicated refs");
    t.is(db[0].payload, db[1].payload, "duplicated payloads");

    const active = db.filter((doc) => !doc.deleted);
    t.is(active.length, 1, "one active job");
    t.true(DateTime.fromJSDate(active[0].visible) >= now, "updated visibility");
    t.falsy(active[0].ack, "has no ack");
  },
});

suites.push({
  title: "clean - strip old jobs",
  test: async (t) => {
    await t.context.insert({
      ...genericJob(v4(), "job-a"),
      visible: DateTime.now().minus({ seconds: 30 }).toJSDate(),
      deleted: DateTime.now().minus({ seconds: 30 }).toJSDate(),
    });
    await t.context.insert({
      ...genericJob(v4(), "job-b"),
      visible: DateTime.now().minus({ seconds: 38 }).toJSDate(),
      deleted: DateTime.now().minus({ seconds: 38 }).toJSDate(),
    });
    await t.context.insert({
      ...genericJob(v4(), "job-c"),
      visible: DateTime.now().minus({ seconds: 10 }).toJSDate(),
      deleted: DateTime.now().minus({ seconds: 10 }).toJSDate(),
    });
    await t.context.insert({
      ...genericJob(v4(), "job-d"),
      visible: DateTime.now().minus({ seconds: 2 }).toJSDate(),
      deleted: DateTime.now().minus({ seconds: 2 }).toJSDate(),
    });
    await t.context.insert({
      ...genericJob(v4(), "job-e"),
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
    });

    await t.context.driver.clean(
      DateTime.now().minus({ seconds: 5 }).toJSDate()
    );
    const db = await t.context.dump();

    t.is(db.length, 2, "contains only items after cutoff");
  },
});

suites.push({
  title: "removeUpcoming - drops upcoming jobs from queue",
  test: async (t) => {
    const ref = v4();
    const refB = v4();

    await t.context.insert({
      ...genericJob(ref, "job-a"),
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
    });

    await t.context.insert({
      ...genericJob(refB, "job-b"),
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
    });

    await t.context.driver.removeUpcoming(ref);
    const db = await t.context.dump();

    t.is(db.length, 1, "contains 1 job");
    t.is(db[0].ref, refB, "leaves unmatched ref");
  },
});

suites.push({
  title: "transaction - manages and rolls back",
  test: async (t) => {
    const refValid = v4();
    const ackValid = v4();
    const refExpired = v4();
    const ackExpired = v4();

    await t.context.insert({
      ...genericJob(refValid, "job-a"),
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
      ack: ackValid,
    });

    await t.context.insert({
      ...genericJob(refExpired, "job-b"),
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
      ack: ackExpired,
    });

    await t.throwsAsync(
      () =>
        t.context.driver.transaction(async () => {
          await t.context.driver.ack(refValid);
          await t.context.driver.ack(refExpired);
        }),
      undefined,
      "throws an error within the transaction"
    );

    const db = await t.context.dump();
    t.is(db.filter((doc) => !doc.deleted).length, 2, "no docs acknowledged");
  },
});

suites.push({
  title: "listen (optional) - responds to new data inserts",
  optionalFeatures: {
    listen: true,
  },
  test: async (t) => {
    const promise = new Promise<void>((resolve) => {
      t.context.driver.events.on("data", () => {
        t.pass();
        resolve();
      });
    });

    await t.context.driver.listen();

    // driver listening is not gaurenteed to be immediate. Pause before inserting
    await new Promise((resolve) => setTimeout(resolve, 500));

    await t.context.insert({
      ...genericJob(v4(), "job-a"),
    });

    await promise;
  },
});
