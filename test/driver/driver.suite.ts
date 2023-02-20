import { ExecutionContext } from "ava";
import { DateTime } from "luxon";
import { v4 } from "uuid";
import { BaseDriver, Queue, QueueDoc } from "../../src/index.js";
import { genericDoc } from "../helpers.js";
import { Context } from "./driver.types.js";

type StringJob = string;

interface DriverSuite<T extends BaseDriver> {
  title: string;
  optionalFeatures?: {
    listen?: boolean;
  };
  test: (t: ExecutionContext<Context<T>>) => void | Promise<void>;
}

export const suites = <T extends BaseDriver = BaseDriver>() =>
  allSuites as unknown[] as DriverSuite<T>[];

const allSuites: DriverSuite<BaseDriver>[] = [];

// basic end to end using the queue
allSuites.push({
  title: "e2e - sucessful run",
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

allSuites.push({
  title: "replaceUpcoming - replaces future jobs",
  test: async (t) => {
    const ref = v4();
    await t.context.insert({
      ...genericDoc(ref, "old-value"),
    });

    await t.context.driver.replaceUpcoming({
      ...genericDoc(ref, "new-value"),
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

allSuites.push({
  title: "replaceUpcoming - does not replace job in progress, adds instead",
  test: async (t) => {
    const ref = v4();
    await t.context.insert({
      ...genericDoc(ref, "old-value"),
      ack: v4(),
    });

    await t.context.driver.replaceUpcoming({
      ...genericDoc(ref, "new-value"),
    });

    // check
    const results = await t.context.dump();
    t.is(results.length, 2, "has two messages in queue");
  },
});

allSuites.push({
  title: "createNext - fails silently if new future job exists",
  test: async (t) => {
    const ref = v4();
    await t.context.insert({
      ...genericDoc(ref, "new-value"),
    });

    await t.context.driver.createNext({
      ...genericDoc(ref, "recur-value"),
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

allSuites.push({
  title: "createNext - does not push over pending job",
  test: async (t) => {
    const ref = v4();
    await t.context.insert({
      ...genericDoc(ref, "existing-value"),
      // mark as in-progress with an ack
      ack: v4(),
    });

    // should fail silently
    await t.context.driver.createNext({
      ...genericDoc(ref, "new-value"),
      // change to "done" job
      visible: DateTime.now().minus({ seconds: 100 }).toJSDate(),
      deleted: DateTime.now().toJSDate(),
    });

    // check
    const results = await t.context.dump();
    t.is(results.length, 1, "has only one message");
    t.is(
      results[0].payload,
      Queue.encodePayload("existing-value"),
      "did not overwrite existing job in progress"
    );
  },
});

allSuites.push({
  title: "take - marks jobs for processing",
  test: async (t) => {
    const refA = v4();
    const refB = v4();
    const refC = v4();
    const now = DateTime.now().toJSDate();
    await t.context.insert({
      ...genericDoc(refA, `${t.title} // job-a`),
      visible: DateTime.now().minus({ seconds: 100 }).toJSDate(),
    });
    await t.context.insert({
      ...genericDoc(refB, `${t.title} // job-b`),
      visible: DateTime.now().minus({ seconds: 200 }).toJSDate(),
    });
    await t.context.insert({
      ...genericDoc(refC, `${t.title} // job-c`),
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

allSuites.push({
  title: "take - takes job in order",
  test: async (t) => {
    const refA = v4();
    const refB = v4();
    const now = DateTime.now();

    await t.context.insert({
      ...genericDoc(refA, `${t.title} // job-a`),
      visible: now.minus({ seconds: 200 }).toJSDate(),
    });
    await t.context.insert({
      ...genericDoc(refB, `${t.title} // job-b`),
      visible: now.minus({ seconds: 300 }).toJSDate(),
    });

    const docs = await t.context.driver.take(30, 1);

    t.is(docs?.[0].ref, refB, "took older job");
  },
});

allSuites.push({
  title: "ack - flags job as completed",
  test: async (t) => {
    const ref = v4();
    const ack = v4();
    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
      visible: DateTime.now().plus({ seconds: 100 }).toJSDate(),
      ack,
    });

    await t.context.driver.ack(ack);

    const db = await t.context.dump();
    t.truthy(db[0].deleted, "was never acked");
  },
});

allSuites.push({
  title: "ack - cannot ack an expired message",
  test: async (t) => {
    const ref = v4();
    const ack = v4();
    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
      visible: DateTime.now().minus({ seconds: 100 }).toJSDate(),
      ack,
    });

    await t.throwsAsync(() => t.context.driver.ack(ack));

    const db = await t.context.dump();
    t.falsy(db[0].deleted, "was acked when it shouldn't be"); // never acked
  },
});

allSuites.push({
  title: "fail - updates job to next attempt",
  test: async (t) => {
    const ref = v4();
    const ack = v4();
    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
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

allSuites.push({
  title: "fail - throws failing an expired job",
  test: async (t) => {
    const ref = v4();
    const ack = v4();
    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
      visible: DateTime.now().minus({ seconds: 100 }).toJSDate(),
      ack,
    });

    await t.throwsAsync(() => t.context.driver.fail(ack, 3000, 9));
  },
});

allSuites.push({
  title: "fail - warns when strict mode is disabled",
  test: async (t) => {
    const d = await t.context.createDriver({ strict: false });
    await t.context.setDriver(d);

    const ref = v4();
    const ack = v4();
    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
      visible: DateTime.now().minus({ seconds: 100 }).toJSDate(),
      ack,
    });

    const promise = new Promise<void>((resolve) => {
      t.context.driver.events.on("warn", () => {
        t.pass();
        resolve();
      });
    });

    await t.context.driver.fail(ack, 3000, 9);
    return promise;
  },
});

allSuites.push({
  title: "dead - moves job to dead letter queue",
  test: async (t) => {
    const ref = v4();
    const ack = v4();

    const doc: QueueDoc = {
      ...genericDoc(ref, `${t.title} // job-a`),
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

allSuites.push({
  title: "dead - throws when DLQ an expired item",
  test: async (t) => {
    const ref = v4();
    const ack = v4();

    const doc: QueueDoc = {
      ...genericDoc(ref, `${t.title} // job-a`),
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

allSuites.push({
  title: "dead - warns on DLQ expired with strict-false",
  test: async (t) => {
    const d = await t.context.createDriver({ strict: false });
    await t.context.setDriver(d);

    const ref = v4();
    const ack = v4();

    const doc: QueueDoc = {
      ...genericDoc(ref, `${t.title} // job-a`),
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

    const promise = new Promise<void>((resolve) => {
      t.context.driver.events.on("warn", () => {
        t.pass();
        resolve();
      });
    });

    await t.context.driver.dead(doc);
    return promise;
  },
});

allSuites.push({
  title: "ping - pushes a job's visibility out",
  test: async (t) => {
    const ref = v4();
    const ack = v4();

    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
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

allSuites.push({
  title: "ping - throws on expired value",
  test: async (t) => {
    const ref = v4();
    const ack = v4();

    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
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

allSuites.push({
  title: "ping - warns on expired value with strict=false",
  test: async (t) => {
    const d = await t.context.createDriver({ strict: false });
    await t.context.setDriver(d);

    const ref = v4();
    const ack = v4();

    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
      visible: DateTime.now().minus({ seconds: 1 }).toJSDate(),
      ack,
    });

    const promise = new Promise<void>((resolve) => {
      t.context.driver.events.on("warn", () => {
        t.pass();
        resolve();
      });
    });

    await t.context.driver.ping(ack, 60);
    return promise;
  },
});

allSuites.push({
  title: "promote - move a future job's run to now",
  test: async (t) => {
    const ref = v4();

    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
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

allSuites.push({
  title: "promote - throws when promoting an expired job",
  test: async (t) => {
    const ref = v4();

    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
      visible: DateTime.now().minus({ days: 1 }).toJSDate(),
    });

    await t.throwsAsync(
      () => t.context.driver.promote(ref),
      undefined,
      "throws an error"
    );
  },
});

allSuites.push({
  title: "promote - warns when promoting an expired job and strict=false",
  test: async (t) => {
    const d = await t.context.createDriver({ strict: false });
    await t.context.setDriver(d);

    const ref = v4();

    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
      visible: DateTime.now().minus({ days: 1 }).toJSDate(),
    });

    const promise = new Promise<void>((resolve) => {
      t.context.driver.events.on("warn", () => {
        t.pass();
        resolve();
      });
    });

    await t.context.driver.promote(ref);
    return promise;
  },
});

allSuites.push({
  title: "delay - push a job out by a specific amount of time",
  test: async (t) => {
    const ref = v4();

    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
    });

    await t.context.driver.delay(ref, 30);
    const db = await t.context.dump();
    const diffNow = DateTime.fromJSDate(db[0].visible).diffNow().as("seconds");

    t.true(diffNow > 29, `time pushed out by 30s (got ${diffNow})`);
  },
});

allSuites.push({
  title: "delay - throws if delay an expired job",
  test: async (t) => {
    const ref = v4();

    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
      visible: DateTime.now().minus({ seconds: 30 }).toJSDate(),
    });

    await t.throwsAsync(() => t.context.driver.delay(ref, 30));
  },
});

allSuites.push({
  title: "delay - warns if delay an expired job strict=false",
  test: async (t) => {
    const d = await t.context.createDriver({ strict: false });
    await t.context.setDriver(d);

    const ref = v4();

    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
      visible: DateTime.now().minus({ seconds: 30 }).toJSDate(),
    });

    const promise = new Promise<void>((resolve) => {
      t.context.driver.events.on("warn", () => {
        t.pass();
        resolve();
      });
    });

    await t.context.driver.delay(ref, 30);
    return promise;
  },
});

allSuites.push({
  title: "replay - insert a new job, cloning the last completed",
  test: async (t) => {
    const ref = v4();
    const now = DateTime.now();

    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
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

allSuites.push({
  title: "clean - strip old jobs",
  test: async (t) => {
    await t.context.insert({
      ...genericDoc(v4(), `${t.title} // job-a`),
      visible: DateTime.now().minus({ seconds: 30 }).toJSDate(),
      deleted: DateTime.now().minus({ seconds: 30 }).toJSDate(),
    });
    await t.context.insert({
      ...genericDoc(v4(), `${t.title} // job-b`),
      visible: DateTime.now().minus({ seconds: 38 }).toJSDate(),
      deleted: DateTime.now().minus({ seconds: 38 }).toJSDate(),
    });
    await t.context.insert({
      ...genericDoc(v4(), `${t.title} // job-c`),
      visible: DateTime.now().minus({ seconds: 10 }).toJSDate(),
      deleted: DateTime.now().minus({ seconds: 10 }).toJSDate(),
    });
    await t.context.insert({
      ...genericDoc(v4(), "job-d"),
      visible: DateTime.now().minus({ seconds: 2 }).toJSDate(),
      deleted: DateTime.now().minus({ seconds: 2 }).toJSDate(),
    });
    await t.context.insert({
      ...genericDoc(v4(), "job-e"),
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
    });

    await t.context.driver.clean(
      DateTime.now().minus({ seconds: 5 }).toJSDate()
    );
    const db = await t.context.dump();

    t.is(db.length, 2, "contains only items after cutoff");
  },
});

allSuites.push({
  title: "removeUpcoming - drops upcoming jobs from queue",
  test: async (t) => {
    const ref = v4();
    const refB = v4();

    await t.context.insert({
      ...genericDoc(ref, `${t.title} // job-a`),
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
    });

    await t.context.insert({
      ...genericDoc(refB, `${t.title} // job-b`),
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
    });

    await t.context.driver.removeUpcoming(ref);
    const db = await t.context.dump();

    t.is(db.length, 1, "contains 1 job");
    t.is(db[0].ref, refB, "leaves unmatched ref");
  },
});

allSuites.push({
  title: "transaction - manages and rolls back",
  test: async (t) => {
    const refValid = v4();
    const ackValid = v4();
    const refExpired = v4();
    const ackExpired = v4();

    await t.context.insert({
      ...genericDoc(refValid, `${t.title} // job-a`),
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
      ack: ackValid,
    });

    await t.context.insert({
      ...genericDoc(refExpired, `${t.title} // job-b`),
      visible: DateTime.now().plus({ seconds: 30 }).toJSDate(),
      ack: ackExpired,
    });

    await t.throwsAsync(
      () =>
        t.context.driver.transaction(async (tx) => {
          await t.context.driver.ack(refValid, tx);
          await t.context.driver.ack(refExpired, tx);
        }),
      undefined,
      "throws an error within the transaction"
    );

    const db = await t.context.dump();
    t.is(db.filter((doc) => !doc.deleted).length, 2, "no docs acknowledged");
  },
});

allSuites.push({
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
      ...genericDoc(v4(), `${t.title} // job-a`),
    });

    await promise;
  },
});

allSuites.push({
  title:
    "e2e - Creates a queue, adds an item, and sees the result in a processor",
  test: async (t) => {
    t.timeout(5000, "Max wait time exceeded");

    const queue = new Queue<StringJob>(t.context.driver, v4());

    const p = new Promise<void>((resolve) => {
      queue.process(
        async (job, api) => {
          t.is(job, "y");
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
      payload: "y",
    });
    await p; // wait for finish
  },
});

allSuites.push({
  title: "e2e - Jobs outside of the retention window are cleaned",
  test: async (t) => {
    const queue = new Queue<StringJob>(t.context.driver, v4());
    const ref = v4();

    // const col = await t.context.driver.getTable();
    await t.context.insert({
      ref,
      visible: DateTime.now().minus({ days: 3 }).toJSDate(),
      deleted: DateTime.now()
        .minus({ days: 3 })
        .plus({ seconds: 10 })
        .toJSDate(),
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
        count: 0,
      },
      payload: Queue.encodePayload("old-value"),
    });

    // start the processor
    queue.process(async (job, api) => {
      await api.ack();
    });

    t.timeout(5000);

    await new Promise((resolve) => setTimeout(resolve, 100));
    const docs = await t.context.dump();
    t.is(docs.length, 0, "all docs cleaned");
  },
});
