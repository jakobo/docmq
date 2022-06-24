import { MongoMemoryReplSet } from "mongodb-memory-server";
import { v4 } from "uuid";
import { Queue } from "../src/queue.js";
import Benchmark from "benchmark";
import { BulkEnqueueJob } from "../src/types.js";
import ora from "ora";

interface SimpleJob {
  success: boolean;
}

const bench = async () => {
  const rs = await MongoMemoryReplSet.create({
    replSet: { count: 1, name: v4(), storageEngine: "wiredTiger" },
  });

  console.log("Mongo @ " + rs.getUri());

  const queue = new Queue<SimpleJob>(rs.getUri(), v4());

  const enqueueTimer = ora("Running benchmark: enqueue()").start();
  const s1 = await new Promise<Benchmark>((resolve, reject) =>
    new Benchmark(
      "enqueue()",
      (deferred: Benchmark.Deferred) => {
        queue
          .enqueue({
            success: true,
          })
          .then(() => deferred.resolve())
          .catch(() => deferred.resolve());
      },
      {
        defer: true,
        async: true,
        onError(err: unknown) {
          reject(err);
        },
        onComplete() {
          // eslint-disable-next-line @typescript-eslint/no-this-alias
          const self: Benchmark = this;
          resolve(self);
        },
      }
    ).run()
  );
  enqueueTimer.succeed();

  const target = 200000;
  const chunk = 1000;
  const getPrepTimer = ora("Preparing benchmark: get()").start();
  let curr = 0;
  while (curr < target) {
    curr += chunk;
    getPrepTimer.text = `Preparing benchmark: get() (${curr}/${target})`;
    const block = new Array(chunk)
      .fill(0)
      .map<BulkEnqueueJob<SimpleJob>>(() => ({
        payload: {
          success: true,
        },
      }));
    await queue.enqueueMany(block);
  }
  getPrepTimer.succeed();

  const getTimer = ora("Running benchmark: get()").start();
  const s2 = await new Promise<Benchmark>((resolve, reject) => {
    const seen: Record<string, boolean> = {};
    queue.process(
      async (job, api) => {
        await api.ack();
      },
      { pause: true }
    );
    let started = false;

    new Benchmark(
      "get()",
      (deferred: Benchmark.Deferred) => {
        const rem = () => {
          queue.events.removeListener("ack", fn);
        };
        const fn = (ref: string) => {
          if (seen[ref]) {
            // ignore concurrent seens, there will be 1 success per ack
            return;
          }
          deferred.resolve();
          seen[ref] = true;
          process.nextTick(() => {
            // clean up seen, keep mem pressure down
            delete seen[ref];
          });
          rem();
        };
        queue.events.addListener("ack", fn);
        if (!started) {
          started = true;
          queue.start();
        }
      },
      {
        defer: true,
        async: true,
        onError(err: unknown) {
          reject(err);
        },
        onComplete() {
          // eslint-disable-next-line @typescript-eslint/no-this-alias
          const self: Benchmark = this;
          resolve(self);
        },
      }
    ).run();
  });
  getTimer.succeed();

  return {
    enqueue: s1,
    get: s2,
  };
};

bench()
  .then((suites) => {
    console.log("Completed All Benchmarks");
    console.log(
      [
        "Summary",
        "--------------------",
        `enqueue() ${Math.floor(1 / suites.enqueue.stats.mean)} ops/s`,
        `process() ${Math.floor(1 / suites.get.stats.mean)} ops/s`,
      ].join("\n")
    );
    process.exit(0);
  })
  .catch((err) => {
    console.error(err);
    process.exit(1);
  });
