import test from "ava";
import { v4 } from "uuid";
import { LokiDriver } from "../src/driver/loki.js";
import { Queue } from "../src/queue.js";

type StringJob = string;

type Context = {
  altered: number;
};

test("context passing", async (t) => {
  const ld = new LokiDriver(v4());
  const q = new Queue<StringJob, undefined, Error, Context>(
    ld,
    "ctx test queue"
  );
  await q.enqueue({
    payload: "one",
  });
  await q.enqueue({
    payload: "two",
  });
  await q.enqueue({
    payload: "three",
  });

  t.plan(3);

  q.process(
    (job, api) => {
      if (api.context.altered > 0) {
        t.fail("context was altered and isn't unqiue to process()");
        return api.fail("Failed to keep a pure context");
      }

      // edit the context directly (by reference)
      // this would cause a future test to fail if context is not
      // fresh on every run
      api.context.altered += 1;

      // use to inspect the context if needed
      // t.log(job, api.context.altered);

      t.pass();

      return api.ack();
    },
    {
      createContext: () => {
        return {
          altered: 0,
        };
      },
    }
  );

  // wait for the queue to return to idle, indicating all jobs are done
  return new Promise((resolve) => {
    q.events.once("idle", () => {
      resolve();
    });
  });
});
