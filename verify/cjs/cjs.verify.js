/* eslint-disable no-undef */
/* eslint-disable @typescript-eslint/no-var-requires */

const Queue = require("docmq");
const LokiDriver = require("docmq/driver/loki").LokiDriver;

if (typeof Queue === "undefined" || typeof LokiDriver === "undefined") {
  throw new Error("CommonJS not requiring cleanly");
}

const ld = new LokiDriver("cjs");

const run = async () => {
  await ld.ready();
  /** @type {import('docmq').QueueDoc} */
  const doc = {
    ref: "a",
    visible: new Date(),
    ack: "b",
    payload: "c",
    attempts: {
      tries: 2,
      max: 2,
      retryStrategy: {
        type: "fixed",
        amount: 1,
      },
    },
    repeat: {
      count: 0,
    },
  };

  // exercises the serialize-error pathway, which is an ESM only module
  try {
    await ld.dead(doc);
  } catch (e) {
    if (e instanceof Queue.DriverNoMatchingAckError) {
      // ok, this is a normal error
      return true;
    }
    throw e;
  }
};

run()
  .then(() => {
    console.info("CommonJS require successful");
  })
  .catch((e) => {
    console.error("The async imports of ESM only modules have gone awry");
    console.error(e);
    process.exit(1);
  });
