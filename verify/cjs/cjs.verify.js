/* eslint-disable no-undef */
/* eslint-disable @typescript-eslint/no-var-requires */

const Queue = require("docmq");
const LokiDriver = require("docmq/driver/loki").LokiDriver;

if (typeof Queue === "undefined" || typeof LokiDriver === "undefined") {
  throw new Error("CommonJS not requiring");
}

// eslint-disable-next-line no-undef
console.info("CommonJS require successful");
