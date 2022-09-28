import { Queue } from "docmq";
import { LokiDriver } from "docmq/driver/loki";

if (typeof Queue === "undefined" || typeof LokiDriver === "undefined") {
  throw new Error("ESM import not working as expected");
}

// eslint-disable-next-line no-undef
console.info("ESM import successful");
