import { DateTime } from "luxon";
import { Queue } from "../src/queue.js";
import { type QueueDoc } from "../src/types.js";

export const genericDoc = (ref: string, payload: string): QueueDoc => ({
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
