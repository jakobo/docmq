import { DateTime, Duration } from "luxon";
import { ObjectId, WithId, type ClientSession, type Collection } from "mongodb";
import { v4 } from "uuid";
import { type QueueDoc, RecurrenceEnum } from "../types.js";
import cron from "cron-parser";

const isFulfilled = <T>(
  input: PromiseSettledResult<T>
): input is PromiseFulfilledResult<T> => input.status === "fulfilled";

const isNotNull = <T>(input: T | null): input is T => {
  return input === null ? false : true;
};

const isDefined = <T>(input: T | undefined): input is T => {
  return typeof input !== "undefined";
};

const DROP_ON_CLONE: Array<keyof WithId<QueueDoc>> = ["_id", "ack", "deleted"];

export const takeOne = async (
  collection: Collection<QueueDoc>,
  visibleFor: number
) => {
  const now = DateTime.now();
  const ack = v4();

  const next = await collection.findOneAndUpdate(
    {
      deleted: null,
      visible: {
        $lte: now.toJSDate(),
      },
    },
    {
      $inc: { tries: 1 },
      $set: {
        ack: ack,
        visible: now.plus({ seconds: visibleFor }).toJSDate(),
      },
    },
    {
      sort: {
        _id: 1,
      },
      returnDocument: "after",
    }
  );

  return next.value;
};

/**
 * Take N pending jobs, set them up with an ack value, and return the resolved values
 * Internally, this calls take() multiple times up to the specified limit.
 * It's better to over-call than to call N times in serial for performance reasons
 */
export const take = async (
  collection: Collection<QueueDoc>,
  visibleFor: number,
  limit = 1
) => {
  const promises: ReturnType<typeof takeOne>[] = [];

  for (let i = 0; i < limit; i++) {
    promises.push(takeOne(collection, visibleFor));
  }

  const results = await Promise.allSettled(promises);

  return results
    .filter(isFulfilled)
    .map((result) => result.value)
    .filter(isNotNull)
    .filter(isDefined);
};

/**
 * Acknowledges a message in a collection by its ack value
 * @param collection
 * @param ack
 */
export const ack = async (
  collection: Collection<QueueDoc>,
  ack: string,
  session: ClientSession
) => {
  const now = DateTime.now();

  if (ack === null) {
    throw new Error("ERR_NULL_ACK");
  }

  const next = await collection.findOneAndUpdate(
    {
      ack,
      visible: {
        $gt: now.toJSDate(),
      },
      deleted: null,
    },
    {
      $set: {
        deleted: now.toJSDate(),
      },
    },
    {
      returnDocument: "after",
      session,
    }
  );

  if (!next.value) {
    throw new Error("ERR_UNKOWN_ACK");
  }
};

/** Fail a message by its ack value, updating its visibility to a specified retry */
export const fail = async (
  collection: Collection<QueueDoc>,
  ack: string,
  retryIn: number,
  attempt: number,
  session: ClientSession
) => {
  const now = DateTime.now();

  if (ack === null) {
    throw new Error("ERR_NULL_ACK");
  }

  const next = await collection.findOneAndUpdate(
    {
      ack,
      visible: {
        $gt: now.toJSDate(),
      },
      deleted: null,
    },
    {
      $set: {
        visible: now.plus({ seconds: retryIn }).toJSDate(),
        "attempts.tries": attempt,
      },
      $unset: {
        ack: true,
      },
    },
    {
      returnDocument: "after",
      session,
    }
  );

  if (!next.value) {
    throw new Error("ERR_UNKOWN_ACK");
  }
};

/**
 * Extend the visibility window of a job for long running processes
 * @param collection
 * @param ack
 * @param extendBy
 */
export const ping = async (
  collection: Collection<QueueDoc>,
  ack: string,
  extendBy = 15
) => {
  const now = DateTime.now();

  const next = await collection.findOneAndUpdate(
    {
      ack,
      visible: {
        $gt: now.toJSDate(),
      },
      deleted: null,
    },
    {
      $set: {
        visible: now.plus({ seconds: extendBy }).toJSDate(),
      },
    },
    {
      returnDocument: "after",
    }
  );

  if (!next.value) {
    throw new Error("ERR_UNKNOWN_ACK");
  }
};

/** Create and insert the next occurence of a job if repeat options are enabled */
export const createNext = async (
  collection: Collection<QueueDoc>,
  doc: QueueDoc,
  session: ClientSession
) => {
  // if no repeat options, eject
  if (!doc.repeat.every) {
    return;
  }

  // if cron, just take next from now
  let nextRun = new Date();
  if (doc.repeat.every.type === RecurrenceEnum.cron) {
    const c = cron.parseExpression(doc.repeat.every.value, {
      currentDate: new Date(),
    });
    nextRun = c.next().toDate();
  } else if (doc.repeat.every.type === RecurrenceEnum.duration) {
    const dur = Duration.fromISO(doc.repeat.every.value);
    const dt = DateTime.now().plus(dur);
    nextRun = dt.toJSDate();
  } else {
    // invalid
    return;
  }

  // create next document and insert
  const next: WithId<QueueDoc> = {
    ...doc,
    _id: undefined as unknown as ObjectId, // clear _id
    ack: undefined, // clear ack
    deleted: undefined, // clear deleted
    visible: nextRun,
    attempts: {
      tries: 0,
      max: doc.attempts.max,
      retryStrategy: doc.attempts.retryStrategy,
    },
    repeat: {
      ...doc.repeat,
      last: nextRun,
      count: doc.repeat.count + 1,
    },
  };

  DROP_ON_CLONE.forEach((key) => delete next[key]);
  await collection.insertOne(next, { session });
};
