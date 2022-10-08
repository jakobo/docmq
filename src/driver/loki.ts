import { DateTime } from "luxon";
import Loki, { type Collection } from "lokijs";
import { v4 } from "uuid";
import { DriverError, MaxAttemptsExceededError } from "../error.js";
import { QueueDoc } from "../types.js";
import { BaseDriver } from "./base.js";
import { loadModule } from "@brillout/load-module";

// not exported from @types/lokijs, so replicated here
// src: https://github.com/DefinitelyTyped/DefinitelyTyped/blob/master/types/lokijs/index.d.ts
interface LokiObj {
  $loki: number;
  meta: {
    created: number; // Date().getTime()
    revision: number;
    updated: number; // Date().getTime()
    version: number;
  };
}

/** A modified QueueDoc to support loki dates as strings */
type LokiRepeat = Omit<QueueDoc["repeat"], "last"> & {
  last?: string;
};

/** A modified QueueDoc to support loki dates as strings */
type LokiDoc = Omit<QueueDoc, "visible" | "deleted" | "repeat"> & {
  visible: string;
  deleted: string | null;
  repeat: LokiRepeat;
};

/** A helper that describes an object with Loki fields added */
type WithLoki<T> = T & LokiObj;

/** Convert a loki record to a queuedoc */
export const fromLoki = (d: LokiDoc): QueueDoc => {
  return {
    ...d,
    visible: DateTime.fromISO(d.visible).toJSDate(),
    deleted: d.deleted ? DateTime.fromISO(d.deleted).toJSDate() : null,
    repeat: {
      ...d.repeat,
      last:
        typeof d.repeat.last === "undefined"
          ? undefined
          : DateTime.fromISO(d.repeat.last).toJSDate(),
    },
  };
};

/** Convert a DocMQ record to Loki format */
export const toLoki = (d: QueueDoc): LokiDoc => {
  return {
    ...d,
    visible: DateTime.fromJSDate(d.visible).toISO(),
    deleted: d.deleted ? DateTime.fromJSDate(d.deleted).toISO() : null,
    repeat: {
      ...d.repeat,
      last:
        typeof d.repeat.last === "undefined"
          ? undefined
          : DateTime.fromJSDate(d.repeat.last).toISO(),
    },
  };
};

/** An array of fields to drop in Loki if we clone the QueueDoc object */
const DROP_ON_CLONE: Array<keyof WithLoki<LokiDoc>> = [
  "$loki",
  "meta",
  "ack",
  "deleted",
];

/** A local cache of clients we received */
const clients: Record<string, Loki> = {};

/** null or undefined in a Loki query ($exists is not sufficient) */
const nullish = {
  $or: [{ $exists: false }, { $eq: null }, { $eq: undefined }],
};

/**
 * Used to manage multiple loki DBs when necessary
 */
export const getClient = (identifier: string) => {
  if (!clients[identifier]) {
    clients[identifier] = new Loki(identifier, {
      adapter: new Loki.LokiMemoryAdapter({ asyncResponses: true }),
    });
  }
  return clients[identifier];
};

/**
 * LokiJS Driver Class. Creates a connection that allows DocMQ to talk to
 * an in-memory LokiJS instance
 */
export class LokiDriver extends BaseDriver {
  protected _db: Loki | undefined;
  protected _jobs: Collection<LokiDoc> | undefined;

  /** Get the Parent Schema object associated with the job list */
  async getSchema() {
    await this.ready();
    if (!this._db) {
      throw new Error("init");
    }
    return Promise.resolve(this._db);
  }

  /** Get the Loki Collection associated with the job list */
  async getTable() {
    await this.ready();
    if (!this._jobs) {
      throw new Error("init");
    }
    return Promise.resolve(this._jobs);
  }

  /** Initializes the Loki connection */
  protected async initialize(connection: string | Loki): Promise<boolean> {
    const client =
      typeof connection === "string" ? getClient(connection) : connection;

    client.on("error", (err) => {
      const e = new DriverError("LokiJS encountered an error");
      if (err instanceof Error) {
        e.original = err;
      }
      this.events.emit("error", e);
    });

    this._db = client;
    this._jobs = client.addCollection(this.getTableName(), {
      unique: [],
      asyncListeners: true,
      clone: true, // https://github.com/techfort/LokiJS/issues/379
      // TODO: Add indexes as performance dictates
    });

    return Promise.resolve(true);
  }

  destroy(): void {
    Object.entries(this._db?.events ?? []).forEach(([key, listeners]) => {
      listeners.forEach((ls) => {
        if (typeof this._db === "undefined") {
          return;
        }
        this._db.removeListener(key, ls);
      });
    });
  }

  async transaction(body: () => Promise<unknown>): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }

    this._jobs.startTransaction();
    await body();
    this._jobs.commit();
  }

  async take(visibility: number, limit = 10): Promise<QueueDoc[]> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }

    const now = DateTime.now();

    // read the next N jobs into an ack state and return them
    const jobs = this._jobs
      .chain()
      .find({
        deleted: nullish,
      })
      .where((doc) => {
        return DateTime.fromISO(doc.visible) <= DateTime.now();
      })
      .sort(
        (a, b) =>
          DateTime.fromISO(a.visible).toJSDate().getTime() -
          DateTime.fromISO(b.visible).toJSDate().getTime()
      )
      .limit(limit)
      .update((doc) => {
        doc.ack = v4();
        doc.visible = now.plus({ seconds: visibility }).toISO();
        return doc;
      })
      .data();

    return jobs.map(fromLoki);
  }

  async ack(ack: string): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }

    if (ack === null) {
      throw new Error("ERR_NULL_ACK");
    }

    const now = DateTime.now();

    const next = this._jobs
      .chain()
      .find({
        ack: {
          $eq: ack,
        },
      })
      .where((doc) => {
        return DateTime.fromISO(doc.visible) > now;
      })
      .update((doc) => {
        doc.deleted = now.toISO();
        return doc;
      })
      .data()?.[0];

    if (!next) {
      throw new Error("NO_MATCHING_JOB");
    }
  }

  async fail(ack: string, retryIn: number, attempt: number): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }
    if (ack === null) {
      throw new Error("ERR_NULL_ACK");
    }
    const now = DateTime.now();

    const next = this._jobs
      .chain()
      .find({
        ack,
        deleted: nullish,
      })
      .where((doc) => {
        return DateTime.fromISO(doc.visible) > now;
      })
      .update((doc) => {
        doc.visible = now.plus({ seconds: retryIn }).toISO();
        doc.attempts.tries = attempt;
        doc.ack = null;
        return doc;
      })
      .data()?.[0];

    if (!next) {
      throw new Error("NO_MATCHING_JOB");
    }
  }

  async dead(doc: QueueDoc): Promise<void> {
    await this.ready();

    const ackVal = doc.ack;
    const now = DateTime.now();
    if (typeof ackVal === "undefined" || !ackVal) {
      throw new Error("Missing ack");
    }

    if (!this._jobs) {
      throw new Error("init");
    }

    const err = new MaxAttemptsExceededError(
      `Exceeded the maximum number of retries (${doc.attempts.max}) for this job`
    );

    // serialize-error is esm-only and must be await imported
    const { serializeError } = (await loadModule(
      "serialize-error"
    )) as typeof import("serialize-error");

    const next = this._jobs
      .chain()
      .find({
        ack: ackVal,
        deleted: nullish,
      })
      .where((doc) => {
        return DateTime.fromISO(doc.visible) > now;
      })
      .update((doc) => {
        doc.dead = true;
        doc.error = JSON.stringify(serializeError(err));
        doc.deleted = now.toISO();
        return doc;
      })
      .data()?.[0];

    if (!next) {
      throw new Error("NO_MATCHING_JOB");
    }
  }

  async ping(ack: string, extendBy = 15): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }
    if (ack === null) {
      throw new Error("ERR_NULL_ACK");
    }
    const now = DateTime.now();

    const next = this._jobs
      .chain()
      .find({
        ack,
        deleted: nullish,
      })
      .where((doc) => {
        return DateTime.fromISO(doc.visible) > now;
      })
      .update((doc) => {
        doc.visible = now.plus({ seconds: extendBy }).toISO();
        return doc;
      })
      .data()?.[0];

    if (!next) {
      throw new Error("NO_MATCHING_JOB");
    }
  }

  async promote(ref: string): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }

    const now = DateTime.now();

    const next = this._jobs
      .chain()
      .find({
        ref: ref,
        deleted: nullish,
      })
      .where((doc) => {
        return DateTime.fromISO(doc.visible) > now;
      })
      .update((doc) => {
        doc.visible = now.toISO();
        return doc;
      })
      .data()?.[0];

    if (!next) {
      throw new Error("NO_MATCHING_JOB");
    }
  }

  async delay(ref: string, delayBy: number): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }

    const next = this._jobs
      .chain()
      .find({
        ref,
        deleted: nullish,
      })
      .where((doc) => {
        return DateTime.fromISO(doc.visible) >= DateTime.now();
      })
      .update((doc) => {
        doc.visible = DateTime.fromISO(doc.visible)
          .plus({ seconds: delayBy })
          .toISO();
        return doc;
      })
      .data()?.[0];

    if (!next) {
      throw new Error("NO_MATCHING_JOB");
    }
  }

  async replay(ref: string): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }

    const last = this._jobs
      .chain()
      .find({
        ref,
      })
      .where((doc) => {
        return (
          doc.deleted === null ||
          DateTime.fromISO(doc.deleted) <= DateTime.now()
        );
      })
      .sort(
        (a, b) =>
          DateTime.fromISO(a.visible).toJSDate().getTime() -
          DateTime.fromISO(b.visible).toJSDate().getTime()
      )
      .limit(1)
      .data()?.[0];

    if (!last) {
      throw new Error("NO_MATCHING_JOB");
    }

    const next: LokiDoc = JSON.parse(JSON.stringify(last));
    DROP_ON_CLONE.forEach((k) => {
      delete (next as WithLoki<LokiDoc>)[k];
    });
    next.visible = DateTime.now().toISO();
    next.repeat.every = undefined;
    next.ack = undefined;
    next.deleted = null;

    this._jobs.insertOne(next);
  }

  async clean(before: Date): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }

    this._jobs
      .chain()
      .where((doc) => {
        return (
          doc.deleted !== null &&
          DateTime.fromISO(doc.deleted) <= DateTime.fromJSDate(before)
        );
      })
      .remove()
      .data();
  }

  async replaceUpcoming(doc: QueueDoc): Promise<QueueDoc> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }

    // remove all future existing
    await this.removeUpcoming(doc.ref);

    // insert new upcoming
    this._jobs.insertOne(toLoki(doc));

    return doc;
  }

  async removeUpcoming(ref: string): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }
    if (!ref) {
      throw new Error("No ref provided");
    }

    this._jobs
      .chain()
      .find({
        ref,
        deleted: nullish,
        ack: nullish,
      })
      .where((doc) => {
        return DateTime.fromISO(doc.visible) >= DateTime.now();
      })
      .remove()
      .data();
  }

  async createNext(doc: QueueDoc): Promise<void> {
    await this.ready();

    const nextRun = this.findNext(doc);
    if (!nextRun) {
      return;
    }
    if (!this._jobs) {
      throw new Error("init");
    }

    const next: QueueDoc = {
      ...doc,
      ack: null,
      deleted: null,
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

    DROP_ON_CLONE.forEach((key) => delete next[key as keyof typeof next]);

    // check for upcoming
    const upcoming = this._jobs
      .chain()
      .find({
        ref: doc.ref,
        deleted: nullish,
        ack: nullish,
      })
      .where((doc) => {
        return DateTime.fromISO(doc.visible) >= DateTime.now();
      })
      .data()?.[0];

    // this is synchronous
    if (!upcoming) {
      this._jobs.insertOne(toLoki(next));
    }
  }

  async listen() {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }

    this._jobs.addListener("insert", () => {
      this.events.emit("data");
    });

    this._jobs.addListener("update", () => {
      this.events.emit("data");
    });
  }
}
