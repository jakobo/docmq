import { DateTime } from "luxon";
import {
  MongoClient,
  MongoServerError,
  type ChangeStream,
  type ClientSession,
  type Collection,
  type Db,
  type ObjectId,
  type WithId,
} from "mongodb";
import { v4 } from "uuid";

import {
  DriverConnectionError,
  DriverError,
  DriverInitializationError,
  DriverNoMatchingAckError,
  DriverNoMatchingRefError,
  MaxAttemptsExceededError,
} from "../error.js";
import { QueueDoc } from "../types.js";
import { BaseDriver } from "./base.js";

/** An array of fields to drop in Mongo if we clone the QueueDoc object */
const DROP_ON_CLONE: Array<keyof WithId<QueueDoc>> = ["_id", "ack", "deleted"];

/** A local cache of clients we received */
const clients: Record<string, MongoClient> = {};

/**
 * Represents a generation of random mongo values
 * ref: https://www.mongodb.com/docs/manual/reference/operator/aggregation/rand/
 */
const RAND = {
  $toString: { $rand: {} },
};
/** RAND values, dash separated, generating enough entropy to avoid a collision */
const RAND_ID = [RAND, "-", RAND, "-", RAND];

/**
 * Recycles Mongo Clients for a given connection definition.
 * Important for serverless invocations, so that we maximimze reuse
 * ref: https://github.com/vercel/next.js/blob/canary/examples/with-mongodb/lib/mongodb.js
 */
export const getClient = (url: string) => {
  if (!clients[url]) {
    clients[url] = new MongoClient(url);
  }
  return clients[url];
};

/** Contains information needed to use an existing mongodb transaction */
interface MDBTxn {
  session: ClientSession;
}

/**
 * **Requires `mongodb` as a Peer Dependency to use**
 *
 * MongoDriver Class. Creates a connection that allows DocMQ to talk to
 * a MongoDB instance (or MongoDB compatible instance) via MongoClient
 */
export class MongoDriver extends BaseDriver<Db, Collection<QueueDoc>, MDBTxn> {
  protected _client: MongoClient | undefined;
  protected _db: Db | undefined;
  protected _jobs: Collection<QueueDoc> | undefined;
  protected _watch: ChangeStream | undefined;
  protected _watchFailing = false;

  /** Get the Mongo Collection associated with the job list */
  async getTable() {
    await this.ready();
    if (!this._jobs) {
      throw new DriverInitializationError();
    }
    return Promise.resolve(this._jobs);
  }

  /** Get the Mongo DB object associated with the job list */
  async getSchema() {
    await this.ready();
    if (!this._db) {
      throw new DriverInitializationError();
    }
    return Promise.resolve(this._db);
  }

  /** Attempt to reconnect to the MongoDB instance when a connection is lost */
  protected async reconnect(cause?: Error | unknown): Promise<boolean> {
    if (typeof this._client === "undefined") {
      return false;
    }
    if ((await this.ready()) === false) {
      return false;
    }

    if (cause) {
      const e = new DriverConnectionError("Reconnecting to MongoDB");
      e.original = cause instanceof Error ? cause : undefined;
      this.events.emit("warn", e);
    }

    const client = this._client;

    // p-retry is ESM only
    const { default: pRetry } = await import("p-retry");

    try {
      await pRetry(async () => {
        await client.db(this.getSchemaName()).command({ hello: 1 });
        this.events.emit("reconnect"); // successful reconnect
      });
      return true;
    } catch (err) {
      const e = new DriverConnectionError("Could not reconnect to MongoDB");
      e.original = err instanceof Error ? err : undefined;
      this.events.emit("halt", e);
    }

    return false;
  }

  /** Initializes the mongo connection */
  protected async initialize(
    connection: string | MongoClient
  ): Promise<boolean> {
    const client =
      typeof connection === "string" ? getClient(connection) : connection;
    await client.connect(); // no-op if already connected

    // increase max listeners to at least 100
    client.setMaxListeners(Math.max(client.getMaxListeners(), 100));

    // attach handlers to mongo client
    // Translate mongo errors to DocMQ Driver errors
    // also catches unhandled error callbacks https://nodejs.org/api/events.html#error-events
    // this occurs when mongodb has given up, meaning its our turn to retry
    client.on("error", (err) => {
      void this.reconnect(err);
    });

    // topology timeouts are treated as an error
    // https://github.com/mongodb/node-mongodb-native/blob/main/src/sdam/topology.ts#L586
    client.on("timeout", () => {
      const e = new DriverConnectionError("MongoDB Operation Timed Out");
      void this.reconnect(e);
    });

    // unable to check out a connection from the pool
    client.on("connectionCheckOutFailed", (err) => {
      void this.reconnect(err);
    });

    // check for oplog support
    const info = await client.db(this.getSchemaName()).command({ hello: 1 });
    const hasOplog =
      typeof info.setName !== "undefined" &&
      typeof info.setVersion !== "undefined";

    if (!hasOplog) {
      const e = new DriverError(
        "Mongo Driver support requires a Replica Set to be enabled in order to avoid excessive polling"
      );
      this.events.emit("halt", e);
      return false; // do not initialize
    }

    this._client = client;
    this._db = client.db(this.getSchemaName());
    this._jobs = this._db.collection(this.getTableName());

    const indexes: Promise<string>[] = [];

    // lookup by ref
    indexes.push(
      this._jobs.createIndex([["ref", 1]], {
        name: "ref_1",
        partialFilterExpression: { ref: { $type: "string" } },
        background: true, // Mongo < 4.2
      })
    );

    // lookup by ack
    indexes.push(
      this._jobs.createIndex([["ack", 1]], {
        name: "ack_1",
        unique: true,
        partialFilterExpression: { ack: { $type: "string" } },
        background: true, // Mongo < 4.2
      })
    );

    // next available jobs
    indexes.push(
      this._jobs.createIndex(
        [
          ["deleted", -1],
          ["ref", 1],
          ["visible", 1], // asc (future)
        ],
        {
          name: "deleted_-1_ref_1_visible_1",
          background: true, // Mongo < 4.2
        }
      )
    );

    // history, optionally by ref against a newest-first visible
    indexes.push(
      this._jobs.createIndex(
        [
          ["ref", 1],
          ["visible", -1], // desc
        ],
        {
          name: "ref_1_visible_-1",
          background: true, // Mongo < 4.2
        }
      )
    );

    // upsert by ref, uses ref & deleted for E of ESR
    indexes.push(
      this._jobs.createIndex(
        [
          ["ref", 1],
          ["deleted", -1],
          ["ack", 1],
          ["visible", 1],
        ],
        {
          name: "ref_1_deleted_-1_ack_1_visible_1",
          background: true, // Mongo < 4.2
        }
      )
    );

    // reservations index, used as part of take()
    indexes.push(
      this._jobs.createIndex(
        [
          ["reservationId", 1],
          ["visible", 1],
        ],
        {
          name: "reservationId_1_visible_1",
          background: true, // Mongo < 4.2
        }
      )
    );

    // a unique index that prevents multiple unacked jobs of the same ref
    // include null values in this index. It cannot be sparse
    // v2 - removed sparse constraint
    indexes.push(
      this._jobs.createIndex(
        [
          ["ref", 1],
          ["deleted", -1],
          ["ack", 1],
        ],
        {
          name: "ref_1_deleted_-1_ack_1_v2",
          unique: true,
          background: true, // Mongo < 4.2
        }
      )
    );

    // make it easy to pull dead items
    indexes.push(
      this._jobs.createIndex(
        [
          ["dead", 1],
          ["deleted", -1],
        ],
        {
          name: "dead_1_deleted_-1",
          partialFilterExpression: { dead: { $type: "boolean" } },
          background: true, // Mongo < 4.2
        }
      )
    );

    await Promise.allSettled(indexes);

    return true;
  }

  destroy(): void {
    if (this._watch) {
      this._watch.removeAllListeners();
    }
  }

  async transaction(body: (tx: MDBTxn) => Promise<unknown>): Promise<void> {
    await this.ready();

    if (!this._client) {
      throw new DriverInitializationError();
    }

    const session = this._client.startSession();

    await session.withTransaction(async () => {
      await body({
        session,
      });
    });
  }

  async take(visibility: number, limit = 10, tx?: MDBTxn): Promise<QueueDoc[]> {
    await this.ready();

    if (!this._jobs) {
      throw new DriverInitializationError();
    }

    const now = DateTime.now();
    const takeId = v4();

    // reserves jobs for the visibility window, and sets a reservationId
    // for retrieval as a two-stage operation
    // https://www.mongodb.com/community/forums/t/how-to-updatemany-documents-and-return-a-list-with-the-updated-documents/154282
    await this._jobs
      .aggregate(
        [
          {
            $match: {
              deleted: null,
              visible: {
                $lte: now.toJSDate(),
              },
            },
          },
          {
            $sort: {
              visible: 1,
              ref: 1,
            },
          },
          { $limit: limit },
          {
            $set: {
              ack: {
                // ack values in a mass-take are prefixed with the take id, followed
                // by a mongo call to generate 32 bytes of random numerical data
                $concat: [takeId, "-", ...RAND_ID],
              },
              visible: now
                .plus({ seconds: Math.max(1, visibility) })
                .toJSDate(),
              reservationId: takeId,
            },
          },
          {
            $merge: {
              into: this.getTableName(),
              on: "_id",
            },
          },
        ],
        { session: tx?.session }
      )
      .toArray();

    const results = await this._jobs
      .find(
        {
          reservationId: takeId,
          visible: {
            $gte: now.toJSDate(),
          },
        },
        { session: tx?.session }
      )
      .toArray();

    return results;
  }

  async ack(ack: string, tx?: MDBTxn): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new DriverInitializationError();
    }

    if (ack === null) {
      throw new Error("ERR_NULL_ACK");
    }

    const now = DateTime.now();

    const next = await this._jobs.findOneAndUpdate(
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
        session: tx?.session,
      }
    );

    if (!next.value) {
      const err = new DriverNoMatchingAckError(ack);
      if (this.isStrict()) {
        throw err;
      } else {
        this.events.emit("warn", err);
      }
    }
  }

  async fail(
    ack: string,
    retryIn: number,
    attempt: number,
    tx?: MDBTxn
  ): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new DriverInitializationError();
    }
    if (ack === null) {
      throw new Error("ERR_NULL_ACK");
    }
    const now = DateTime.now();

    const next = await this._jobs.findOneAndUpdate(
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
        session: tx?.session,
      }
    );

    if (!next.value) {
      const err = new DriverNoMatchingAckError(ack);
      if (this.isStrict()) {
        throw err;
      } else {
        this.events.emit("warn", err);
      }
    }
  }

  async dead(doc: QueueDoc, tx?: MDBTxn): Promise<void> {
    await this.ready();

    const ackVal = doc.ack;
    const now = DateTime.now();
    if (typeof ackVal === "undefined" || !ackVal) {
      throw new Error("Missing ack");
    }

    if (!this._jobs) {
      throw new DriverInitializationError();
    }

    const err = new MaxAttemptsExceededError(
      `Exceeded the maximum number of retries (${doc.attempts.max}) for this job`
    );

    // serialize-error is ESM only
    const { serializeError } = await import("serialize-error");

    const next = await this._jobs.updateOne(
      {
        ack: ackVal,
        visible: {
          $gt: now.toJSDate(),
        },
        deleted: null,
      },
      {
        $set: {
          dead: true,
          error: JSON.stringify(serializeError(err)),
          deleted: now.toJSDate(),
        },
      },
      {
        session: tx?.session,
      }
    );

    if (next.matchedCount < 1) {
      const err = new DriverNoMatchingAckError(ackVal);
      if (this.isStrict()) {
        throw err;
      } else {
        this.events.emit("warn", err);
      }
    }
  }

  async ping(ack: string, extendBy = 15, tx?: MDBTxn): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new DriverInitializationError();
    }
    if (ack === null) {
      throw new Error("ERR_NULL_ACK");
    }
    const now = DateTime.now();

    const next = await this._jobs.findOneAndUpdate(
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
        session: tx?.session,
      }
    );

    if (!next.value) {
      const err = new DriverNoMatchingAckError(ack);
      if (this.isStrict()) {
        throw err;
      } else {
        this.events.emit("warn", err);
      }
    }
  }

  async promote(ref: string, tx?: MDBTxn): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new DriverInitializationError();
    }

    const next = await this._jobs.findOneAndUpdate(
      {
        ref,
        visible: {
          $gte: new Date(),
        },
        deleted: null,
      },
      {
        $set: {
          visible: new Date(),
        },
      },
      {
        returnDocument: "after",
        session: tx?.session,
      }
    );

    if (!next.value) {
      const err = new DriverNoMatchingRefError(ref);
      if (this.isStrict()) {
        throw err;
      } else {
        this.events.emit("warn", err);
      }
    }
  }

  async delay(ref: string, delayBy: number, tx?: MDBTxn): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new DriverInitializationError();
    }

    const next = await this._jobs.findOneAndUpdate(
      {
        ref,
        visible: {
          $gte: new Date(),
        },
        deleted: null,
      },
      [
        {
          $set: {
            visible: {
              $add: ["$visible", 1000 * delayBy],
            },
          },
        },
      ],
      {
        returnDocument: "after",
        session: tx?.session,
      }
    );

    if (!next.value) {
      const err = new DriverNoMatchingRefError(ref);
      if (this.isStrict()) {
        throw err;
      } else {
        this.events.emit("warn", err);
      }
    }
  }

  async replay(ref: string, tx?: MDBTxn): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new DriverInitializationError();
    }
    await this._jobs
      .aggregate(
        [
          {
            $match: {
              ref,
              deleted: {
                $lte: new Date(),
              },
            },
          },
          {
            $sort: {
              deleted: -1,
            },
          },
          {
            $limit: 1,
          },
          { $addFields: { visible: new Date() } },
          {
            $unset: ["_id", "ack", "deleted", "repeat.every"],
          },
          {
            $merge: {
              into: this.getTableName(),
            },
          },
        ],
        {
          session: tx?.session,
        }
      )
      .toArray();
  }

  async clean(before: Date, tx?: MDBTxn): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new DriverInitializationError();
    }

    await this._jobs.deleteMany(
      {
        deleted: {
          $lte: before,
        },
      },
      { session: tx?.session }
    );
  }

  async replaceUpcoming(doc: QueueDoc, tx?: MDBTxn): Promise<QueueDoc> {
    await this.ready();

    if (!this._jobs) {
      throw new DriverInitializationError();
    }

    await this._jobs.replaceOne(
      {
        // uses ref, deleted, ack as key for upsert
        // https://www.mongodb.com/docs/manual/core/retryable-writes/#duplicate-key-errors-on-upsert
        ref: doc.ref,
        deleted: null,
        ack: null,
        visible: {
          $gte: new Date(),
        },
      },
      doc,
      { upsert: true, session: tx?.session }
    );

    return doc;
  }

  async removeUpcoming(ref: string, tx?: MDBTxn): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new DriverInitializationError();
    }
    if (!ref) {
      throw new Error("No ref provided");
    }

    await this._jobs.deleteMany(
      {
        ref: ref,
        deleted: null,
        ack: null,
        visible: {
          $gte: new Date(),
        },
      },
      { session: tx?.session }
    );
  }

  async createNext(doc: QueueDoc): Promise<void> {
    await this.ready();

    const nextRun = this.findNext(doc);
    if (!nextRun) {
      return;
    }
    if (!this._jobs) {
      throw new DriverInitializationError();
    }

    // create next document and insert
    const next: WithId<QueueDoc> = {
      ...doc,
      _id: undefined as unknown as ObjectId, // set on insert
      ack: null, // clear ack
      deleted: null, // clear deleted
      reservationId: undefined, // clear reservation id
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

    try {
      await this._jobs.insertOne(next, {
        // session: DISABLED: THROW INSTEAD OF INVALIDATING ACKS
      });
    } catch (e: unknown) {
      // throw non mongo server errors
      if (!(e instanceof MongoServerError)) {
        throw e;
      }

      // throw non 11000 errors
      if (e.code !== 11000) {
        throw e;
      }
    }
  }

  async listen() {
    await this.ready();

    if (!this._jobs) {
      throw new DriverInitializationError();
    }
    if (this._watch) {
      return Promise.resolve();
    }

    this._watch = this._jobs.watch([{ $match: { operationType: "insert" } }]);

    this._watch.on("change", (change) => {
      if (change.operationType !== "insert") {
        return;
      }
      this.events.emit("data");
    });

    // reconnect to the change stream and set the watch emitter up again
    const reconnectToChangeStream = async (original?: Error | unknown) => {
      // destroy self
      this._watch?.removeAllListeners();
      this._watch = undefined;
      try {
        const ok = await this.reconnect(original);
        if (ok) {
          void this.listen(); // set up listeners again
        }
      } catch (err) {
        if (err instanceof DriverConnectionError) {
          this.events.emit("error", err);
        } else {
          const e = new DriverConnectionError(
            "Could not reconnect to the MongoDB change stream"
          );
          e.original = err instanceof Error ? err : undefined;
          this.events.emit("halt", e);
        }
      }
    };

    // change stream broke (for example, network change or broken pipe, triggered by mongo)
    this._watch.on("error", (err) => {
      void reconnectToChangeStream(err);
    });

    // external close of stream object (triggered by mongodb)
    this._watch.on("close", () => {
      void reconnectToChangeStream();
    });
  }
}
