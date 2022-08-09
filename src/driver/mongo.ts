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
import { serializeError } from "serialize-error";
import { v4 } from "uuid";

import {
  MaxAttemptsExceededError,
  NonReplicatedMongoInstanceError,
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

/**
 * **Requires `mongodb` as a Peer Dependency to use**
 *
 * MongoDriver Class. Creates a connection that allows DocMQ to talk to
 * a MongoDB instance (or MongoDB compatible instance) via MongoClient
 */
export class MongoDriver extends BaseDriver {
  protected _client: MongoClient | undefined;
  protected _session: ClientSession | undefined;
  protected _db: Db | undefined;
  protected _jobs: Collection<QueueDoc> | undefined;
  protected _watch: ChangeStream | undefined;

  /** Get the Mongo Collection associated with the job list */
  async getTable() {
    await this.ready();
    if (!this._jobs) {
      throw new Error("init");
    }
    return Promise.resolve(this._jobs);
  }

  /** Get the Mongo DB object associated with the job list */
  async getSchema() {
    await this.ready();
    if (!this._db) {
      throw new Error("init");
    }
    return Promise.resolve(this._db);
  }

  /** Initializes the mongo connection */
  protected async initialize(
    connection: string | MongoClient
  ): Promise<boolean> {
    const client =
      typeof connection === "string" ? getClient(connection) : connection;
    await client.connect(); // no-op if already connected

    // check for oplog support
    const info = await client.db(this.table).command({ hello: 1 });
    const hasOplog =
      typeof info.setName !== "undefined" &&
      typeof info.setVersion !== "undefined";
    if (!hasOplog) {
      throw new NonReplicatedMongoInstanceError(
        "Mongo Driver support requires a Replica Set to be enabled"
      );
    }

    this._client = client;
    this._db = client.db(this.schema);
    this._jobs = this._db.collection(this.table);

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

  async transaction(body: () => Promise<unknown>): Promise<void> {
    await this.ready();

    if (!this._client) {
      throw new Error("init");
    }

    if (typeof this._session === "undefined") {
      this._session = this._client.startSession();
    }

    // if in a transaction, just run the transacting body
    // else wrap actio with transaction
    if (this._session.inTransaction()) {
      await body();
    } else {
      await this._session.withTransaction(async () => {
        await body();
      });
    }
  }

  async take(visibility: number, limit = 10): Promise<QueueDoc[]> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }

    const now = DateTime.now();
    const takeId = v4();

    // reserves jobs for the visibility window, and sets a reservationId
    // for retrieval as a two-stage operation
    // https://www.mongodb.com/community/forums/t/how-to-updatemany-documents-and-return-a-list-with-the-updated-documents/154282
    await this._jobs
      .aggregate([
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
            visible: now.plus({ seconds: visibility }).toJSDate(),
            reservationId: takeId,
          },
        },
        {
          $merge: this.getTableName(),
        },
      ])
      .toArray();

    const results = await this._jobs
      .find({
        reservationId: takeId,
        visible: {
          $gte: now.toJSDate(),
        },
      })
      .toArray();

    return results;
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
        session: this._session,
      }
    );

    if (!next.value) {
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
        session: this._session,
      }
    );

    if (!next.value) {
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
        session: this._session,
      }
    );

    if (next.matchedCount < 1) {
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
        session: this._session,
      }
    );

    if (!next.value) {
      throw new Error("NO_MATCHING_JOB");
    }
  }

  async promote(ref: string): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
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
        session: this._session,
      }
    );

    if (!next.value) {
      throw new Error("NO_MATCHING_JOB");
    }
  }

  async delay(ref: string, delayBy: number): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
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
        session: this._session,
      }
    );

    if (!next.value) {
      throw new Error("NO_MATCHING_JOB");
    }
  }

  async replay(ref: string): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
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
            $merge: this.getTableName(),
          },
        ],
        {
          session: this._session,
        }
      )
      .toArray();
  }

  async clean(before: Date): Promise<void> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
    }

    await this._jobs.deleteMany(
      {
        deleted: {
          $lte: before,
        },
      },
      { session: this._session }
    );
  }

  async replaceUpcoming(doc: QueueDoc): Promise<QueueDoc> {
    await this.ready();

    if (!this._jobs) {
      throw new Error("init");
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
      { upsert: true, session: this._session }
    );

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

    await this._jobs.deleteMany(
      {
        ref: ref,
        deleted: null,
        ack: null,
        visible: {
          $gte: new Date(),
        },
      },
      { session: this._session }
    );
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

  listen(): void | null | undefined {
    if (!this._jobs) {
      throw new Error("init");
    }
    if (this._watch) {
      return;
    }

    this._watch = this._jobs.watch([{ $match: { operationType: "insert" } }]);
    this._watch.on("error", (err) => {
      console.error(`Mongo ChangeStream Error: ${err.message}`);
      this._watch?.removeAllListeners();
      Promise.resolve(this._watch?.close()).finally(() => {
        this._watch = undefined;
        this.listen();
      });
    });

    this._watch.on("change", (change) => {
      if (change.operationType !== "insert") {
        return;
      }
      this.events.emit("data");
    });
  }
}
