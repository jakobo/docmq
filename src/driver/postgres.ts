import { DateTime } from "luxon";
import pg from "pg";
import {
  asError,
  DriverConnectionError,
  DriverError,
  DriverInitializationError,
  DriverNoMatchingAckError,
  DriverNoMatchingRefError,
  MaxAttemptsExceededError,
} from "../error.js";
import { QueueDoc } from "../types.js";
import { BaseDriver } from "./base.js";
import crypto from "crypto";

/** Describes the postgres row */
type QueueRow = {
  r: string;
  visible: Date;
  deleted: Date | null;
  ack: string | null;
  dead: boolean;
  error: string | null;
  payload: string | null;
  attempts_tries: number;
  attempts_max: number;
  attempts_retry_strategy: string;
  repeat_count: number;
  repeat_last: Date | null;
  repeat_every: string | null;
};

export const SQL_COLUMNS = `(
  r,
  visible,
  deleted,
  ack,
  dead,
  error,
  payload,
  attempts_tries,
  attempts_max,
  attempts_retry_strategy,
  repeat_count,
  repeat_last,
  repeat_every
)`;

/** Convert a pg row to a QueueDoc object */
export const fromPg = (row: QueueRow): QueueDoc => {
  return {
    ref: row.r,
    visible: row.visible,
    deleted: row.deleted ?? null,
    ack: row.ack,
    dead: row.dead,
    error: row.error ?? undefined,
    payload: row.payload,
    attempts: {
      tries: row.attempts_tries,
      max: row.attempts_max,
      retryStrategy: JSON.parse(row.attempts_retry_strategy),
    },
    repeat: {
      count: row.repeat_count,
      last: row.repeat_last ?? undefined,
      every: row.repeat_every ? JSON.parse(row.repeat_every) : undefined,
    },
  };
};

/** Convert a doc to a pg row */
export const toPg = (doc: QueueDoc) => [
  doc.ref,
  doc.visible,
  doc.deleted ?? null,
  doc.ack ?? null,
  doc.dead ?? false,
  doc.error ?? null,
  doc.payload ?? null,
  doc.attempts.tries,
  doc.attempts.max,
  JSON.stringify(doc.attempts.retryStrategy),
  doc.repeat.count,
  doc.repeat.last ?? null,
  doc.repeat.every ?? null,
];

interface QueryIdent {
  schema: string;
  table: string;
}

export const escapeIdentifier = (s: string) =>
  '"' + s.replace(/"/g, '""') + '"';

// create a consistent name index, namespaced to the table
const nameIndex = (name: string, fields: string[], table: string) => {
  const f = fields.join("_");
  const h = crypto
    .createHash("sha256")
    .update(table)
    .digest("hex")
    .substring(0, 8);

  return escapeIdentifier(`docmq-${name}-${f}-${h}`);
};

// a NIL UUID
const NIL = "00000000-0000-0000-0000-000000000000";
const MAX_LISTENERS = 100;

export const QUERIES = {
  notify: {
    query: () => `SELECT pg_notify('docmq', '' || random())`,
  },
  /** Sets up the database */
  setup: {
    query: ({ schema, table }: QueryIdent) => `
      CREATE SCHEMA IF NOT EXISTS ${schema};

      CREATE TABLE IF NOT EXISTS ${table} (
	      id uuid NOT NULL DEFAULT gen_random_uuid(),
	      r uuid NOT NULL,
        visible timestamptz NOT NULL,
	      deleted timestamptz NULL,
	      ack uuid NULL,
	      dead boolean NULL DEFAULT false,
	      error text NULL,
	      payload text NULL,
	      attempts_tries integer NOT NULL DEFAULT 0,
	      attempts_max integer NOT NULL DEFAULT 1,
	      attempts_retry_strategy text NULL,
	      repeat_count integer NOT NULL DEFAULT 0,
	      repeat_last timestamptz NULL,
	      repeat_every text NULL,
        gen_deleted timestamptz GENERATED ALWAYS AS (COALESCE(deleted, to_timestamp(0))) STORED,
        gen_ack uuid GENERATED ALWAYS AS (COALESCE(ack, '${NIL}'::uuid)) STORED,
	      CONSTRAINT ${nameIndex("jobs", ["pk"], table)} PRIMARY KEY (id),
        CONSTRAINT ${nameIndex(
          "jobs",
          ["unique", "r", "ack", "deleted"],
          table
        )} UNIQUE (r, gen_ack, gen_deleted)
      );

      CREATE INDEX IF NOT EXISTS ${nameIndex(
        "jobs",
        ["ack"],
        table
      )} ON ${table} (ack);

      CREATE INDEX IF NOT EXISTS ${nameIndex(
        "jobs",
        ["deleted", "r", "visible"],
        table
      )} ON ${table} (deleted, r, visible);

      CREATE INDEX IF NOT EXISTS ${nameIndex(
        "jobs",
        ["r", "visible"],
        table
      )} ON ${table} (r, visible);

      CREATE INDEX IF NOT EXISTS ${nameIndex(
        "jobs",
        ["dead", "deleted"],
        table
      )} ON ${table} (dead, deleted);
    `,
  },

  /** Take the next set of jobs to execute, returning the altered rows */
  take: {
    query: ({ table }: QueryIdent) => `
      UPDATE ${table}
      SET
        ack = gen_random_uuid(),
        visible = now() + ($1::integer * interval '1 second')
      WHERE r IN (
        SELECT r
        FROM ${table}
        WHERE
          deleted IS NULL
          AND visible <= now()
        ORDER BY visible ASC
        LIMIT $2::integer
        FOR UPDATE SKIP LOCKED
      )
      RETURNING *`,
    variables: ({
      visibility,
      limit,
    }: {
      visibility: number;
      limit: number;
    }) => [visibility, limit],
  },

  /** Ack a job */
  ack: {
    query: ({ table }: QueryIdent) => `
      UPDATE ${table}
      SET deleted = now()
      WHERE
        ack = $1::uuid
        AND visible > now()
        AND deleted IS NULL;
    `,
    variables: ({ ack }: { ack: string }) => [ack],
  },

  /** Fail a job */
  fail: {
    query: ({ table }: QueryIdent) => `
      UPDATE ${table}
      SET
        ack = null,
        visible = now() + ($2::integer * interval '1 second'),
        attempts_tries = $3::integer
      WHERE
        ack = $1::uuid
        AND visible > now()
        AND deleted IS NULL;
    `,
    variables: ({
      ack,
      retryIn,
      attempt,
    }: {
      ack: string;
      retryIn: number;
      attempt: number;
    }) => [ack, retryIn, attempt],
  },

  /** Mark a job as dead (DLQ) */
  dead: {
    query: ({ table }: QueryIdent) => `
      UPDATE ${table}
      SET
        dead = true,
        error = $2::text,
        deleted = now()
      WHERE
        ack = $1::uuid
        AND visible > now()
        AND deleted IS NULL;
    `,
    variables: ({ ack, error }: { ack: string; error: string }) => [ack, error],
  },

  cleanOldJobs: {
    query: ({ table }: QueryIdent) => `
      DELETE FROM ${table}
      WHERE deleted < $1::timestamptz`,
    variables: ({ before }: { before: Date }) => [
      DateTime.fromJSDate(before).toISO(),
    ],
  },

  /** Extend a job by its ack value */
  extendByAck: {
    query: ({ table }: QueryIdent) => `
      UPDATE ${table}
      SET
        visible = now() + ($2::integer * interval '1 second')
      WHERE
        ack = $1::uuid
        AND visible > now()
        AND deleted IS NULL`,
    variables: ({ ack, extendBy }: { ack: string; extendBy: number }) => [
      ack,
      extendBy,
    ],
  },

  /** Delay a job by its ref value */
  delayByRef: {
    query: ({ table }: QueryIdent) => `
      UPDATE ${table}
      SET
        visible = now() + ($2::integer * interval '1 second')
      WHERE
        r = $1::uuid
        AND visible > now()
        AND deleted IS NULL`,
    variables: ({ ref, delayBy }: { ref: string; delayBy: number }) => [
      ref,
      delayBy,
    ],
  },

  /** Promote a job to run immediately based on its ref */
  promoteByRef: {
    query: ({ table }: QueryIdent) => `
      UPDATE ${table}
      SET
        visible = now()
      WHERE
        r = $1::uuid
        AND visible > now()
        AND deleted IS NULL`,
    variables: ({ ref }: { ref: string }) => [ref],
  },

  /** Replay a job by ref (reinsert w/ no repetition options */
  replayByRef: {
    query: ({ table }: QueryIdent) => `
      INSERT INTO ${table} ${SQL_COLUMNS}
      SELECT
        r,                          -- ref
        now(),                      -- visible
        null,                       -- deleted
        null,                       -- ack
        false,                      -- dead
        null,                       -- error
        payload,                    -- payload
        0,                          -- attempts_tries
        1,                          -- attempts_max
        attempts_retry_strategy,    -- attempts_retry_strategy
        0,                          -- repeat_count
        null,                       -- repeat_last
        null                        -- repeat_every

        FROM ${table}
        WHERE
          r = $1::uuid
          AND deleted <= now()
        ORDER BY deleted DESC
        LIMIT 1`,
    variables: ({ ref }: { ref: string }) => [ref],
  },

  /** Replace any upcoming versions of a job with new data */
  // TODO coalesce deleted
  replaceUpcoming: {
    query: ({ table }: QueryIdent) => `
      INSERT INTO ${table} ${SQL_COLUMNS}
      VALUES (
        $1::uuid,           -- r
        $2::timestamptz,    -- visible
        $3::timestamptz,    -- deleted
        $4::uuid,           -- ack
        $5::boolean,        -- dead
        $6::text,           -- error
        $7::text,           -- payload
        $8::int,            -- attempts_tries
        $9::int,            -- attempts_max
        $10::text,          -- attempts_retry_strategy (json)
        $11::int,           -- repeat_count
        $12::timestamptz,   -- repeat_last
        $13::text           -- repeat_every (json)
      )
      ON CONFLICT (r, gen_ack, gen_deleted) DO UPDATE SET 
        visible = $2::timestamptz,
        payload = $7::text,
        attempts_tries = 0,
        attempts_max = $9::integer,
        attempts_retry_strategy = $10::text,
        repeat_last = null,
        repeat_every = $13::text;
    `,
    variables: (doc: QueueDoc) => toPg(doc),
  },

  /** Insert a single job, letting conflicts create an error */
  insertOne: {
    query: ({ table }: QueryIdent) => `
      INSERT INTO ${table} ${SQL_COLUMNS}
      VALUES (
        $1::uuid,           -- r
        $2::timestamptz,    -- visible
        $3::timestamptz,    -- deleted
        $4::uuid,           -- ack
        $5::boolean,        -- dead
        $6::text,           -- error
        $7::text,           -- payload
        $8::int,            -- attempts_tries
        $9::int,            -- attempts_max
        $10::text,          -- attempts_retry_strategy (json)
        $11::int,           -- repeat_count
        $12::timestamptz,   -- repeat_last
        $13::text           -- repeat_every (json)
      )
    `,
    variables: (doc: QueueDoc) => toPg(doc),
  },

  /** Insert the next occurence of a job. Ignores conflict if future job already changed */
  insertNext: {
    query: ({ table }: QueryIdent) => `
      INSERT INTO ${table} ${SQL_COLUMNS}
      VALUES (
        $1::uuid,           -- r
        $2::timestamptz,    -- visible
        $3::timestamptz,    -- deleted
        $4::uuid,           -- ack
        $5::boolean,        -- dead
        $6::text,           -- error
        $7::text,           -- payload
        $8::int,            -- attempts_tries
        $9::int,            -- attempts_max
        $10::text,          -- attempts_retry_strategy (json)
        $11::int,           -- repeat_count
        $12::timestamptz,   -- repeat_last
        $13::text           -- repeat_every (json)
      )
      ON CONFLICT (r, gen_ack, gen_deleted) DO NOTHING;
    `,
    variables: (doc: QueueDoc) => toPg(doc),
  },

  /** Remove upcoming jobs by their ref */
  removeUpcoming: {
    query: ({ table }: QueryIdent) => `
      DELETE FROM ${table}
      WHERE
        r = $1::uuid
        AND visible > now()
        AND deleted IS NULL
        AND ack IS NULL`,
    variables: ({ ref }: { ref: string }) => [ref],
  },
};

interface PGTransaction {
  active: boolean;
  client: pg.PoolClient;
}

/**
 * **Requires `pg` as a Peer Dependency to use**
 *
 * Postgres Driver Class. Creates a connection that allows DocMQ to talk to
 * a Postgres or Postgres-compatible instance
 */
export class PgDriver extends BaseDriver<never, never, PGTransaction> {
  protected _pool: pg.Pool | undefined;
  protected _watch: Promise<pg.PoolClient> | undefined;
  protected _workerClient: pg.PoolClient | undefined;
  protected _validSchema: Promise<boolean> | undefined;

  getPool() {
    if (typeof this._pool === "undefined") {
      throw new Error("not initialized");
    }
    return this._pool;
  }

  getQueryObjects() {
    return {
      schema: escapeIdentifier(this.getSchemaName()),
      table:
        escapeIdentifier(this.getSchemaName()) +
        "." +
        escapeIdentifier(this.getTableName()),
    };
  }

  /** Initializes the mongo connection */
  protected async initialize(connection: pg.Pool): Promise<boolean> {
    if (!this._pool) {
      this._pool = connection;
      this._pool.on("error", (err) => {
        const e = new DriverError("Postgres driver encountered an error");
        e.original = err;
        this.events.emit("error", e);
      });
      this._pool.setMaxListeners(
        Math.max(MAX_LISTENERS, this._pool.getMaxListeners())
      );
    }

    // ensure a valid schema before continuing
    if (!this._validSchema) {
      this._validSchema = new Promise((resolve) =>
        connection.query(QUERIES.setup.query(this.getQueryObjects()), (err) => {
          if (err) {
            console.error(err);
          }
          resolve(true);
        })
      );
    }

    await this._validSchema;
    return true;
  }

  destroy(): void {
    if (this._workerClient) {
      this._workerClient.removeAllListeners();
    }
  }

  /** Perform a 2-phase commit if you need access to postgres' underlying transaction */
  async transaction(
    body: (tx: PGTransaction) => Promise<unknown>
  ): Promise<void> {
    await this.ready();
    if (!this._pool) {
      throw new DriverInitializationError();
    }

    const client = await this._pool.connect();

    try {
      await client.query("BEGIN");
      await body({
        active: true,
        client,
      });
      await client.query("COMMIT");
      client.release();
    } catch (e) {
      await client.query("ROLLBACK");
      client.release();
      throw e;
    }
  }

  async take(
    visibility: number,
    limit = 10,
    tx?: PGTransaction
  ): Promise<QueueDoc[]> {
    await this.ready();

    const client = tx?.client ?? this._pool;

    if (!client) {
      throw new DriverInitializationError();
    }

    const q = QUERIES.take.query(this.getQueryObjects());
    const v = QUERIES.take.variables({ visibility, limit });

    try {
      const results = await client.query<QueueRow>(q, v);
      const docs = results.rows.map(fromPg);
      return docs;
    } catch (e) {
      const err = new DriverError(
        "Encountered an error running a postgres query: " +
          JSON.stringify({
            query: q,
            variables: v,
          })
      );
      err.original = asError(e);
      this.events.emit("error", err);
    }

    return [];
  }

  async ack(ack: string, tx?: PGTransaction): Promise<void> {
    await this.ready();

    const client = tx?.client ?? this._pool;

    if (!client) {
      throw new DriverInitializationError();
    }

    if (ack === null) {
      throw new Error("ERR_NULL_ACK");
    }

    const q = QUERIES.ack.query(this.getQueryObjects());
    const v = QUERIES.ack.variables({
      ack,
    });

    try {
      const results = await client.query<QueueRow>(q, v);
      if (results.rowCount < 1) {
        throw new DriverNoMatchingAckError(ack);
      }
    } catch (e) {
      if (e instanceof DriverNoMatchingAckError) {
        throw e; // rethrow as immediate error
      }
      const err = new DriverError(
        "Encountered an error running a postgres query: " +
          JSON.stringify({
            query: q,
            variables: v,
          })
      );
      err.original = asError(e);
      this.events.emit("error", err);
    }
  }

  async fail(
    ack: string,
    retryIn: number,
    attempt: number,
    tx?: PGTransaction
  ): Promise<void> {
    await this.ready();

    const client = tx?.client ?? this._pool;

    if (!client) {
      throw new DriverInitializationError();
    }

    if (ack === null) {
      throw new Error("ERR_NULL_ACK");
    }

    const q = QUERIES.fail.query(this.getQueryObjects());
    const v = QUERIES.fail.variables({
      ack,
      retryIn,
      attempt,
    });

    try {
      const results = await client.query<QueueRow>(q, v);
      if (results.rowCount < 1) {
        throw new DriverNoMatchingAckError(ack);
      }
    } catch (e) {
      if (e instanceof DriverNoMatchingAckError) {
        throw e; // rethrow as immediate error
      }
      const err = new DriverError(
        "Encountered an error running a postgres query: " +
          JSON.stringify({
            query: q,
            variables: v,
          })
      );
      err.original = asError(e);
      this.events.emit("error", err);
    }
  }

  async dead(doc: QueueDoc, tx?: PGTransaction): Promise<void> {
    await this.ready();

    const ackVal = doc.ack;
    if (typeof ackVal === "undefined" || !ackVal) {
      throw new Error("Missing ack");
    }

    const client = tx?.client ?? this._pool;

    if (!client) {
      throw new DriverInitializationError();
    }

    const err = new MaxAttemptsExceededError(
      `Exceeded the maximum number of retries (${doc.attempts.max}) for this job`
    );

    // serialize-error is ESM only
    const { serializeError } = await import("serialize-error");

    const q = QUERIES.dead.query(this.getQueryObjects());
    const v = QUERIES.dead.variables({
      ack: ackVal,
      error: JSON.stringify(serializeError(err)),
    });

    try {
      const res = await client.query<QueueRow>(q, v);
      if (res.rowCount === 0) {
        throw new DriverNoMatchingAckError(ackVal);
      }
    } catch (e) {
      if (e instanceof DriverNoMatchingAckError) {
        throw e; // rethrow as immediate error
      }
      const err = new DriverError(
        "Encountered an error running a postgres query: " +
          JSON.stringify({
            query: q,
            variables: v,
          })
      );
      err.original = asError(e);
      this.events.emit("error", err);
    }
  }

  async ping(ack: string, extendBy = 15, tx?: PGTransaction): Promise<void> {
    await this.ready();

    const client = tx?.client ?? this._pool;

    if (!client) {
      throw new DriverInitializationError();
    }

    if (ack === null) {
      throw new Error("ERR_NULL_ACK");
    }

    const q = QUERIES.extendByAck.query(this.getQueryObjects());
    const v = QUERIES.extendByAck.variables({
      ack,
      extendBy,
    });

    try {
      const results = await client.query<QueueRow>(q, v);
      if (results.rowCount < 1) {
        throw new DriverNoMatchingAckError(ack);
      }
    } catch (e) {
      if (e instanceof DriverNoMatchingAckError) {
        throw e; // rethrow as immediate error
      }
      const err = new DriverError(
        "Encountered an error running a postgres query: " +
          JSON.stringify({
            query: q,
            variables: v,
          })
      );
      err.original = asError(e);
      this.events.emit("error", err);
    }
  }

  async promote(ref: string, tx?: PGTransaction): Promise<void> {
    await this.ready();

    const client = tx?.client ?? this._pool;

    if (!client) {
      throw new DriverInitializationError();
    }

    const q = QUERIES.promoteByRef.query(this.getQueryObjects());
    const v = QUERIES.promoteByRef.variables({
      ref,
    });

    try {
      const results = await client.query<QueueRow>(q, v);
      if (results.rowCount < 1) {
        throw new DriverNoMatchingRefError("ERR_UNKNOWN_ACK_OR_EXPIRED");
      }
    } catch (e) {
      if (e instanceof DriverNoMatchingRefError) {
        throw e; // rethrow as immediate error
      }
      const err = new DriverError(
        "Encountered an error running a postgres query: " +
          JSON.stringify({
            query: q,
            variables: v,
          })
      );
      err.original = asError(e);
      this.events.emit("error", err);
    }
  }

  async delay(ref: string, delayBy: number, tx?: PGTransaction): Promise<void> {
    await this.ready();

    const client = tx?.client ?? this._pool;

    if (!client) {
      throw new DriverInitializationError();
    }

    const q = QUERIES.delayByRef.query(this.getQueryObjects());
    const v = QUERIES.delayByRef.variables({
      ref,
      delayBy,
    });

    try {
      const results = await client.query<QueueRow>(q, v);

      if (results.rowCount < 1) {
        throw new DriverNoMatchingRefError(ref);
      }
    } catch (e) {
      if (e instanceof DriverNoMatchingRefError) {
        throw e; // rethrow as immediate error
      }
      const err = new DriverError(
        "Encountered an error running a postgres query: " +
          JSON.stringify({
            query: q,
            variables: v,
          })
      );
      err.original = asError(e);
      this.events.emit("error", err);
    }
  }

  async replay(ref: string, tx?: PGTransaction): Promise<void> {
    await this.ready();

    const client = tx?.client ?? this._pool;

    if (!client) {
      throw new DriverInitializationError();
    }

    const q = QUERIES.replayByRef.query(this.getQueryObjects());
    const v = QUERIES.replayByRef.variables({
      ref,
    });

    try {
      await client.query<QueueRow>(q, v);
    } catch (e) {
      const err = new DriverError(
        "Encountered an error running a postgres query: " +
          JSON.stringify({
            query: q,
            variables: v,
          })
      );
      err.original = asError(e);
      this.events.emit("error", err);
    }
  }

  async clean(before: Date, tx?: PGTransaction): Promise<void> {
    await this.ready();

    const client = tx?.client ?? this._pool;

    if (!client) {
      throw new DriverInitializationError();
    }

    const q = QUERIES.cleanOldJobs.query(this.getQueryObjects());
    const v = QUERIES.cleanOldJobs.variables({
      before,
    });

    try {
      await client.query<QueueRow>(q, v);
    } catch (e) {
      const err = new DriverError(
        "Encountered an error running a postgres query: " +
          JSON.stringify({
            query: q,
            variables: v,
          })
      );
      err.original = asError(e);
      this.events.emit("error", err);
    }
  }

  async replaceUpcoming(doc: QueueDoc, tx?: PGTransaction): Promise<QueueDoc> {
    await this.ready();

    const client = tx?.client ?? this._pool;

    if (!client) {
      throw new DriverInitializationError();
    }

    const q = QUERIES.replaceUpcoming.query(this.getQueryObjects());
    const v = QUERIES.replaceUpcoming.variables(doc);

    try {
      await client.query<QueueRow>(q, v);
      await client.query(QUERIES.notify.query());
    } catch (e) {
      const err = new DriverError(
        "Encountered an error running a postgres query: " +
          JSON.stringify({
            query: q,
            variables: v,
          })
      );
      err.original = asError(e);
      this.events.emit("error", err);
    }

    return doc;
  }

  async removeUpcoming(ref: string, tx?: PGTransaction): Promise<void> {
    await this.ready();

    const client = tx?.client ?? this._pool;

    if (!client) {
      throw new DriverInitializationError();
    }

    if (!ref) {
      throw new Error("No ref provided");
    }

    const q = QUERIES.removeUpcoming.query(this.getQueryObjects());
    const v = QUERIES.removeUpcoming.variables({
      ref,
    });

    try {
      await client.query<QueueRow>(q, v);
    } catch (e) {
      const err = new DriverError(
        "Encountered an error running a postgres query: " +
          JSON.stringify({
            query: q,
            variables: v,
          })
      );
      err.original = asError(e);
      this.events.emit("error", err);
    }
  }

  async createNext(doc: QueueDoc): Promise<void> {
    await this.ready();

    const nextRun = this.findNext(doc);
    if (!nextRun) {
      return;
    }
    if (!this._pool) {
      throw new DriverInitializationError();
    }

    const next: QueueDoc = {
      ...doc,
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

    const q = QUERIES.insertNext.query(this.getQueryObjects());
    const v = QUERIES.insertNext.variables(next);

    try {
      await this._pool.query<QueueRow>(q, v);
    } catch (e) {
      const err = new DriverError(
        "Encountered an error running a postgres query: " +
          JSON.stringify({
            query: q,
            variables: v,
          })
      );
      err.original = asError(e);
      this.events.emit("error", err);
    }
  }

  async listen() {
    if (!this._pool) {
      throw new DriverInitializationError();
    }
    if (this._pool.totalCount <= 1) {
      return; // do not listen unless there are enough connections
    }
    if (this._watch) {
      return;
    }

    this._watch = this._pool.connect();
    let client: pg.PoolClient | undefined;

    try {
      client = await this._watch;

      client.query("LISTEN docmq", () => {
        /*empty*/
      });
      client.on("notification", () => {
        this.events.emit("data");
      });
      client.on("error", (err) => {
        if (client) {
          client.release();
          client.removeAllListeners();
        }
        this._watch = undefined;
        const e = new DriverConnectionError(
          "Postgres change stream encountered an error and needs to reconnect"
        );
        e.original = err;
        this.events.emit("error", e);
        void this.listen();
      });
    } catch (e) {
      if (client) {
        client.release();
        const err = new DriverConnectionError(
          "Could not connect to Postgres for LISTEN/NOTIFY"
        );
        err.original = e instanceof Error ? e : undefined;
        this.events.emit("halt", err);
      }
    }
  }
}
