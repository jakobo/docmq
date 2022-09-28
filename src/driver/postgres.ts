import { DateTime } from "luxon";
import pg from "pg";
import { MaxAttemptsExceededError } from "../error.js";
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

export const QUERIES = {
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
        AND deleted IS NULL`,
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
        AND deleted IS NULL`,
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
        AND deleted IS NULL`,
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
        repeat_every = $13::text`,
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
      ON CONFLICT (r, gen_ack, gen_deleted) DO NOTHING`,
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

/**
 * **Requires `pg` as a Peer Dependency to use**
 *
 * Postgres Driver Class. Creates a connection that allows DocMQ to talk to
 * a Postgres or Postgres-compatible instance
 */
export class PgDriver extends BaseDriver {
  protected _pool: pg.Pool | undefined;
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
  async transaction(body: () => Promise<unknown>): Promise<void> {
    await this.ready();
    if (!this._pool) {
      throw new Error("init");
    }

    const client = await this._pool.connect();
    await client.query("BEGIN");
    try {
      await body();
      await client.query("COMMIT");
    } catch (e) {
      await client.query("ROLLBACK");
      throw e;
    } finally {
      client.release();
    }
  }

  async take(visibility: number, limit = 10): Promise<QueueDoc[]> {
    await this.ready();

    if (!this._pool) {
      throw new Error("init");
    }

    const client = await this._pool.connect();

    try {
      const results = await client.query<QueueRow>(
        QUERIES.take.query(this.getQueryObjects()),
        QUERIES.take.variables({ visibility, limit })
      );

      const docs = results.rows.map(fromPg);
      return docs;

      // from row to doc
    } finally {
      client.release();
    }
  }

  async ack(ack: string): Promise<void> {
    await this.ready();

    if (!this._pool) {
      throw new Error("init");
    }

    if (ack === null) {
      throw new Error("ERR_NULL_ACK");
    }

    const client = await this._pool.connect();

    try {
      const results = await client.query<QueueRow>(
        QUERIES.ack.query(this.getQueryObjects()),
        QUERIES.ack.variables({
          ack,
        })
      );

      if (results.rowCount < 1) {
        throw new Error("ERR_NO_ACK_RESPONSE");
      }
    } finally {
      client.release();
    }
  }

  async fail(ack: string, retryIn: number, attempt: number): Promise<void> {
    await this.ready();

    if (!this._pool) {
      throw new Error("init");
    }

    if (ack === null) {
      throw new Error("ERR_NULL_ACK");
    }

    const client = await this._pool.connect();
    try {
      const results = await client.query<QueueRow>(
        QUERIES.fail.query(this.getQueryObjects()),
        QUERIES.fail.variables({
          ack,
          retryIn,
          attempt,
        })
      );

      if (results.rowCount < 1) {
        throw new Error("ERR_NO_ACK_RESPONSE");
      }
    } finally {
      client.release();
    }
  }

  async dead(doc: QueueDoc): Promise<void> {
    await this.ready();

    const ackVal = doc.ack;
    if (typeof ackVal === "undefined" || !ackVal) {
      throw new Error("Missing ack");
    }

    if (!this._pool) {
      throw new Error("init");
    }

    const err = new MaxAttemptsExceededError(
      `Exceeded the maximum number of retries (${doc.attempts.max}) for this job`
    );

    // serialize-error is esm-only and must be await imported
    const serializeError = (await import("serialize-error")).serializeError;

    const client = await this._pool.connect();
    try {
      const res = await client.query<QueueRow>(
        QUERIES.dead.query(this.getQueryObjects()),
        QUERIES.dead.variables({
          ack: ackVal,
          error: JSON.stringify(serializeError(err)),
        })
      );
      if (res.rowCount === 0) {
        throw new Error("Cannot mark an expired item as dead");
      }
    } finally {
      client.release();
    }
  }

  async ping(ack: string, extendBy = 15): Promise<void> {
    await this.ready();

    if (!this._pool) {
      throw new Error("init");
    }
    if (ack === null) {
      throw new Error("ERR_NULL_ACK");
    }

    const client = await this._pool.connect();
    try {
      const results = await client.query<QueueRow>(
        QUERIES.extendByAck.query(this.getQueryObjects()),
        QUERIES.extendByAck.variables({
          ack,
          extendBy,
        })
      );

      if (results.rowCount < 1) {
        throw new Error("ERR_UNKNOWN_ACK");
      }
    } finally {
      client.release();
    }
  }

  async promote(ref: string): Promise<void> {
    await this.ready();

    if (!this._pool) {
      throw new Error("init");
    }

    const client = await this._pool.connect();
    try {
      const results = await client.query<QueueRow>(
        QUERIES.promoteByRef.query(this.getQueryObjects()),
        QUERIES.promoteByRef.variables({
          ref,
        })
      );

      if (results.rowCount < 1) {
        throw new Error("ERR_UNKNOWN_ACK_OR_EXPIRED");
      }
    } finally {
      client.release();
    }
  }

  async delay(ref: string, delayBy: number): Promise<void> {
    await this.ready();

    if (!this._pool) {
      throw new Error("init");
    }

    const client = await this._pool.connect();
    try {
      const results = await client.query<QueueRow>(
        QUERIES.delayByRef.query(this.getQueryObjects()),
        QUERIES.delayByRef.variables({
          ref,
          delayBy,
        })
      );

      if (results.rowCount < 1) {
        throw new Error("ERR_UNKNOWN_ACK_OR_EXPIRED");
      }
    } finally {
      client.release();
    }
  }

  async replay(ref: string): Promise<void> {
    await this.ready();

    if (!this._pool) {
      throw new Error("init");
    }

    const client = await this._pool.connect();
    try {
      await client.query<QueueRow>(
        QUERIES.replayByRef.query(this.getQueryObjects()),
        QUERIES.replayByRef.variables({
          ref,
        })
      );
    } finally {
      client.release();
    }
  }

  async clean(before: Date): Promise<void> {
    await this.ready();

    if (!this._pool) {
      throw new Error("init");
    }

    const client = await this._pool.connect();
    try {
      await client.query<QueueRow>(
        QUERIES.cleanOldJobs.query(this.getQueryObjects()),
        QUERIES.cleanOldJobs.variables({
          before,
        })
      );
    } finally {
      client.release();
    }
  }

  async replaceUpcoming(doc: QueueDoc): Promise<QueueDoc> {
    await this.ready();

    if (!this._pool) {
      throw new Error("init");
    }

    const q = QUERIES.replaceUpcoming.query(this.getQueryObjects());
    const v = QUERIES.replaceUpcoming.variables(doc);

    try {
      await this._pool.query<QueueRow>(q, v);
    } catch (e) {
      console.error(q, v);
      throw e;
    }

    return doc;
  }

  async removeUpcoming(ref: string): Promise<void> {
    await this.ready();

    if (!this._pool) {
      throw new Error("init");
    }
    if (!ref) {
      throw new Error("No ref provided");
    }

    const client = await this._pool.connect();
    try {
      await client.query<QueueRow>(
        QUERIES.removeUpcoming.query(this.getQueryObjects()),
        QUERIES.removeUpcoming.variables({
          ref,
        })
      );
    } finally {
      client.release();
    }
  }

  async createNext(doc: QueueDoc): Promise<void> {
    await this.ready();

    const nextRun = this.findNext(doc);
    if (!nextRun) {
      return;
    }
    if (!this._pool) {
      throw new Error("init");
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

    const client = await this._pool.connect();
    try {
      await client.query<QueueRow>(
        QUERIES.insertNext.query(this.getQueryObjects()),
        QUERIES.insertNext.variables(next)
      );
    } finally {
      client.release();
    }
  }

  // listen(): void | null | undefined {
  //   if (!this._jobs) {
  //     throw new Error("init");
  //   }
  //   if (this._watch) {
  //     return;
  //   }

  //   // begin listening
  //   this._watch = this._jobs.watch([{ $match: { operationType: "insert" } }]);

  //   this._watch.on("change", (change) => {
  //     if (change.operationType !== "insert") {
  //       return;
  //     }
  //     this.events.emit("data");
  //   });
  // }
}
