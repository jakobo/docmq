import { DateTime } from "luxon";
import pg from "pg";
import { serializeError } from "serialize-error";

import { MaxAttemptsExceededError } from "../error.js";
import { QueueDoc, RepeatStrategy, RetryStrategy } from "../types.js";
import { BaseDriver } from "./base.js";

/** Describes the postgres row */
type QueueRow = {
  ref: string;
  visible: string;
  deleted: string | null;
  ack: string | null;
  dead: boolean;
  error: string | null;
  payload: string | null;
  attempts_tries: number;
  attempts_max: number;
  attempts_retry_strategy: string;
  repeat_count: number;
  repeat_last: string | null;
  repeat_every: string | null;
};

const SQL_COLUMNS = `(
  ref,
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

const toDoc = (row: QueueRow): QueueDoc => {
  return {
    ref: row.ref,
    visible: DateTime.fromISO(row.visible).toJSDate(),
    deleted: row.deleted ? DateTime.fromISO(row.deleted).toJSDate() : null,
    ack: row.ack,
    dead: row.dead,
    payload: row.payload,
    attempts: {
      tries: row.attempts_tries,
      max: row.attempts_max,
      retryStrategy: JSON.parse(row.attempts_retry_strategy),
    },
    repeat: {
      count: row.repeat_count,
      last: row.repeat_last
        ? DateTime.fromISO(row.repeat_last).toJSDate()
        : undefined,
      every: row.repeat_every ? JSON.parse(row.repeat_every) : undefined,
    },
  };
};

interface QueryIdent {
  schema: string;
  table: string;
}

const QUERIES = {
  /** Sets up the database */
  setup: {
    query: ({ schema, table }: QueryIdent) => `
      CREATE TABLE IF NOT EXISTS ${schema}.${table} (
	      id uuid NOT NULL DEFAULT gen_random_uuid(),
	      "ref" uuid NOT NULL,
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
	      CONSTRAINT jobs_pk PRIMARY KEY (id),
	      CONSTRAINT jobs_unique_ref_ack_deleted UNIQUE ("ref",ack,deleted)
      );
      CREATE INDEX IF NOT EXISTS jobs_ack_idx ON docmq.jobs (ack);
      CREATE INDEX IF NOT EXISTS jobs_deleted_ref_visible_idx ON docmq.jobs (deleted,"ref",visible);
      CREATE INDEX IF NOT EXISTS jobs_ref_visible_idx ON docmq.jobs ("ref",visible);
      CREATE INDEX IF NOT EXISTS jobs_dead_deleted_idx ON docmq.jobs (dead,deleted);
    `,
  },

  /** Take the next set of jobs to execute, returning the altered rows */
  take: {
    query: ({ schema, table }: QueryIdent) => `
      UPDATE ${schema}.${table}
      SET
        ack = gen_random_id(),
        visible = now() + ($1::integer * interval '1 second')
      WHERE ref = (
        SELECT ref
        FROM ${schema}.${table}
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
    query: ({ schema, table }: QueryIdent) => `
      UPDATE ${schema}.${table}
      SET deleted = now()
      WHERE
        ack = $1::uuid
        AND visible > now()
        AND deleted IS NULL`,
    variables: ({ ack }: { ack: string }) => [ack],
  },

  /** Fail a job */
  fail: {
    query: ({ schema, table }: QueryIdent) => `
      UPDATE ${schema}.${table}
      SET
        ack = null,
        visible = now() + ($2::integer * interval '1 second'),
        blob = blob ||
          jsonb_set(blob, '{attempt}', blob->attempt || '{"tries": $3::integer}')
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
    query: ({ schema, table }: QueryIdent) => `
      UPDATE ${schema}.${table}
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
    query: ({ schema, table }: QueryIdent) => `
      DELETE FROM ${schema}.${table}
      WHERE deleted < $1::timestamp`,
    variables: ({ before }: { before: string }) => [before],
  },

  /** Extend a job by its ack value */
  extendByAck: {
    query: ({ schema, table }: QueryIdent) => `
      UPDATE ${schema}.${table}
      SET
        visible = now() + ($2::integer * interval '1 second'),
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
    query: ({ schema, table }: QueryIdent) => `
      UPDATE ${schema}.${table}
      SET
        visible = now() + ($2::integer * interval '1 second'),
      WHERE
        ref = $1::uuid
        AND visible > now()
        AND deleted IS NULL`,
    variables: ({ ref, delayBy }: { ref: string; delayBy: number }) => [
      ref,
      delayBy,
    ],
  },

  /** Promote a job to run immediately based on its ref */
  promoteByRef: {
    query: ({ schema, table }: QueryIdent) => `
      UPDATE ${schema}.${table}
      SET
        visible = now(),
      WHERE
        ref = $1::uuid
        AND visible > now()
        AND deleted IS NULL`,
    variables: ({ ref }: { ref: string }) => [ref],
  },

  /** Replay a job by ref (reinsert w/ no repetition options */
  replayByRef: {
    query: ({ schema, table }: QueryIdent) => `
      INSERT INTO ${schema}.${table} ${SQL_COLUMNS}
      SELECT
        ref,
        now(),
        null,
        null,
        false,
        null,
        payload,
        0,
        1,
        attempts_retry_strategy,
        0,
        null,
        null

        FROM ${schema}.${table}
        WHERE
          ref = $1::uuid
          AND deleted <= now()
        ORDER BY deleted DESC
        LIMIT 1`,
    variables: ({ ref }: { ref: string }) => [ref],
  },

  /** Replace any upcoming versions of a job with new data */
  replaceUpcoming: {
    query: ({ schema, table }: QueryIdent) => `
      INSERT INTO ${schema}.${table} ${SQL_COLUMNS}
      VALUES ($1::uuid, $2::timestamp, null, null, false, null, $3::text, 0, $4::integer, $5::text, 0, null, $6::text)
      ON CONFLICT (ref, deleted, ack) DO UPDATE SET
        visible = $2::timestamp,
        payload = $3::text,
        attempts_tries = 0,
        attempts_max = $4::integer,
        attempts_retry_strategy = $5::text,
        repeat_last = null,
        repeat_every = $6::text`,
    variables: ({
      ref,
      visible,
      payload,
      maxAttempts,
      retryStrategy,
      repeatEvery,
    }: {
      ref: string;
      visible: Date;
      payload: string | null;
      maxAttempts: number;
      retryStrategy: RetryStrategy;
      repeatEvery: RepeatStrategy | null;
    }) => [
      ref,
      visible,
      payload,
      maxAttempts,
      JSON.stringify(retryStrategy),
      repeatEvery ? JSON.stringify(repeatEvery) : null,
    ],
  },

  /** Insert the next occurence of a job. Ignores conflict if future job already changed */
  insertNext: {
    query: ({ schema, table }: QueryIdent) => `
      INSERT INTO ${schema}.${table} ${SQL_COLUMNS}
      VALUES ($1::uuid, $2::timestamp, null, null, false, null, $3::text, 0, $4::integer, $5::text, $6::integer, $7::text, $8::text)
      ON CONFLICT (ref, deleted, ack) DO NOTHING`,
    variables: ({
      ref,
      visible,
      payload,
      maxAttempts,
      retryStrategy,
      repeatCount,
      repeatLast,
      repeatEvery,
    }: {
      ref: string;
      visible: Date;
      payload: string | null;
      maxAttempts: number;
      retryStrategy: RetryStrategy;
      repeatCount: number;
      repeatLast: Date;
      repeatEvery: RepeatStrategy | null;
    }) => [
      ref,
      visible,
      payload,
      maxAttempts,
      JSON.stringify(retryStrategy),
      repeatCount,
      DateTime.fromJSDate(repeatLast).toISO(),
      repeatEvery ? JSON.stringify(repeatEvery) : null,
    ],
  },

  /** Remove upcoming jobs by their ref */
  removeUpcoming: {
    query: ({ schema, table }: QueryIdent) => `
      DELETE FROM ${schema}.${table}
      WHERE
        ref = $1::uuid
        AND visible > now()
        AND deleted IS NULL
        AND ack IS NULL`,
    variables: ({ ref }: { ref: string }) => [ref],
  },

  /** Get the history without a ref */
  history: {
    query: ({ schema, table }: QueryIdent) => `
      SELECT * FROM ${schema}.${table}
      ORDER BY visible DESC
      OFFSET $1::integer
      LIMIT $2::integer`,
    variables: ({ offset, limit }: { offset: number; limit: number }) => [
      offset,
      limit,
    ],
  },

  /** Get the history with a known ref */
  historyByRef: {
    query: ({ schema, table }: QueryIdent) => `
      SELECT * FROM ${schema}.${table}
      WHERE ref = $1::uuid
      ORDER BY visible DESC
      OFFSET $2::integer
      LIMIT $3::integer`,
    variables: ({
      ref,
      offset,
      limit,
    }: {
      ref: string;
      offset: number;
      limit: number;
    }) => [ref, offset, limit],
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

  protected getTableIdentifier() {
    return `${this.getSchemaName()}.${this.getTableName()}`;
  }

  /** Initializes the mongo connection */
  protected async initialize(connection: pg.Pool): Promise<boolean> {
    if (!this._pool) {
      this._pool = connection;
    }

    // this check will evolve
    if (!this._validSchema) {
      let c: pg.PoolClient | undefined;
      this._validSchema = this._pool
        .connect()
        .then((client) => {
          c = client;
          return client.query(
            `SELECT COUNT(*) from ${this.getTableIdentifier()}`
          );
        })
        .then(() => true)
        .finally(() => {
          if (c) {
            c.release();
          }
        });
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
      console.error(e);
      await client.query("ROLLBACK");
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
        QUERIES.take.query({
          schema: this.getSchemaName(),
          table: this.getTableName(),
        }),
        QUERIES.take.variables({ visibility, limit })
      );

      const docs = results.rows.map(toDoc);
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
        QUERIES.ack.query({
          schema: this.getSchemaName(),
          table: this.getTableName(),
        }),
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
        QUERIES.fail.query({
          schema: this.getSchemaName(),
          table: this.getTableName(),
        }),
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

    const client = await this._pool.connect();
    try {
      await client.query<QueueRow>(
        QUERIES.dead.query({
          schema: this.getSchemaName(),
          table: this.getTableName(),
        }),
        QUERIES.dead.variables({
          ack: ackVal,
          error: JSON.stringify(serializeError(err)),
        })
      );
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
        QUERIES.extendByAck.query({
          schema: this.getSchemaName(),
          table: this.getTableName(),
        }),
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
      await client.query<QueueRow>(
        QUERIES.promoteByRef.query({
          schema: this.getSchemaName(),
          table: this.getTableName(),
        }),
        QUERIES.promoteByRef.variables({
          ref,
        })
      );
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
      await client.query<QueueRow>(
        QUERIES.delayByRef.query({
          schema: this.getSchemaName(),
          table: this.getTableName(),
        }),
        QUERIES.delayByRef.variables({
          ref,
          delayBy,
        })
      );
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
        QUERIES.replayByRef.query({
          schema: this.getSchemaName(),
          table: this.getTableName(),
        }),
        QUERIES.replayByRef.variables({
          ref,
        })
      );
    } finally {
      client.release();
    }
  }

  async history(
    ref: string | null,
    limit = 10,
    offset = 0
  ): Promise<QueueDoc[]> {
    await this.ready();

    if (!this._pool) {
      throw new Error("init");
    }

    const query = ref
      ? QUERIES.historyByRef.query({
          schema: this.getSchemaName(),
          table: this.getTableName(),
        })
      : QUERIES.history.query({
          schema: this.getSchemaName(),
          table: this.getTableName(),
        });

    const variables = ref
      ? QUERIES.historyByRef.variables({ ref, offset, limit })
      : QUERIES.history.variables({ offset, limit });

    const client = await this._pool.connect();
    try {
      const results = await client.query<QueueRow>(query, variables);

      const docs = results.rows.map(toDoc);
      return docs;
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
        QUERIES.cleanOldJobs.query({
          schema: this.getSchemaName(),
          table: this.getTableName(),
        }),
        QUERIES.cleanOldJobs.variables({
          before: DateTime.fromJSDate(before).toISO(),
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

    const client = await this._pool.connect();

    await client.query<QueueRow>(
      QUERIES.replaceUpcoming.query({
        schema: this.getSchemaName(),
        table: this.getTableName(),
      }),
      QUERIES.replaceUpcoming.variables({
        ref: doc.ref,
        visible: doc.visible,
        payload: doc.payload,
        maxAttempts: doc.attempts.max,
        retryStrategy: doc.attempts.retryStrategy,
        repeatEvery: doc.repeat,
      })
    );

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
        QUERIES.removeUpcoming.query({
          schema: this.getSchemaName(),
          table: this.getTableName(),
        }),
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

    const client = await this._pool.connect();
    try {
      await client.query<QueueRow>(
        QUERIES.insertNext.query({
          schema: this.getSchemaName(),
          table: this.getTableName(),
        }),
        QUERIES.insertNext.variables({
          ref: doc.ref,
          visible: nextRun,
          payload: doc.payload,
          maxAttempts: doc.attempts.max,
          retryStrategy: doc.attempts.retryStrategy,
          repeatCount: doc.repeat.count + 1,
          repeatLast: nextRun,
          repeatEvery: doc.repeat,
        })
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
