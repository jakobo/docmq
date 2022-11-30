import { DateTime, Duration } from "luxon";
import cron from "cron-parser";
import EventEmitter from "eventemitter3";

import {
  type DriverOptions,
  type DriverEmitter,
  type QueueDoc,
  type Driver,
} from "../types.js";
import { DriverNotImplementedError } from "../error.js";

/** asynced is a helper method that accepts any number of unknown arguments, and returns a Promise<unknown> */
const asynced = (...args: unknown[]) =>
  new Promise<unknown>((r) => {
    r(args);
  });

export class BaseDriver<Schema = unknown, Table = unknown, TxInfo = unknown>
  implements Driver<Schema, Table, TxInfo>
{
  events: DriverEmitter;
  private conn: unknown;
  private schema: string;
  private table: string;
  private init: Promise<boolean>;
  constructor(connection: unknown, options?: DriverOptions) {
    this.conn = connection;
    this.events = new EventEmitter() as DriverEmitter;
    this.schema = options?.schema ?? "docmq";
    this.table = options?.table ?? "jobs";
    this.init = this.initialize(connection);
  }

  /** Initialize and connect to the driver. Operation should be treated as idempoetent */
  protected async initialize(connection: unknown) {
    await asynced(connection);
    return true;
  }

  /** A promise that resolves when initialization is complete */
  async ready() {
    await this.init;
    return true;
  }

  /** Gets the schema object or name */
  async getSchema(): Promise<Schema> {
    await asynced();
    throw new DriverNotImplementedError();
  }

  /** Gets the schema name */
  getSchemaName() {
    return this.schema;
  }

  /** Get the table object or name */
  async getTable(): Promise<Table> {
    await asynced();
    throw new DriverNotImplementedError();
  }

  /** Gets the table name */
  getTableName() {
    return this.table;
  }

  /** Bookend a transaction with driver specific handling */
  async transaction(body: (txn: TxInfo) => Promise<unknown>) {
    await asynced(body);
    throw new DriverNotImplementedError();
  }

  /** Take N items from the queue for processing */
  async take(visibility: number, limit = 1, tx?: TxInfo): Promise<QueueDoc[]> {
    await asynced(visibility, limit, tx);
    throw new DriverNotImplementedError();
  }

  /** Ack a job, removing it from the queue */
  async ack(ack: string, tx?: TxInfo) {
    await asynced(ack, tx);
    throw new DriverNotImplementedError();
  }

  /** Promote a job, making it immediately available for running */
  async promote(ref: string, tx?: TxInfo) {
    await asynced(ref, tx);
    throw new DriverNotImplementedError();
  }

  /** Delay a job, pushing its visibility window out */
  async delay(ref: string, delayBy: number, tx?: TxInfo) {
    await asynced(ref, delayBy, tx);
    throw new DriverNotImplementedError();
  }

  /** Replay a job, copying and inserting a new job to run immediately */
  async replay(ref: string, tx?: TxInfo) {
    await asynced(ref, tx);
    throw new DriverNotImplementedError();
  }

  /** Fail a job, shifting the next run ahead to a retry time */
  async fail(ack: string, retryIn: number, attempt: number, tx?: TxInfo) {
    await asynced(ack, retryIn, attempt, tx);
    throw new DriverNotImplementedError();
  }

  /** Place an item into the dead letter queue and ack it */
  async dead(doc: QueueDoc, tx?: TxInfo) {
    await asynced(doc, tx);
    throw new DriverNotImplementedError();
  }

  /** Extend the runtime of a job */
  async ping(ack: string, extendBy = 15, tx?: TxInfo) {
    await asynced(ack, extendBy, tx);
    throw new DriverNotImplementedError();
  }

  /** Remove any jobs that are before a certain date */
  async clean(before: Date, tx?: TxInfo) {
    await asynced(before, tx);
    throw new DriverNotImplementedError();
  }

  /** Replace any upcoming instances of a doc with new data */
  async replaceUpcoming(doc: QueueDoc, tx?: TxInfo): Promise<QueueDoc> {
    await asynced(doc, tx);
    throw new DriverNotImplementedError();
  }

  /** Remove all upcoming instances of a job by its ref */
  async removeUpcoming(ref: string, tx?: TxInfo) {
    await asynced(ref, tx);
    throw new DriverNotImplementedError();
  }

  /** Finds the next occurence of a job, either through a cron or duration */
  findNext(doc: QueueDoc, relativeTo?: Date): Date | undefined {
    // if no repeat options, eject
    if (!doc.repeat.every || !doc.repeat.last) {
      return;
    }

    let nextRun: Date | undefined;
    const now = relativeTo ?? new Date();
    const tz = doc.repeat.timezone ?? undefined;

    if (doc.repeat.every.type === "cron") {
      const c = cron.parseExpression(doc.repeat.every.value, {
        currentDate: doc.repeat.last,
        tz,
        iterator: true,
      });
      // loop until future is >= now
      let future = c.next();
      while (future.value.toDate() < now) {
        future = c.next();
      }
      nextRun = future.value.toDate();
    } else if (doc.repeat.every.type === "duration") {
      const dur = Duration.fromISO(doc.repeat.every.value);
      const luxNow = DateTime.fromJSDate(now);
      let future = tz
        ? DateTime.fromJSDate(doc.repeat.last).setZone(tz).plus(dur)
        : DateTime.fromJSDate(doc.repeat.last).plus(dur);
      // loop until future is >= now
      while (future < luxNow) {
        future = future.plus(dur);
      }
      nextRun = future.toJSDate();
    } else {
      // invalid
      return;
    }

    return nextRun;
  }

  /** Create the next instance of a job */
  async createNext(doc: QueueDoc) {
    await asynced(doc);
    throw new DriverNotImplementedError();
  }

  /** Begin listening for changes on the data source. Should operate idempotently */
  async listen() {
    await asynced();
  }

  /** Destroy and clean up the driver */
  destroy() {
    // noop
  }
}
