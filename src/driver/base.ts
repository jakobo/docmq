import { DateTime, Duration } from "luxon";
import cron from "cron-parser";
import EventEmitter from "events";

import {
  type DriverOptions,
  type DriverEmitter,
  type QueueDoc,
  type Driver,
} from "../types.js";

const asynced = (...args: unknown[]) =>
  new Promise<unknown>((r) => {
    r(args);
  });

export class BaseDriver implements Driver {
  events: DriverEmitter;
  protected connection: unknown;
  protected options: DriverOptions | undefined;
  protected schema: string;
  protected table: string;
  protected init: Promise<boolean>;
  constructor(connection: unknown, options?: DriverOptions) {
    this.connection = connection;
    this.options = options;
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
  async getSchema(): Promise<unknown> {
    await asynced();
    throw new Error("Not implemented");
  }

  /** Gets the schema name */
  getSchemaName() {
    return this.schema;
  }

  /** Get the table object or name */
  async getTable(): Promise<unknown> {
    await asynced();
    throw new Error("Not implemented");
  }

  /** Gets the table name */
  getTableName() {
    return this.table;
  }

  /** Bookend a transaction with driver specific handling */
  async transaction(body: () => Promise<unknown>) {
    await asynced(body);
    throw new Error("Not implemented");
  }

  /** Take N items from the queue for processing */
  async take(visibility: number, limit = 1): Promise<QueueDoc[]> {
    await asynced(visibility, limit);
    throw new Error("Not implemented");
  }

  /** Ack a job, removing it from the queue */
  async ack(ack: string) {
    await asynced(ack);
    throw new Error("Not implemented");
  }

  /** Promote a job, making it immediately available for running */
  async promote(ref: string) {
    await asynced(ref);
    throw new Error("Not implemented");
  }

  /** Delay a job, pushing its visibility window out */
  async delay(ref: string, delayBy: number) {
    await asynced(ref, delayBy);
    throw new Error("Not implemented");
  }

  /** Replay a job, copying and inserting a new job to run immediately */
  async replay(ref: string) {
    await asynced(ref);
    throw new Error("Not implemented");
  }

  /** Fail a job, shifting the next run ahead to a retry time */
  async fail(ack: string, retryIn: number, attempt: number) {
    await asynced(ack, retryIn, attempt);
    throw new Error("Not implemented");
  }

  /** Place an item into the dead letter queue and ack it */
  async dead(doc: QueueDoc) {
    await asynced(doc);
    throw new Error("Not implemented");
  }

  /** Extend the runtime of a job */
  async ping(ack: string, extendBy = 15) {
    await asynced(ack, extendBy);
    throw new Error("Not implemented");
  }

  /** Remove any jobs that are before a certain date */
  async clean(before: Date) {
    await asynced(before);
    throw new Error("Not implemented");
  }

  /** Replace any upcoming instances of a doc with new data */
  async replaceUpcoming(doc: QueueDoc): Promise<QueueDoc> {
    await asynced(doc);
    throw new Error("Not implemented");
  }

  /** Remove all upcoming instances of a job by its ref */
  async removeUpcoming(ref: string) {
    await asynced(ref);
    throw new Error("Not implemented");
  }

  /** Finds the next occurence of a job, either through a cron or duration */
  findNext(doc: QueueDoc): Date | undefined {
    // if no repeat options, eject
    if (!doc.repeat.every) {
      return;
    }

    // if cron, just take next from now
    let nextRun = new Date();
    if (doc.repeat.every.type === "cron") {
      const c = cron.parseExpression(doc.repeat.every.value, {
        currentDate: new Date(),
      });
      nextRun = c.next().toDate();
    } else if (doc.repeat.every.type === "duration") {
      const dur = Duration.fromISO(doc.repeat.every.value);
      const dt = DateTime.now().plus(dur);
      nextRun = dt.toJSDate();
    } else {
      // invalid
      return;
    }

    return nextRun;
  }

  /** Create the next instance of a job */
  async createNext(doc: QueueDoc) {
    await asynced(doc);
    throw new Error("Not implemented");
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
