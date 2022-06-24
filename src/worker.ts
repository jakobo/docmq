import {
  MaxAttemptsExceededError,
  UnAckedHandlerError,
  UncaughtHandlerError,
} from "./error.js";
import { DateTime } from "luxon";
import { type ClientSession } from "mongodb";
import { serializeError } from "serialize-error";
import { ack, createNext, ping } from "./mongo/functions.js";
import {
  type Collections,
  type HandlerApi,
  type JobHandler,
  type QueueDoc,
  type WorkerOptions,
  type Emitter,
} from "./types.js";

/**
 * Internal Worker Class. Mostly a container class that encapsulates the worker actions
 */
export class Worker<T> {
  session: ClientSession;
  collections: Collections;
  handler: JobHandler<T>;
  emitter: Emitter;
  visibility: number;
  doc: QueueDoc;
  payload: unknown;

  constructor(options: WorkerOptions<T>) {
    this.session = options.session;
    this.collections = options.collections;
    this.handler = options.handler;
    this.emitter = options.emitter;
    this.visibility = options.visibility;
    this.doc = options.doc;
    this.payload = JSON.parse(this.doc.payload)._;
  }

  /** Create an API that performs the necessary docdb operations */
  createApi(status: ProcessStatus): HandlerApi {
    return {
      ref: this.doc.ref,
      ack: async (result: unknown) => {
        status.ack = true;
        const ackVal = this.doc.ack;
        if (typeof ackVal === "undefined") {
          throw new Error("Missing ack");
        }

        await this.session.withTransaction(async () => {
          await ack(this.collections.jobs, ackVal, this.session);
          await createNext(this.collections.jobs, this.doc, this.session);
          this.emitter.emit("ack", this.doc.ref, result, this.payload);
        });
      },
      fail: async (result: unknown) => {
        status.fail = true;
        const ackVal = this.doc.ack;
        if (typeof ackVal === "undefined") {
          throw new Error("Missing ack");
        }

        await this.session.withTransaction(async () => {
          await ack(this.collections.jobs, ackVal, this.session);
          await createNext(this.collections.jobs, this.doc, this.session);
          this.emitter.emit(
            "fail",
            this.doc.ref,
            result,
            this.payload,
            this.doc.attempts.tries,
            this.doc.attempts.max
          );
        });
      },
      ping: async (extendBy = this.visibility) => {
        if (typeof this.doc.ack === "undefined") {
          throw new Error("Missing ack");
        }
        await ping(this.collections.jobs, this.doc.ack, extendBy);
        this.emitter.emit("ping", this.doc.ref, extendBy);
      },
    };
  }

  async processOne() {
    const status: ProcessStatus = {
      ack: false,
      fail: false,
    };
    const api = this.createApi(status);

    // Dead Letter Queue support
    // if dead (retries exhausted), move to dlq, ack, schedule next, and return within a transaction
    if (this.doc.attempts.tries > this.doc.attempts.max) {
      await this.session.withTransaction(async () => {
        const err = new MaxAttemptsExceededError(
          `Exceeded the maximum number of retries (${this.doc.attempts.max}) for this job`
        );
        await this.collections.deadLetterQueue.insertOne(
          {
            ...this.doc,
            ack: undefined,
            visible: DateTime.now().toJSDate(),
            error: serializeError(err),
          },
          { session: this.session }
        );

        const ackVal = this.doc.ack;
        if (typeof ackVal === "undefined") {
          throw new Error("Missing ack");
        }

        await ack(this.collections.jobs, ackVal, this.session);
        await createNext(this.collections.jobs, this.doc, this.session);
        this.emitter.emit(
          "fail",
          this.doc.ref,
          err,
          this.payload,
          this.doc.attempts.tries,
          this.doc.attempts.max
        );
      });
      return;
    }

    // run handler
    try {
      await this.handler(this.payload as T, api);
    } catch (e) {
      // uncaught exception from handler
      const err = new UncaughtHandlerError(
        "An implicit fail() was triggered because the handler threw an error"
      );
      err.original = e instanceof Error ? e : undefined;
      await api.fail(err);
    }

    // unacked / unfailed is a failure
    if (!status.ack && !status.fail) {
      const err = new UnAckedHandlerError(
        "No ack() or fail() was called in the handler and may represent an error in your code"
      );
      await api.fail(err);
    }
  }

  destroy() {
    // TODO
  }
}

interface ProcessStatus {
  ack: boolean;
  fail: boolean;
}
