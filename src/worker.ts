import {
  asError,
  MaxAttemptsExceededError,
  UnAckedHandlerError,
  UncaughtHandlerError,
  WorkerAPIError,
} from "./error.js";
import { DateTime } from "luxon";
import { type ClientSession } from "mongodb";
import { serializeError } from "serialize-error";
import { ack, createNext, fail, ping } from "./mongo/functions.js";
import {
  type Collections,
  type HandlerApi,
  type JobHandler,
  type QueueDoc,
  type WorkerOptions,
  type Emitter,
} from "./types.js";
import { exponentialBackoff, fixedBackoff, linearBackoff } from "./backoff.js";

interface ProcessStatus {
  ack: boolean;
  fail: boolean;
}

const FALLBACK_RETRY_DELAY = 5;

/**
 * Internal Worker Class. Mostly a container class that encapsulates the worker actions
 */
export class Worker<T, A = unknown, F extends Error = Error> {
  protected name: string;
  protected session: ClientSession;
  protected collections: Collections;
  protected handler: JobHandler<T, A, F>;
  protected emitter: Emitter<T, A, F>;
  protected visibility: number;
  protected doc: QueueDoc;
  protected payload: T;

  constructor(options: WorkerOptions<T, A, F>) {
    this.name = options.name;
    this.session = options.session;
    this.collections = options.collections;
    this.handler = options.handler;
    this.emitter = options.emitter;
    this.visibility = options.visibility;
    this.doc = options.doc;
    this.payload = options.payload;
  }

  protected fqqn() {
    return this.name;
  }

  /** Create an API that performs the necessary docdb operations */
  createApi(status: ProcessStatus): HandlerApi<A, F> {
    return {
      ref: this.doc.ref,
      attempt: this.doc.attempts.tries,
      visible: this.visibility,
      ack: async (result) => {
        status.ack = true;

        try {
          const ackVal = this.doc.ack;
          if (typeof ackVal === "undefined" || !ackVal) {
            throw new Error("Missing ack");
          }
          await this.session.withTransaction(async () => {
            await ack(this.collections.jobs, ackVal, this.session);
            await createNext(this.collections.jobs, this.doc, this.session);
          });
          this.emitter.emit("ack", {
            queue: this.fqqn(),
            ref: this.doc.ref,
            payload: this.payload,
            attempt: this.doc.attempts.tries,
            maxTries: this.doc.attempts.max,
            result,
          });
        } catch (e) {
          const err = new WorkerAPIError("Unable to ACK message successfully");
          err.original = asError(e);
          err.api = "ack";
          this.emitter.emit("error", err);
        }
      },
      fail: async (result, retryOptions) => {
        status.fail = true;

        try {
          const ackVal = this.doc.ack;
          if (typeof ackVal === "undefined" || !ackVal) {
            throw new Error("Missing ack");
          }

          // calculate delay until next job
          let delay = 0;
          if (typeof retryOptions?.after !== "undefined") {
            delay = Math.ceil(
              DateTime.now()
                .until(DateTime.fromJSDate(retryOptions.after))
                .toDuration()
                .shiftTo("seconds")
                .get("seconds")
            );
          } else if (this.doc.attempts.retryStrategy.type === "linear") {
            delay = linearBackoff(
              this.doc.attempts.retryStrategy,
              this.doc.attempts.tries
            );
          } else if (this.doc.attempts.retryStrategy.type === "exponential") {
            delay = exponentialBackoff(
              this.doc.attempts.retryStrategy,
              this.doc.attempts.tries
            );
          } else {
            // unknown, use fixed
            delay = fixedBackoff(
              this.doc.attempts.retryStrategy ?? {
                type: "fixed",
                amount: FALLBACK_RETRY_DELAY,
              }
            );
          }

          await fail(
            this.collections.jobs,
            ackVal,
            delay,
            retryOptions?.attempt ?? this.doc.attempts.tries,
            this.session
          );
          this.emitter.emit("fail", {
            queue: this.fqqn(),
            ref: this.doc.ref,
            payload: this.payload,
            attempt: this.doc.attempts.tries,
            maxTries: this.doc.attempts.max,
            error: typeof result === "string" ? new Error(result) : result,
          });
        } catch (e) {
          const err = new WorkerAPIError("Unable to FAIL message successfully");
          err.original = asError(e);
          err.api = "fail";
          this.emitter.emit("error", err);
        }
      },
      ping: async (extendBy = this.visibility) => {
        try {
          if (typeof this.doc.ack === "undefined" || !this.doc.ack) {
            throw new Error("Missing ack");
          }
          await ping(this.collections.jobs, this.doc.ack, extendBy);
          this.emitter.emit(
            "ping",
            {
              queue: this.fqqn(),
              ref: this.doc.ref,
              payload: this.payload,
              attempt: this.doc.attempts.tries,
              maxTries: this.doc.attempts.max,
            },
            extendBy
          );
        } catch (e) {
          const err = new WorkerAPIError("Unable to PING message successfully");
          err.original = asError(e);
          err.api = "ping";
          this.emitter.emit("error", err);
        }
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
        if (typeof ackVal === "undefined" || !ackVal) {
          throw new Error("Missing ack");
        }

        await ack(this.collections.jobs, ackVal, this.session);
        await createNext(this.collections.jobs, this.doc, this.session);
        this.emitter.emit("dead", {
          queue: this.fqqn(),
          ref: this.doc.ref,
          payload: this.payload,
          attempt: this.doc.attempts.tries,
          maxTries: this.doc.attempts.max,
          error: typeof err === "string" ? new Error(err) : err,
        });
      });
      return;
    }

    // run handler
    try {
      await this.handler(this.payload, api);
    } catch (e) {
      // uncaught exception from handler
      const err = new UncaughtHandlerError(
        "An implicit fail() was triggered because the handler threw an error"
      );
      err.original = e instanceof Error ? e : undefined;
      await api.fail(err);
      return;
    }

    // unacked / unfailed is a failure
    if (!status.ack && !status.fail) {
      const err = new UnAckedHandlerError(
        "No ack() or fail() was called in the handler and may represent an error in your code"
      );
      await api.fail(err);
      return;
    }
  }

  destroy() {
    // TODO
  }
}
