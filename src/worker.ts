import {
  asError,
  MaxAttemptsExceededError,
  UnAckedHandlerError,
  UncaughtHandlerError,
  WorkerAPIError,
  WorkerProcessingError,
} from "./error.js";
import { DateTime } from "luxon";
import {
  type HandlerApi,
  type QueueDoc,
  type WorkerOptions,
  type Emitter,
  type Driver,
} from "./types.js";
import { exponentialBackoff, fixedBackoff, linearBackoff } from "./backoff.js";

/** An internal status to determine if we've ack-ed or fail-ed something */
interface ProcessStatus {
  ack: boolean;
  fail: boolean;
}

// a fallback delay
const FALLBACK_RETRY_DELAY = 5;

/**
 * Internal Worker Class. Mostly a container class that encapsulates the worker actions
 */
export class Worker<T, A = unknown, F extends Error = Error> {
  protected driver: Driver;
  protected emitter: Emitter<T, A, F>;
  protected doc: QueueDoc;
  protected options: WorkerOptions<T, A, F>;

  constructor(options: WorkerOptions<T, A, F>) {
    this.options = options;
    this.driver = options.driver;
    this.emitter = options.emitter;
    this.doc = options.doc;
  }

  protected fqqn() {
    return this.options.name;
  }

  /** Create an API that performs the necessary docdb operations */
  createApi(status: ProcessStatus): HandlerApi<A, F> {
    return {
      ref: this.doc.ref,
      attempt: this.doc.attempts.tries,
      visible: this.options.visibility,
      ack: async (result) => {
        status.ack = true;

        try {
          const ackVal = this.doc.ack;
          if (typeof ackVal === "undefined" || !ackVal) {
            throw new Error("Missing ack");
          }

          const event = {
            queue: this.fqqn(),
            ref: this.doc.ref,
            payload: this.options.payload,
            attempt: this.doc.attempts.tries,
            maxTries: this.doc.attempts.max,
            result,
            next: this.driver.findNext(this.doc),
          };

          await this.driver.transaction(async () => {
            await this.driver.createNext(this.doc); // no transaction, but failing prevents ack
            await this.driver.ack(ackVal);
            this.emitter.emit("ack", event);
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

          const event = {
            queue: this.fqqn(),
            ref: this.doc.ref,
            payload: this.options.payload,
            attempt: this.doc.attempts.tries,
            maxTries: this.doc.attempts.max,
            error: typeof result === "string" ? new Error(result) : result,
            next: DateTime.now().plus({ seconds: delay }).toJSDate(),
          };

          await this.driver.transaction(async () => {
            await this.driver.fail(
              ackVal,
              delay,
              retryOptions?.attempt ?? this.doc.attempts.tries
            );
            this.emitter.emit("fail", event);
          });
        } catch (e) {
          const err = new WorkerAPIError("Unable to FAIL message successfully");
          err.original = asError(e);
          err.api = "fail";
          this.emitter.emit("error", err);
        }
      },
      ping: async (extendBy = this.options.visibility) => {
        try {
          if (typeof this.doc.ack === "undefined" || !this.doc.ack) {
            throw new Error("Missing ack");
          }
          await this.driver.ping(this.doc.ack, extendBy);

          this.emitter.emit(
            "ping",
            {
              queue: this.fqqn(),
              ref: this.doc.ref,
              payload: this.options.payload,
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
      try {
        const err = new MaxAttemptsExceededError(
          `Exceeded the maximum number of retries (${this.doc.attempts.max}) for this job`
        );
        const event = {
          queue: this.fqqn(),
          ref: this.doc.ref,
          payload: this.options.payload,
          attempt: this.doc.attempts.tries,
          maxTries: this.doc.attempts.max,
          error: typeof err === "string" ? new Error(err) : err,
          next: this.driver.findNext(this.doc),
        };
        await this.driver.transaction(async () => {
          await this.driver.dead(this.doc);
          await this.driver.createNext(this.doc);
          this.emitter.emit("dead", event);
        });
      } catch (e) {
        const err = new WorkerProcessingError(
          "Unable to commit the dead letter queue transaction"
        );
        err.original = asError(e);
        this.emitter.emit("error", err);
      }
      return;
    }

    // run handler
    try {
      await this.options.handler(this.options.payload, api);
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
