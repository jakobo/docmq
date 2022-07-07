import { v4 } from "uuid";
import { EventEmitter } from "events";
import { DateTime, Duration } from "luxon";
import cron from "cron-parser";

import {
  type QueueDocRecurrence,
  type JobHandler,
  type ProcessorConfig,
  type QueueDoc,
  type QueueOptions,
  type Emitter,
  type QueueStats,
  type RetryStrategy,
  type JobDefinition,
  type Driver,
} from "./types.js";
import { Worker } from "./worker.js";
import {
  asError,
  DocMQError,
  EnqueueError,
  ProcessorError,
  UnknownError,
  UnknownWorkerError,
} from "./error.js";

const DEFAULT_VISIBILITY = 30; // seconds
const DEFAULT_CONCURRENCY = 5;
const noop = () => {
  /* noop */
};

const resetStats = (): QueueStats => ({
  start: new Date(),
  end: new Date(),
  enqueued: 0,
  processed: 0,
  outcomes: {
    success: 0,
    failure: 0,
  },
  errors: {},
});

/**
 * The DocMQ `Queue` object is responsible for both the enqueueing and processing
 * operations of a queue. Once instantiated, a queue may be used for either or
 * both of these features.
 *
 * Most commonly, you will call `enqueue()` with a payload, which will then be
 * received by a matching `process()` function.
 *
 * On construction, a queue can receive a variety of {@see QueueOptions}, such
 * as using an alternate db, changing the retention policy for completed jobs,
 * and configuring intervals for output such as Queue statistics.
 *
 * Once created, a Queue will generate events, available at `queue.events`.
 *
 * @example
 * ```ts
 * const u = process.env.MONGO_URL;
 * const queue = new Queue(u, "myQueue");
 * queue.enqueue({
 *   sample: "payload"
 * })
 *
 * queue.process(async (job, api) => {
 *   console.log(job);
 *   await api.ack();
 * })
 * ```
 */
export class Queue<T, A = unknown, F extends Error = Error> {
  /**
   * An emitter associated with all interesting events that a queue can create
   * See: {@link Emitter}
   */
  events: Emitter<T, A, F>;

  protected name: string;
  protected driver: Driver;
  protected opts: Required<QueueOptions>;
  protected workers: Worker<T, A, F>[];
  protected destroyed: boolean;
  protected statInterval?: ReturnType<typeof setInterval>;
  protected stats: QueueStats;

  /** Wrap the payload in a JSON encoding */
  static encodePayload(p: unknown) {
    return JSON.stringify({ _: p });
  }

  /** Decode the payload, stripping away the outer JSON encoding */
  static decodePayload<T>(s: string) {
    return JSON.parse(s)._ as T;
  }

  constructor(driver: Driver, name: string, options?: QueueOptions) {
    if (name.length < 1) {
      throw new DocMQError("Queue name must be at least one letter long");
    }

    this.name = name;
    this.destroyed = false;
    this.workers = [];
    this.driver = driver;
    this.events = new EventEmitter() as Emitter<T, A, F>;
    this.opts = {
      retention: {
        jobs: options?.retention?.jobs ?? 3600,
      },
      statInterval:
        options?.statInterval === 0 ? 0 : options?.statInterval ?? 5,
    };

    // initialize stats
    this.stats = resetStats();

    // dispatch stats on interval
    if (this.opts.statInterval > 0) {
      this.addStatListeners();
      this.statInterval = setInterval(
        () => this.emitStats(),
        this.opts.statInterval * 1000
      );
    }
  }

  /**
   * Get the options this Queue was created with (readonly)
   * Can be useful to understand how the queue was configured, or in testing,
   * to insert data and simulate e2e scenarios
   */
  options(): Readonly<Required<QueueOptions>> {
    return this.opts;
  }

  /** A function that returns a promise resolving once all init dependenices are resolved */
  async ready() {
    try {
      await this.driver.ready();
    } catch (e) {
      this.events.emit(
        "error",
        e instanceof DocMQError
          ? e
          : new UnknownError("An unknown error occured")
      );
      throw e;
    }
    return true;
  }

  /**
   * Add a job to DocMQ
   * @param job A job, specified by {@link JobDefinition}
   */
  async enqueue(job: JobDefinition<T> | JobDefinition<T>[]) {
    const bulkJobs = Array.isArray(job) ? job : [job];

    if (this.destroyed) {
      throw new Error("Will not enqueue into a destroyed object");
    }

    // wait for ready
    await this.ready();

    const refList: string[] = [];
    const jobs = bulkJobs.map((v) => {
      let begin = v.runAt ?? new Date();
      let runEvery: QueueDocRecurrence | undefined | null;

      if (v.runEvery) {
        // check for duration first as its faster
        const d = Duration.fromISO(v.runEvery);
        if (d.isValid) {
          runEvery = {
            type: "duration",
            value: v.runEvery,
          };
        } else {
          // try cron
          try {
            const c = cron.parseExpression(v.runEvery, {
              currentDate: begin,
            });
            begin = c.next().toDate();
            runEvery = {
              type: "cron",
              value: v.runEvery,
            };
          } catch {
            // it was set, but neither was valid. This is an error
            throw new Error(
              `Job with ref (${
                v.ref ?? "unknown"
              }) and payload (${JSON.stringify(
                v.payload
              )}) had an invalid runEvery value. runEvery must be an ISO-8601 Duration or parsable cron expression.`
            );
          }
        }
      } else if (v.runEvery === null) {
        runEvery = null;
      }

      const retryStrategy: RetryStrategy = v.retryStrategy ?? {
        type: "fixed",
        amount: DEFAULT_VISIBILITY,
        jitter: 0,
      };

      if (v.ref) {
        refList.push(v.ref);
      }

      const doc: QueueDoc = {
        ref: v.ref ?? v4(),
        ack: null,
        visible: begin,
        payload: Queue.encodePayload(v.payload),
        attempts: {
          tries: 0,
          max: v.retries === 0 ? 0 : v.retries ?? 5,
          retryStrategy: {
            ...retryStrategy,
            jitter: retryStrategy.jitter ?? 0,
          },
        },
        repeat: {
          count: 0,
          last: begin,
          every: runEvery,
        },
      };
      return doc;
    });

    // replace all future jobs with these new values
    // if a job has "ack", it's pending and should be left alone
    const results = await Promise.allSettled(
      jobs.map((j) => this.driver.replaceUpcoming(j))
    );

    // split into success/failure
    const success: JobDefinition<T>[] = [];
    const failure: JobDefinition<T>[] = [];
    results.forEach((r, idx) => {
      const j = bulkJobs[idx];
      if (r.status === "rejected") {
        failure.push(j);
      } else {
        success.push(j);
      }
    });

    // emit add event
    this.events.emit("add", bulkJobs);

    // emit error event if required
    if (failure.length > 0) {
      const err = new EnqueueError(
        "Unable to add the included jobs to the queue"
      );
      err.jobs = failure;
      this.events.emit("error", err);
    }
  }

  /**
   * Promote a job by its ref so that it runs immediately. Only available
   * if supported by the DB Driver
   *
   * ```ts
   * await queue.promote("ref-value")
   * ```
   */
  async promote(ref: string) {
    await this.ready();
    await this.driver.promote(ref);
  }

  /**
   * Delay a job by its ref for a specified amount of time. This delays the
   * future execution of a job, but does not change its recurrence information
   *
   * ```ts
   * await queue.delay("ref-value", 15); // delay 15 seconds
   * ```
   */
  async delay(ref: string, delayBy: number) {
    await this.ready();
    await this.driver.delay(ref, delayBy);
  }

  /**
   * Replay a job. A replayed job will have its recurrence removed and is designed
   * for development, debugging, and testing scenarios. It's normally better to
   * enqueue a new job via `enqueue()` than to replay an existing job.
   *
   * Only available if supported by the DB Driver
   *
   * ```ts
   * await queue.replay("ref-value")
   * ```
   */
  async replay(ref: string) {
    await this.ready();
    await this.driver.replay(ref);
  }

  /**
   * Get the history of a ref's upcoming and previous runs
   */
  async history(ref: string | null, limit = 10, offset = 0) {
    await this.ready();
    return await this.driver.history(ref, limit, offset);
  }

  /**
   * Process pending jobs in the queue using the provided handler and configuration.
   * When starting a processor, you must include a `handler` which can receive and
   * acknowledge jobs. The simplest handler would be
   * ```ts
   * process(async (job, api) => {
   *   await api.ack()
   * })
   * ```
   *
   * Doing so would immediately "ack" the job, confirming it completed successfully.
   * Unacked jobs are treated as jobs which timed out, and will be picked up again
   * by the queue, retries allowing.
   *
   * Configuring a processor is done by specifying the {@link ProcessorConfig} as
   * the second argument; there you can control the concurrency, change the
   * visibility window for processing, and change how often DocMQ polls for new
   * events when in an idle state.
   */
  process(handler: JobHandler<T, A, F>, config?: ProcessorConfig) {
    if (this.destroyed) {
      throw new Error("Cannot process a destroyed queue");
    }

    let started = false;
    let paused = config?.pause === true ? true : false;
    const concurrency =
      typeof config?.concurrency === "number"
        ? Math.max(config.concurrency, 1)
        : DEFAULT_CONCURRENCY;
    const visibility =
      config?.visibility === 0
        ? config.visibility
        : config?.visibility || DEFAULT_VISIBILITY;
    const pollInterval =
      (typeof config?.pollInterval === "number" && config.pollInterval > 0
        ? config.pollInterval
        : 5) * 1000;

    const isPaused = () => paused;

    /**
     * Takes the next N items and schedules their work. Ensures only
     * one async operation is running at time manipulating this.workers
     */
    const takeAndProcess = async () => {
      if (paused || this.destroyed) {
        return;
      }

      // concurrency - max concurrency - current workers
      const limit = concurrency - this.workers.length;

      const next = await this.driver.take(visibility, limit);
      this.events.emit("log", `Received ${next.length} jobs`);
      next.forEach((doc) => {
        const w = new Worker<T, A, F>({
          driver: this.driver,
          name: this.name,
          doc,
          payload: Queue.decodePayload<T>(doc.payload),
          handler,
          emitter: this.events,
          visibility,
        });
        this.workers.push(w);
        this.events.emit("process", {
          ref: doc.ref,
          queue: this.name,
          attempt: doc.attempts.tries,
          maxTries: doc.attempts.max,
        });
        w.processOne()
          .then(() => {
            // on complete, remove self
            this.workers = (this.workers || []).filter((mw) => mw !== w);
            if (this.workers.length === 0) {
              this.events.emit("idle");
            }
          })
          .catch((e: unknown) => {
            this.workers = (this.workers || []).filter((mw) => mw !== w);
            const err = new UnknownWorkerError(
              "processOne: An unknown worker error occurred"
            );
            err.original = asError(e);
            this.events.emit("error", err);
          })
          .finally(() => {
            // check for new work, regardless ofd success/failure
            process.nextTick(() =>
              takeAndProcess().catch((e: unknown) => {
                const err = new UnknownWorkerError(
                  "takeAndProcess: An unknown worker error occurred"
                );
                err.original = asError(e);
                this.events.emit("error", err);
              })
            );
          });
      });
    };

    /**
     * When called, begins a run loop that
     * A) begins listening for new inserts to proactively trigger a take op
     * B) begins a blocking loop w/ async sleep to idle query for jobs
     */
    const run = async () => {
      // prevent duplicate run ops
      if (paused || started || this.destroyed) {
        return;
      }
      started = true;

      // wait for ready
      await this.ready();

      this.driver.listen();
      this.driver.events.on("data", () => {
        takeAndProcess().catch((e: unknown) => {
          const err = new ProcessorError(
            "An unknown error occured during takeAndProccess"
          );
          err.original = asError(e);
          this.events.emit("error", err);
        });
      });

      // start garbage collection of old jobs
      let gcTimer: ReturnType<typeof setTimeout> | undefined;
      const gc = () => {
        this.driver
          .clean(
            DateTime.now()
              .minus({ seconds: this.options().retention.jobs })
              .toJSDate()
          )
          .catch((e) => {
            const err = new ProcessorError(
              "Could not run garbage collection loop"
            );
            err.original = asError(e);
            this.events.emit("error", err);
          });
        gcTimer = setTimeout(() => {
          gc();
        }, 5000);
      };
      gc();

      try {
        while (!isPaused()) {
          await takeAndProcess();
          await sleep(pollInterval);
        }
      } catch (e) {
        const err = new ProcessorError(
          "Encountered a problem with the run() loop. When this happens, the queue is paused until a new change event comes in from mongo. Jobs will remain queued."
        );
        err.original = asError(e);
        this.events.emit("error", err);
      }

      if (typeof gcTimer !== "undefined") {
        clearTimeout(gcTimer);
      }

      this.driver.events.removeAllListeners("data");

      started = false; // can start again
    };

    // on start, unpause the queue and begin a run loop
    this.events.on("start", () => {
      paused = false;
      run().catch(noop);
    });

    // stopping sets the pause, letting queues drain
    this.events.on("stop", () => {
      paused = true;
    });

    process.nextTick(() => {
      if (config?.pause) {
        return;
      }
      // auto-start
      this.events.emit("start");
    });
  }

  /**
   * Remove a job by its ref value
   */
  async remove(ref: string) {
    await this.driver.removeUpcoming(ref);
  }

  /**
   * Start the queue if it isn't already started. If you provided
   * {@link ProcessorConfig}.pause = true, then calling this will
   * start the queue. This method has no effect on an already
   * started queue.
   */
  start() {
    this.events.emit("start");
  }

  /**
   * Stop the queue if it is already running. In some situations, it
   * may be desirable to stop the queue such as a shutdown operation. To
   * promote a clean shutdown, you can call `stop()` and listen for the
   * `idle` event to confirm all worker calls completed.
   */
  stop() {
    this.events.emit("stop");
  }

  /**
   * Destroy a queue. This removes all listeners, preventing a queue from
   * being restarted. Additionally, once destroyed, this queue cannot be
   * used for inserting additional jobs into the queue.
   */
  destroy() {
    this.destroyed = true; // hard stop all activity
    this.events.emit("stop");
    this.workers?.forEach((w) => w.destroy());
    this.events.removeAllListeners();
  }

  /** Add the stat listeners, using our own event system to capture outcomes */
  protected addStatListeners() {
    this.events.on("fail", (info) => {
      this.stats.outcomes.failure += 1;

      let errorType = "Error";
      if (typeof info.error === "undefined" || typeof info.error === "string") {
        errorType = "Error";
      } else if (info.error instanceof DocMQError) {
        // contains discriminator
        errorType = info.error.type;
      }

      if (typeof this.stats.errors[errorType] === "undefined") {
        this.stats.errors[errorType] = 0;
      }

      this.stats.errors[errorType] += 1;
    });

    this.events.on("add", () => {
      this.stats.enqueued += 1;
    });

    this.events.on("process", () => {
      this.stats.processed += 1;
    });

    this.events.on("ack", () => {
      this.stats.outcomes.success += 1;
    });
  }

  /** Emit the stats via the emitter */
  protected emitStats() {
    const st = this.stats;
    this.stats = resetStats();
    this.events.emit("stats", {
      queue: this.name,
      ...st,
      end: new Date(), // update transmission time
    });
  }
}

/** Sleep for a pre-determined amount of time in Ms */
const sleep = (durationMs: number) =>
  new Promise((resolve) => {
    setTimeout(resolve, durationMs);
  });
