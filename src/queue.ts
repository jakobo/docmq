import { MongoClient } from "mongodb";
import { v4 } from "uuid";
import { EventEmitter } from "events";
import { DateTime, Duration } from "luxon";
import cron from "cron-parser";

import { removeExpired, replaceUpcoming, take } from "./mongo/functions.js";
import { updateIndexes } from "./mongo/indexes.js";
import {
  type Collections,
  type ConfigDoc,
  type QueueDocRecurrence,
  type DeadQueueDoc,
  type EnqueueJobOptions,
  type JobHandler,
  type ProcessorConfig,
  type QueueDoc,
  type QueueOptions,
  type Topology,
  type Emitter,
  type BulkEnqueueJobOptions,
  type QueueStats,
  type RetryStrategy,
  type ExtendedQueueOptions,
} from "./types.js";
import { Worker } from "./worker.js";
import {
  asError,
  DocMQError,
  EnqueueError,
  NonReplicatedMongoInstanceError,
  ProcessorError,
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
  events: Readonly<Emitter<T, A, F>>;

  protected name: string;
  protected client: MongoClient;
  protected opts: ExtendedQueueOptions;
  protected workers: Worker<T, A, F>[];
  protected destroyed: boolean;
  protected topology: Promise<Topology>;
  protected indexesReady: Promise<boolean>;
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

  constructor(url: string, name: string, options?: QueueOptions) {
    if (name.length < 1) {
      throw new DocMQError("Queue name must be at least one letter long");
    }

    this.name = name;
    this.destroyed = false;
    this.workers = [];
    this.client = new MongoClient(url);
    this.events = new EventEmitter() as Emitter<T, A, F>;
    this.opts = {
      db: options?.db ?? "docmq",
      collections: {
        job: name,
        deadLetter: `${name}/dead`,
        config: `${name}/config`,
      },
      retention: {
        jobs: options?.retention?.jobs ?? 86400,
      },
      statInterval:
        options?.statInterval === 0 ? 0 : options?.statInterval ?? 5,
    };

    // store promises for concurrent tasks
    // these are idempotent, but required for our queue to work
    this.topology = this.determineTopology();
    this.indexesReady = updateIndexes(this.collections());

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
  options(): Readonly<ExtendedQueueOptions> {
    return this.opts;
  }

  /** A function that returns a promise resolving once all init dependenices are resolved */
  async ready() {
    await Promise.allSettled([this.topology, this.indexesReady]);
  }

  /**
   * Add a job to DocMQ
   * @param payload The payload to use when executing this job
   * @param options A set of {@link EnqueueJobOptions} for this job
   */
  async enqueue(payload: T, options?: EnqueueJobOptions) {
    return this.enqueueMany([
      {
        ...(options ?? {}),
        payload,
      },
    ]);
  }

  /**
   * Add multiple jobs to DocMQ
   * @param bulkJobs A set of jobs with their payload included. See {@link BulkEnqueueJobOptions}
   */
  async enqueueMany(bulkJobs: BulkEnqueueJobOptions<T>[]) {
    if (this.destroyed) {
      throw new Error("Will not enqueue into a destroyed object");
    }

    // wait for ready
    await this.ready();
    const topology = await this.topology;
    if (!topology.hasOplog) {
      const err = new NonReplicatedMongoInstanceError(
        "DocMQ requires an oplog in order to gaurentee events such as scheduling future work"
      );
      this.events.emit("error", err);
      throw err;
    }

    const refList: string[] = [];
    const jobs = bulkJobs.map((v) => {
      let begin = v.runAt ?? new Date();
      let runEvery: QueueDocRecurrence | undefined;

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
      jobs.map((j) => replaceUpcoming(this.collections().jobs, j))
    );

    // split into success/failure
    const success: BulkEnqueueJobOptions<T>[] = [];
    const failure: BulkEnqueueJobOptions<T>[] = [];
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

      const next = await take(this.collections().jobs, visibility, limit);
      this.events.emit("log", `Received ${next.length} jobs`);
      next.forEach((doc) => {
        const w = new Worker<T, A, F>({
          session: this.client.startSession(),
          collections: this.collections(),
          name: this.fqqn(),
          doc,
          payload: Queue.decodePayload<T>(doc.payload),
          handler,
          emitter: this.events,
          visibility,
        });
        this.workers.push(w);
        this.events.emit("process", {
          ref: doc.ref,
          queue: this.fqqn(),
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
              "An unknown worker error occurred"
            );
            err.original = asError(e);
            this.events.emit("error", err);
          })
          .finally(() => {
            // check for new work, regardless ofd success/failure
            process.nextTick(() =>
              takeAndProcess().catch((e: unknown) => {
                const err = new UnknownWorkerError(
                  "An unknown worker error occurred"
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
      const topology = await this.topology;

      if (!topology.hasOplog) {
        this.events.emit(
          "error",
          new NonReplicatedMongoInstanceError(
            "DocMQ requires Mongo replication to be enabled (even if a cluster size of 1) for oplog functionality."
          )
        );
        return;
      }

      const watch = this.collections().jobs.watch([
        { $match: { operationType: "insert" } },
      ]);

      watch.on("change", (change) => {
        if (change.operationType !== "insert") {
          return;
        }
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
        removeExpired(
          this.collections().jobs,
          DateTime.now()
            .minus({ seconds: this.options().retention.jobs })
            .toJSDate()
        ).catch((e) => {
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

      watch.removeAllListeners();

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

  /** Get a set of collection objects for this queue */
  protected collections(): Collections {
    const o = this.options();
    const db = this.client.db(o.db);

    return {
      jobs: db.collection<QueueDoc>(o.collections.job),
      deadLetterQueue: db.collection<DeadQueueDoc>(o.collections.deadLetter),
      config: db.collection<ConfigDoc>(o.collections.config),
    };
  }

  /** Provide an informational object based on the hello command */
  protected async determineTopology(): Promise<Topology> {
    const info = await this.client.db(this.options().db).command({ hello: 1 });

    // https://www.mongodb.com/docs/manual/reference/command/hello/#replica-sets
    const hasOplog =
      typeof info.setName !== "undefined" &&
      typeof info.setVersion !== "undefined";

    return {
      hasOplog,
    };
  }

  /** Used internally to identify a queue as a combination of its DB name and collection name */
  protected fqqn() {
    return `${this.options()?.db ?? "docmq"}/${this.name}`;
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
      queue: this.fqqn(),
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
