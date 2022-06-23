import { take } from "./mongo/functions.js";
import { MongoClient } from "mongodb";
import { v4 } from "uuid";
import { updateIndexes } from "./mongo/util.js";
import {
  RecurrenceEnum,
  type Collections,
  type ConfigDoc,
  type QueueDocRecurrence,
  type DeadQueueDoc,
  type EnqueueJob,
  type JobHandler,
  type ProcessorConfig,
  type QueueDoc,
  type QueueOptions,
  type Topology,
  type Emitter,
  BulkEnqueueJob,
} from "./types.js";
import { Worker } from "./worker.js";
import { EventEmitter } from "events";
import { Duration } from "luxon";
import cron from "cron-parser";
import {
  asError,
  NonReplicatedMongoInstanceError,
  ProcessorError,
  UnknownWorkerError,
} from "./error.js";

const T_30_SECONDS = 30;
const noop = () => {};

export class Queue<T> {
  events: Readonly<Emitter>;

  protected name: string;
  protected client: MongoClient;
  protected options: QueueOptions | undefined;
  protected workers: Worker<T>[];
  protected destroyed: boolean;
  protected topology: Promise<Topology>;
  protected indexesReady: Promise<boolean>;

  constructor(url: string, name: string, options?: QueueOptions) {
    this.name = name;
    this.destroyed = false;
    this.workers = [];
    this.client = new MongoClient(url);
    this.events = new EventEmitter() as Emitter;
    this.options = options;

    // store promises for concurrent tasks
    // these are idempotent, but required for our queue to work
    this.topology = this.determineTopology();
    this.indexesReady = updateIndexes(this.collections());
  }

  protected ready() {
    return Promise.all([this.topology, this.indexesReady]);
  }

  protected db() {
    return this.client.db(this.options?.db ?? "docqueue");
  }

  protected collections(): Collections {
    return {
      jobs: this.db().collection<QueueDoc>(this.name),
      deadLetterQueue: this.db().collection<DeadQueueDoc>(
        `${this.name}/failed`
      ),
      config: this.db().collection<ConfigDoc>(`${this.name}/config`),
    };
  }

  async enqueue(payload: T, options?: EnqueueJob) {
    return this.enqueueMany([
      {
        ...(options ?? {}),
        payload,
      },
    ]);
  }

  async enqueueMany(bulkJobs: BulkEnqueueJob<T>[]) {
    if (this.destroyed) {
      throw new Error("Will not enqueue into a destroyed object");
    }

    // wait for ready
    await this.ready();
    const topology = await this.topology;
    if (!topology.hasOplog) {
      const err = new NonReplicatedMongoInstanceError(
        "Docqueue requires an oplog in order to gaurentee events such as scheduling future work"
      );
      this.events.emit("error", err);
      throw err;
    }

    const jobs = bulkJobs.map((v) => {
      let begin = v.runAt ?? new Date();
      let runEvery: QueueDocRecurrence | undefined;

      if (v.runEvery) {
        // check for duration first as its faster
        const d = Duration.fromISO(v.runEvery);
        if (d.isValid) {
          runEvery = {
            type: RecurrenceEnum.duration,
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
              type: RecurrenceEnum.cron,
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

      const doc: QueueDoc = {
        ref: v.ref ?? v4(),
        visible: begin,
        payload: JSON.stringify({
          _: v.payload,
        }),
        attempts: {
          tries: 0,
          max: v.retries === 0 ? 0 : v.retries ?? 5,
        },
        repeat: {
          count: 0,
          last: begin,
          every: runEvery,
        },
      };
      return doc;
    });

    await this.collections().jobs.insertMany(jobs);
  }

  async determineTopology(): Promise<Topology> {
    const info = await this.db().command({ hello: 1 });

    // https://www.mongodb.com/docs/manual/reference/command/hello/#replica-sets
    const hasOplog =
      typeof info.setName !== "undefined" &&
      typeof info.setVersion !== "undefined";

    return {
      hasOplog,
    };
  }

  async get(handler: JobHandler<T>, config?: ProcessorConfig) {
    if (this.destroyed) {
      throw new Error("Cannot process a destroyed queue");
    }
    const visibility =
      config?.visibility === 0
        ? config.visibility
        : config?.visibility || T_30_SECONDS;
    const next = await take(this.collections().jobs, visibility, 1);
    if (next.length === 0) {
      return;
    }
    const w = new Worker<T>({
      session: this.client.startSession(),
      collections: this.collections(),
      doc: next[0],
      handler,
      emitter: this.events,
      visibility,
    });
    try {
      await w.processOne();
    } catch (e) {
      const err = new UnknownWorkerError("An unknown worker error occurred");
      err.original = asError(e);
      this.events.emit("error", err);
    }
  }

  process(handler: JobHandler<T>, config?: ProcessorConfig) {
    if (this.destroyed) {
      throw new Error("Cannot process a destroyed queue");
    }

    let idle = true;
    let started = false;
    let paused = config?.pause === true ? true : false;
    const concurrency = Math.max(config?.concurrency ?? 1, 1);
    const visibility =
      config?.visibility === 0
        ? config.visibility
        : config?.visibility || T_30_SECONDS;
    const pollInterval =
      typeof config?.pollIntervalMs === "number" && config.pollIntervalMs > 0
        ? config.pollIntervalMs
        : 5000;

    const isPaused = () => paused;

    /**
     * Takes the next N items and schedules their work. Ensures only
     * one async operation is running at time manipulating this.workers
     */
    const takeAndProcess = async () => {
      if (!idle || paused || this.destroyed) {
        return;
      }
      idle = false;

      // concurrency - max concurrency - current workers
      const limit = concurrency - this.workers.length;

      const next = await take(this.collections().jobs, visibility, limit);
      this.events.emit("log", `Received ${next.length} jobs`);
      next.forEach((doc) => {
        const w = new Worker<T>({
          session: this.client.startSession(),
          collections: this.collections(),
          doc,
          handler,
          emitter: this.events,
          visibility,
        });
        this.workers.push(w);
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
          });
      });

      idle = true;
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
            "Docqueue requires Mongo replication to be enabled (even if a cluster size of 1) for oplog functionality."
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

  start() {
    this.events.emit("start");
  }

  stop() {
    this.events.emit("stop");
  }

  destroy() {
    this.destroyed = true; // hard stop all activity
    this.events.emit("stop");
    this.workers?.forEach((w) => w.destroy());
    this.events.removeAllListeners();
    this.events.removeAllListeners();
  }
}

/** Sleep for a pre-determined amount of time in Ms */
const sleep = (durationMs: number) =>
  new Promise((resolve) => {
    setTimeout(resolve, durationMs);
  });
