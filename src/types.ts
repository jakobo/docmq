import { type DocMQError } from "./error.js";
import { type ClientSession, type WithId, type Collection } from "mongodb";
import type TypedEventEmitter from "typed-emitter";

type MaybePromise<T> = T | Promise<T>;

export interface Topology {
  hasOplog: boolean;
}

export interface QueueOptions {
  /** A name to use for the document db, defaults to "docmq" */
  db?: string;
  /** Specify alternate retentions for message types */
  retention?: {
    /** Number of seconds to retain processed jobs with no further work, default 86400 (1 day). DocMQ cleans expired jobs on a regular interval. */
    jobs?: number;
    /** Number of runs to retain in the job history document, default 1 */
    runs?: number;
  };
}

export interface ProcessorConfig {
  /** Should the processor be paused on creation? If so, no events will be called until you emit a "start" event. */
  pause?: boolean;
  /** The number of concurrent handlers to run, defaults to `5`. Jobs tend to be IO bound, increasing this number allows for more jobs to run in parallel, but at a higher RPU load in serverless environments such as Mongo Atlas */
  concurrency?: number;
  /** A number of seconds that defines the default TTL of a processor */
  visibility?: number;
  /**
   * Set a polling interval for mongo instances as a backup for oplog notifications
   * Ideally, mongo's oplog will notify us when there are new messages. Because this
   * requires an insert operation, a poll is implemented as a fallback. Defaults to
   * 5000Ms
   */
  pollIntervalMs?: number;
}

export interface EnqueueJob {
  /** A reference identifier for the job. If not specified, a v4() uuid will be used */
  ref?: string;
  /** A date in the future when this job should run, or omit to run immediately */
  runAt?: Date;
  /** An ISO-8601 duration or a cron expression representing how frequently to run the job */
  runEvery?: string;
  /** The number of allowed retries for this job before giving up and assuming the job failed. Defaults to 0 */
  retries?: number;
}

export interface BulkEnqueueJob<T = unknown> extends EnqueueJob {
  /** The job's payload */
  payload: T;
}

export enum RecurrenceEnum {
  duration = "duration",
  cron = "cron",
}

export interface QueueDocRecurrence {
  type: RecurrenceEnum;
  value: string;
}

export interface QueueDoc {
  /** A date describing when this job is available for processing */
  visible: Date;
  /** A date describing when this job was ended (no further work planned) removing it from future visibility checks */
  deleted?: Date | null;
  /** A reference ID that helps query related occurences of a job */
  ref: string;
  /** The ack ID string used for operating on a specific instance of a job */
  ack?: string;
  /** The job's payload. If an object or object-like value is passed, it will be passed through JSON.stringify */
  payload: string;
  /** Information on the number of attempts and max allowed */
  attempts: {
    /** The current attempt number */
    tries: number;
    /** The maximum number of attempts allowed before marking the job as `ended` */
    max: number;
  };
  /** Information on recurrence of the job */
  repeat: {
    /** The number of times this ref has repeated */
    count: number;
    /** Last known enqueue time. When using ISO-8601 durations, this is the time "next" is based on. This exists because `visible` represents the next known time, including retries */
    last?: Date;
    /** Recurrence information, either as an ISO-8601 duration or a cron expression */
    every?: QueueDocRecurrence;
  };
}

export interface DeadQueueDoc extends QueueDoc {
  /** Contains information about the error encountered */
  error: unknown;
}

export interface ConfigDoc {
  version: number;
  config: string | null;
}

export interface Collections {
  jobs: Collection<QueueDoc>;
  deadLetterQueue: Collection<DeadQueueDoc>;
  config: Collection<ConfigDoc>;
}

/** DocMQ's EventEmitter makes it easy to attach logging or additional behavior to your workflow */
export type Emitter = TypedEventEmitter<{
  /** Triggered when the Processor loop goes idle, meaning 0 jobs are currently in-process */
  idle: () => MaybePromise<void>;
  /** A debug message with additional logging details */
  debug: (message: string, ...details: unknown[]) => MaybePromise<void>;
  /** A log-level message */
  log: (message: string) => MaybePromise<void>;
  /** A warning-level message */
  warn: (message: string) => MaybePromise<void>;
  /** Occurs when an error / exception is triggered within DocMQ */
  error: (error: DocMQError) => MaybePromise<void>;
  /** The processor should be started if possible */
  start: () => MaybePromise<void>;
  /** The processor should be stopped */
  stop: () => MaybePromise<void>;
  /** A job was completed successfully */
  ack: <T = unknown, U = unknown>(
    ref: string,
    result: T,
    payload: U
  ) => MaybePromise<void>;
  /** A job has failed one of its execution attempts */
  fail: <T = unknown, U = unknown>(
    ref: string,
    result: T,
    payload: U,
    attempt: number,
    max: number
  ) => MaybePromise<void>;
  /** A job has failed all of its execution attempts */
  dead: (ref: string) => MaybePromise<void>;
  /** A job asked to extend its visibility window */
  ping: (ref: string, extendBy: number) => MaybePromise<void>;
}>;

export interface HandlerApi {
  /** The reference value for the job */
  ref: string;
  /** Acknowledge "ack" the job, marking it as successfully handled */
  ack: (result?: unknown) => Promise<void>;
  /** Fail the job, triggering any requeue/rescheduling logic */
  fail: (result?: unknown) => Promise<void>;
  /** Request to extend the running time for the current job */
  ping: (extendBy: number) => Promise<void>;
}

export type JobHandler<T = unknown> = (
  payload: T,
  api: HandlerApi
) => Promise<unknown>;

export interface WorkerOptions<T> {
  session: ClientSession;
  collections: Collections;
  doc: WithId<QueueDoc>;
  handler: JobHandler<T>;
  emitter: Emitter;
  visibility: number;
}
