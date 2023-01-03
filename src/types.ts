import { type DocMQError } from "./error.js";
import EventEmitter from "eventemitter3";

/** A return value that can possibly be wrapped in a promise */
type MaybePromise<T> = T | Promise<T>;
/** Any returnable default, used to describe functions that use return to exit early */
type Returnable = void | null | undefined;
/** Makes all keys in T required */
type RequireKeyed<T, K extends keyof T> = T & { [P in K]-?: T[P] };

/** The default context for jobs when processed */
export type DefaultContext = Record<string, unknown>;

export interface QueueOptions {
  /** Specify alternate retentions for message types */
  retention?: {
    /** Number of seconds to retain processed jobs with no further work, default 3600 (1 hour). DocMQ cleans expired jobs on a regular interval. */
    jobs?: number;
  };
  /**
   * Set an interval to receive statistics via queue.events.on("stat"). Measured in
   * seconds. Defaults to `5`
   */
  statInterval?: number;
}

export interface ProcessorConfig<C> {
  /** Should the processor be paused on creation? If so, no events will be called until you emit a "start" event. */
  pause?: boolean;
  /** The number of concurrent handlers to run, defaults to `5`. Jobs tend to be IO bound, increasing this number allows for more jobs to run in parallel, but at a higher RPU load in serverless environments such as Mongo Atlas */
  concurrency?: number;
  /** A number of seconds that defines the default TTL of a processor */
  visibility?: number;
  /**
   * Set a polling interval for mongo instances as a backup for oplog notifications
   * Ideally, mongo's oplog will notify us when there are new messages. Because this
   * requires an insert operation, a poll is implemented as a fallback. Measured in
   * seconds. Defaults to `5`
   */
  pollInterval?: number;
  createContext?: () => MaybePromise<C>;
}

export interface FixedRetryStrategy {
  /** The type of retry strategy */
  type: "fixed";
  /** The fixed value for every retry */
  amount: number;
  /** The amount of jitter to use when scheduling retries. Defaults to `0` */
  jitter?: number;
}

export interface ExponentialRetryStrategy {
  /** The type of retry strategy */
  type: "exponential";
  /** The minimum amount of time between retry attempts */
  min: number;
  /** Caps the maximum amount of time between retry attempts */
  max: number;
  /** Control the exponential rate of growth such that: `delay = factor ^ (attempt - 1) */
  factor: number;
  /** The amount of jitter to use when scheduling retries. Defaults to `0` */
  jitter?: number;
}

export interface LinearRetryStrategy {
  /** The type of retry strategy */
  type: "linear";
  /** The minimum amount of time between retry attempts */
  min: number;
  /** Caps the maximum amount of time between retry attempts */
  max: number;
  /** Control the linear rate of growht such that: `delay = factor * attempt` */
  factor: number;
  /** The amount of jitter to use when scheduling retries. Defaults to `0` */
  jitter?: number;
}

export type RetryStrategy =
  | FixedRetryStrategy
  | LinearRetryStrategy
  | ExponentialRetryStrategy;

/** The interface used when enqueing one or many jobs */
export interface JobDefinition<T> {
  /** A reference identifier for the job. If not specified, a v4() uuid will be used */
  ref?: string;
  /** The job's payload */
  payload: T;
  /** A date in the future when this job should run, or omit to run immediately */
  runAt?: Date;
  /** An ISO-8601 duration or a cron expression representing how frequently to run the job, or `null` to clear the value */
  runEvery?: string | null;
  /** The optional IANA timezone when scheduling future repeats of the job when using `runEvery` */
  timezone?: string | null;
  /** The number of allowed retries for this job before giving up and assuming the job failed. Defaults to 0 */
  retries?: number;
  /** Specify the retry strategy for the job, defaulting to a fixed retry of 5s */
  retryStrategy?: RetryStrategy;
}

export interface QueueDocRecurrence {
  type: "duration" | "cron";
  value: string;
}

export interface RepeatStrategy {
  /** The number of times this ref has repeated */
  count: number;
  /** Last known enqueue time. When using ISO-8601 durations, this is the time "next" is based on. This exists because `visible` represents the next known time, including retries */
  last?: Date;
  /** Recurrence information, either as an ISO-8601 duration or a cron expression */
  every?: QueueDocRecurrence | null;
  /** The IANA timezone this job should occur in which scheduling future runs via `every`, or `null | undefined` to ignore and use the system's timezone */
  timezone?: string | null;
}

export interface QueueDoc {
  /** A reference ID that helps query related occurences of a job */
  ref: string;
  /** A date describing when this job is available for processing */
  visible: Date;
  /** The ack ID string used for operating on a specific instance of a job */
  ack: string | null | undefined;
  /** A date describing when this job was ended (no further work planned) removing it from future visibility checks */
  deleted?: Date | null;
  /** A boolean indicating if this job was placed into the dead letter queue */
  dead?: boolean;
  /** An optional internal string used for reserving jobs when a DB Driver must separate the update from select */
  reservationId?: string;
  /** If a job is marked dead, this will contain the error information */
  error?: string;
  /** The job's payload. If an object or object-like value is passed, it will be passed through JSON.stringify */
  payload: string | null;
  /** Information on the number of attempts and max allowed */
  attempts: {
    /** The current attempt number */
    tries: number;
    /** The maximum number of attempts allowed before marking the job as `ended` */
    max: number;
    /** The backoff strategy to use. If unspecified, it will use a fixed backoff based on the queue's visibility window  */
    retryStrategy: RetryStrategy;
  };
  /** Information on recurrence of the job */
  repeat: RepeatStrategy;
}

export interface EmitterJob<T = unknown, A = unknown, F = unknown> {
  queue: string;
  ref: string;
  payload?: T;
  attempt: number;
  maxTries: number;
  statusCode?: number;
  result?: A;
  error?: DocMQError | Error | F;
  next?: Date;
}

export type EmitterJobWithPayload<TData, TAck, TFail> = RequireKeyed<
  EmitterJob<TData, TAck, TFail>,
  "payload"
>;

/** DocMQ's EventEmitter makes it easy to attach logging or additional behavior to your workflow */
export type Emitter<
  TData,
  TAck,
  TFail extends Error = Error,
  TContext = DefaultContext
> = EventEmitter<{
  /** Triggered when the Processor loop goes idle, meaning 0 jobs are currently in-process */
  idle: () => void;
  /** A debug message with additional logging details */
  debug: (message: string, ...details: unknown[]) => void;
  /** A log-level message */
  log: (message: string) => void;
  /** A warning-level message */
  warn: (warning: string | DocMQError) => void;
  /** Occurs when an error / exception is triggered within DocMQ */
  error: (error: DocMQError) => void;
  /** Occurs when an unrecoverable error is triggered within DocMQ and no further processing can occur */
  halt: (error: DocMQError) => void;
  /** The processor is starting */
  start: () => void;
  /** The processor is stopping */
  stop: () => void;
  /** A set of jobs was added to the queue */
  add: (jobs: JobDefinition<TData>[]) => void;
  /** A job was pulled for processing */
  process: (info: EmitterJob<TData, TAck, TFail>) => void;
  /** A job was completed successfully */
  ack: (
    info: EmitterJobWithPayload<TData, TAck, TFail>,
    context: TContext
  ) => void;
  /** A job has failed one of its execution attempts */
  fail: (info: EmitterJob<TData, TAck, TFail>, context: TContext) => void;
  /** A job has failed all of its execution attempts */
  dead: (info: EmitterJob<TData, TAck, TFail>, context: TContext) => void;
  /** A job asked to extend its visibility window */
  ping: (
    info: EmitterJob<TData, TAck, TFail>,
    extendBy: number,
    context: TContext
  ) => void;
  /** A report of statistics for this queue */
  stats: (stats: QueueStats & { queue: string }) => void;
}>;

export interface ProcessAPI {
  /** If a queue is paused, this will restart processing of the queue, running the currently associated processor */
  start: () => void;
  /** If a queue is running, this will prevent future jobs from being consumed by the processor. Existing jobs in-flight are allowed to conclude */
  stop: () => void;
}

export type MiddlewareFunction<T> = (value: T) => void | Promise<void>;

export interface FailureRetryOptions {
  /** If specified, the visibility window will be shifted to after this date */
  after?: Date;
  /** If specified, the current attempt number will be shifted to the specified value */
  attempt?: number;
}

export interface HandlerApi<
  TAck = unknown,
  TFail extends Error = Error,
  TContext = DefaultContext
> {
  /** The reference value for the job */
  ref: string;
  /** The number of attempts made for this job */
  attempt: number;
  /** How long (seconds) the Job was initially reserved for */
  visible: number;
  /** The current context for this execution */
  context: TContext;
  /** Acknowledge "ack" the job, marking it as successfully handled */
  ack: (result?: TAck) => Promise<void>;
  /** Fail the job, triggering any requeue/rescheduling logic */
  fail: (
    error: DocMQError | TFail | string,
    retryOptions?: FailureRetryOptions
  ) => Promise<void>;
  /** Request to extend the running time for the current job */
  ping: (extendBy: number) => Promise<void>;
}

export type JobHandler<
  TData,
  TAck = unknown,
  TFail extends Error = Error,
  TContext = DefaultContext
> = (
  payload: TData,
  api: HandlerApi<TAck, TFail, TContext>
) => Promise<unknown>;

/** The DriverEmitter controls events related to the handling of the DB driver */
export type DriverEmitter = EventEmitter<{
  /** Triggered when new data arrives */
  data: () => void;
  /** Triggered on an internal Driver warning */
  warn: (error: DocMQError) => void;
  /** Triggered on an internal Driver Error */
  error: (error: DocMQError) => void;
  /** Triggered on an unrecoverable Driver Error */
  halt: (error: DocMQError) => void;
  /** Triggered when a driver needs to reconnect if using a persistent connection */
  reconnect: () => void;
}>;

/** A set of options that are passed to a DB Driver */
export interface DriverOptions {
  /** Specifies the DB schema or Document DB to use */
  schema?: string;
  /** Specifies the DB table or Document DB Collection to use */
  table?: string;
}

/** Describes a DB Driver for DocMQ */
export interface Driver<Schema = unknown, Table = unknown, TxInfo = unknown> {
  /** An event emitter for driver-related events */
  events: DriverEmitter;
  /** Returns the name of the requested schema */
  getSchemaName(): string;
  /** Returns the name of the requested table */
  getTableName(): string;
  /** Returns the schema object, ORM, or the schema name. Driver dependent. */
  getSchema(): MaybePromise<Schema>;
  /** Returns the table object, ORM, or the table name. Driver dependent. */
  getTable(): MaybePromise<Table>;
  /** Returns a promise that resolves to `true` when all initialization steps are complete */
  ready(): Promise<boolean>;
  /** Begins a transaction, executing the contents of the body inside of the transaction */
  transaction(body: (txn: TxInfo) => Promise<unknown>): Promise<Returnable>;
  /** Takes one or more upcoming jobs and locks them for exclusive use */
  take(visibility: number, limit?: number, tx?: TxInfo): Promise<QueueDoc[]>;
  /** Acknowledges a job, marking it completed */
  ack(ack: string, tx?: TxInfo): Promise<Returnable>;
  /** Fails a job, adjusting the job to retry in an expected timeframe */
  fail(
    ack: string,
    retryIn: number,
    attempt: number,
    tx?: TxInfo
  ): Promise<Returnable>;
  /** Moves a job to the dead letter queue and acks it */
  dead(doc: QueueDoc, tx?: TxInfo): Promise<Returnable>;
  /** Extends the runtime of a job by the requested amount */
  ping(ack: string, extendBy?: number, tx?: TxInfo): Promise<Returnable>;
  /** Promote a job, making it immediately visible */
  promote(ref: string, tx?: TxInfo): Promise<Returnable>;
  /** Delay a job, making it visible much later */
  delay(ref: string, delayBy: number, tx?: TxInfo): Promise<Returnable>;
  /** Replay a job, cloning it and making the new one run immediately */
  replay(ref: string, tx?: TxInfo): Promise<Returnable>;
  /** Cleans up old and completed jobs in the system, ran periodically */
  clean(before: Date, tx?: TxInfo): Promise<Returnable>;
  /** Replace all upcoming instances of a job with a new definition */
  replaceUpcoming(doc: QueueDoc, tx?: TxInfo): Promise<QueueDoc>;
  /** Remove all upcoming instances of a job */
  removeUpcoming(ref: string, tx?: TxInfo): Promise<Returnable>;
  /** Finds the next occurence of a job, either by cron or ISO-8601 duration */
  findNext(doc: QueueDoc): Date | undefined;
  /** Create and insert the next occurence of a job */
  createNext(doc: QueueDoc): Promise<Returnable>;
  /** Enables any listeners for drivers that support pub/sub design */
  listen(): Promise<Returnable>;
  /** Destroy the driver and close connections */
  destroy(): Returnable;
}

export interface WorkerOptions<
  TData,
  TAck,
  TFail extends Error = Error,
  TContext = DefaultContext
> {
  driver: Driver;
  name: string;
  doc: QueueDoc;
  payload: TData;
  handler: JobHandler<TData, TAck, TFail, TContext>;
  emitter: Emitter<TData, TAck, TFail, TContext>;
  visibility: number;
  createContext?: () => MaybePromise<TContext>;
}

export interface QueueStats {
  start: Date;
  end: Date;
  enqueued: number;
  processed: number;
  outcomes: {
    success: number;
    failure: number;
  };
  errors: Record<string, number>;
}
