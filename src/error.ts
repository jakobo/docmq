/**
 * The base DocMQ Error. All errors in DocMQ inherit from this object.
 * To help with disambiguation, a `type` property is also set for users
 * needing a custom descriminator.
 */
export class DocMQError extends Error {
  original: Error | undefined;
  type = "DocMQError";
  constructor(message: string) {
    super(message);
  }
}

/**
 * Raised when an error is encountered during the enqueue() or enqueueMany()
 * process. `jobs` will contain a list of job objects that failed to enqueue
 *
 * Emitted via `.on("error"...)`
 */
export class EnqueueError extends DocMQError {
  type = "EnqueueError";
  jobs: unknown[] = [];
  errors: unknown[] = [];
}

/**
 * Raised when a job has exceeded the maximum number of attempts. This is an
 * informational error.
 *
 * Emitted via `.on("dead"...)`
 */
export class MaxAttemptsExceededError extends DocMQError {
  type = "MaxAttemptsExceededError";
}

/**
 * Raised when a handler function throws an uncaught exception. These errors
 * are intercepted by DocMQ to avoid a process.exit.
 *
 * Emitted via `.on("error"...)`
 */
export class UncaughtHandlerError extends DocMQError {
  type = "UncaughtHandlerError";
}

/**
 * Raised when a process function never called ack() before returning.
 * This most commonly represents an error in your code, as the job will
 * run again after the visibility window lapses.
 *
 * Emitted via `.on("error"...)`
 */
export class UnAckedHandlerError extends DocMQError {
  type = "UnAckedHandlerError";
}

/**
 * An error that represents a generic error within the DocMQ processor.
 * If raised, it usually means there's a bug in DocMQ. Raise an issue at
 * https://github.com/jakobo/docmq
 */
export class ProcessorError extends DocMQError {
  type = "ProcessorError";
}

/**
 * An error that represents a generic error within the DocMQ worker.
 * If raised, it usually means there's a bug in DocMQ. Raise an issue at
 * https://github.com/jakobo/docmq
 */
export class UnknownWorkerError extends DocMQError {
  type = "UnknownWorkerError";
}

/**
 * An error that represents a generic error within the DocMQ.
 * If raised, it usually means there's a bug in DocMQ. Raise an issue at
 * https://github.com/jakobo/docmq
 */
export class UnknownError extends DocMQError {
  type = "UnknownError";
}

/**
 * Raised when calling an api method (`api.ack()`, `api.ping()`, etc) fails
 * for an unknown reason. Most commonly, this means the server has lost its
 * connection to the Document database. `.api` will contain the API invoked
 * when this error was called.
 */
export class WorkerAPIError extends DocMQError {
  type = "WorkerAPIError";
  api = "unknown";
  ref = "unknown";
}

/** The worker encountered an error during processing */
export class WorkerProcessingError extends DocMQError {
  type = "WorkerProcessingError";
}

/**
 * A generic driver error for an unknown cause. Driver errors extend from this,
 * making it easy to separate driver errors from other DocMQ errors when handling
 * DocMQs `error` callback
 */
export class DriverError extends DocMQError {
  type = "DriverError";
}

/** Describes calling a feature that is not implemented in the driver */
export class DriverNotImplementedError extends DriverError {
  type = "DriverNotImplementedError";
  constructor() {
    super(
      "DocMQ attempted to call a method on the driver that is not implemented"
    );
  }
}

/** Thrown when a driver method is called before the driver is initialized */
export class DriverInitializationError extends DriverError {
  type = "DriverInitializationError";
  constructor() {
    super("A driver message was called before it completed its intialization");
  }
}

/**
 * Thrown when there is no matching Ref when attempting to ack/ping/fail/etc
 */
export class DriverNoMatchingAckError extends DriverError {
  type = "DriverNoMatchingAckError";
  ack = "unknown";
  constructor(message: string) {
    super(`Unable to find a suitable record matching ack: ${message}`);
    this.ack = message;
  }
}

/**
 * Thrown when there is no matching Ref when attempting to ack/ping/fail/etc
 */
export class DriverNoMatchingRefError extends DriverError {
  type = "DriverNoMatchingRefError";
  ref = "unknown";
  constructor(message: string) {
    super(`Unable to find a suitable record matching ref: ${message}`);
    this.ref = message;
  }
}

/**
 * A connection problem reported by the driver
 */
export class DriverConnectionError extends DriverError {
  type = "DriverError";
}

/** Casts an object into an error from a few well-known variations */
export const asError = (e: unknown): Error => {
  try {
    return e instanceof Error
      ? e
      : typeof e === "string"
      ? new Error(e)
      : new Error(JSON.stringify(e));
  } catch {
    return new UnknownError("An unknown error occured");
  }
};
