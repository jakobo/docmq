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
 * Raised when processing begins. If the mongodb instance is not a ReplicaSet,
 * then it's impossible to run DocMQ safely; transactional support is required
 * in order to perform the ack + createNext operation pair. Once raised, this
 * queue will not process jobs.
 *
 * Emitted via `.on("error"...)`
 */
export class NonReplicatedMongoInstanceError extends DocMQError {
  type = "NonReplicatedMongoInstanceError";
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
 * connection to the Document database.
 *
 * Emitted via `.on("error"...)`
 */
export class WorkerAPIError extends DocMQError {
  type = "WorkerAPIError";
  api = "unknown";
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
