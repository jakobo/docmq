export class DocMQError extends Error {
  original: Error | undefined;
  type = "DocMqError";
  constructor(message: string) {
    super(message);
  }
}

export class MaxAttemptsExceededError extends DocMQError {
  type = "MaxAttemptsExceededError";
}

export class UncaughtHandlerError extends DocMQError {
  type = "UncaughtHandlerError";
}

export class UnAckedHandlerError extends DocMQError {
  type = "UnAckedHandlerError";
}

export class NonReplicatedMongoInstanceError extends DocMQError {
  type = "NonReplicatedMongoInstanceError";
}

export class ProcessorError extends DocMQError {
  type = "ProcessorError";
}

export class UnknownWorkerError extends DocMQError {
  type = "UnknownWorkerError";
}

export class UnknownError extends DocMQError {
  type = "UnknownError";
}

export class WorkerAPIError extends DocMQError {
  type = "WorkerAPIError";
  api = "unknown";
}

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
