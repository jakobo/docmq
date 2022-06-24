export class DocMQError extends Error {
  original: Error | undefined;
}

export class MaxAttemptsExceededError extends DocMQError {}
export class UncaughtHandlerError extends DocMQError {}
export class UnAckedHandlerError extends DocMQError {}
export class NonReplicatedMongoInstanceError extends DocMQError {}
export class ProcessorError extends DocMQError {}
export class UnknownWorkerError extends DocMQError {}
export class UnknownError extends DocMQError {}

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
