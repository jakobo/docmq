export class DocqueueError extends Error {
  original: Error | undefined;
}

export class MaxAttemptsExceededError extends DocqueueError {}
export class UncaughtHandlerError extends DocqueueError {}
export class UnAckedHandlerError extends DocqueueError {}
export class NonReplicatedMongoInstanceError extends DocqueueError {}
export class ProcessorError extends DocqueueError {}
export class UnknownWorkerError extends DocqueueError {}
export class UnknownError extends DocqueueError {}

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
