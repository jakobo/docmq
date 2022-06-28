import {
  ExponentialRetryStrategy,
  FixedRetryStrategy,
  LinearRetryStrategy,
} from "./types.js";

/** Create an exponential backoff based on a set of options & number of attempts */
export const exponentialBackoff = (
  opts: ExponentialRetryStrategy,
  attempt: number
) => {
  const delay = opts.min + Math.pow(opts.factor, attempt - 1);
  const jit = Math.round(Math.random() * (opts.jitter ?? 0));
  return Math.min(delay + jit, opts.max);
};

/** Create a linear backoff based on a set of options & number of attempts */
export const linearBackoff = (opts: LinearRetryStrategy, attempt: number) => {
  const delay = opts.min + opts.factor * attempt;
  const jit = Math.round(Math.random() * (opts.jitter ?? 0));
  return Math.min(delay + jit, opts.max);
};

/** Create a fixed backoff based on a set of options */
// eslint-disable-next-line @typescript-eslint/no-unused-vars
export const fixedBackoff = (opts: FixedRetryStrategy) => {
  const jit = Math.round(Math.random() * (opts.jitter ?? 0));
  return opts.amount + jit;
};
