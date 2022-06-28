import test from "ava";
import {
  exponentialBackoff,
  fixedBackoff,
  linearBackoff,
} from "../src/backoff.js";
import {
  ExponentialRetryStrategy,
  FixedRetryStrategy,
  LinearRetryStrategy,
} from "../src/types.js";

test("fixed backoff - 5, 5, 5, 5, 5", (t) => {
  const strategy: FixedRetryStrategy = {
    type: "fixed",
    amount: 5,
    jitter: 0,
  };

  const actual = [
    fixedBackoff(strategy),
    fixedBackoff(strategy),
    fixedBackoff(strategy),
    fixedBackoff(strategy),
    fixedBackoff(strategy),
  ];

  const expected = [5, 5, 5, 5, 5];

  t.deepEqual(actual, expected);
});

test("exponential backoff - 1, 2, 4, 8, 16", (t) => {
  const strategy: ExponentialRetryStrategy = {
    type: "exponential",
    min: 0,
    max: 999999,
    jitter: 0,
    factor: 2,
  };

  const actual = [
    exponentialBackoff(strategy, 1),
    exponentialBackoff(strategy, 2),
    exponentialBackoff(strategy, 3),
    exponentialBackoff(strategy, 4),
    exponentialBackoff(strategy, 5),
  ];

  const expected = [1, 2, 4, 8, 16];

  t.deepEqual(actual, expected);
});

test("linear backoff - 2, 4, 6, 8, 10", (t) => {
  const strategy: LinearRetryStrategy = {
    type: "linear",
    min: 0,
    max: 999999,
    jitter: 0,
    factor: 2,
  };

  const actual = [
    linearBackoff(strategy, 1),
    linearBackoff(strategy, 2),
    linearBackoff(strategy, 3),
    linearBackoff(strategy, 4),
    linearBackoff(strategy, 5),
  ];

  const expected = [2, 4, 6, 8, 10];

  t.deepEqual(actual, expected);
});
