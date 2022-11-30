import test from "ava";
import { DateTime } from "luxon";
import { v4 } from "uuid";
import { BaseDriver } from "../src/index.js";
import { Queue } from "../src/queue.js";
import { QueueDoc } from "../src/types.js";

const IANA_PHX = "America/Phoenix";
const IANA_LA = "America/Los_Angeles";

const laxTime = DateTime.local()
  .setZone(IANA_LA)
  .set({
    month: 11,
    day: 5,
    year: 2022,
    hour: 4,
  })
  .startOf("hour");

const phxTime = DateTime.local()
  .setZone(IANA_PHX)
  .set({
    month: 11,
    day: 5,
    year: 2022,
    hour: 4,
  })
  .startOf("hour");

const seedTime = laxTime.minus({ days: 3 });

const makeIsoDoc = (time: DateTime, zone: string): QueueDoc => {
  return {
    ref: v4(),
    visible: time.toJSDate(),
    ack: v4(),
    attempts: {
      tries: 0,
      max: 999,
      retryStrategy: {
        type: "fixed",
        amount: 5,
      },
    },
    repeat: {
      count: 0,
      every: {
        type: "duration",
        value: "P1D",
      },
      timezone: zone,
      last: time.toJSDate(),
    },
    payload: Queue.encodePayload("sample"),
  };
};

const makeCronDoc = (time: DateTime, zone: string): QueueDoc => {
  const base = makeIsoDoc(time, zone);
  return {
    ...base,
    repeat: {
      ...base.repeat,
      every: {
        type: "cron",
        value: "0 4 */1 * *", // At 04:00 on every day-of-month.
      },
    },
  };
};

test("findNext with ISO duration and Los Angeles (DST)", (t) => {
  const time = laxTime;
  const doc = makeIsoDoc(time, IANA_LA);
  const d = new BaseDriver("none");

  t.is(
    d.findNext(doc, seedTime.toJSDate())?.getTime(),
    time.plus({ days: 1 }).toJSDate().getTime(),
    "Respects change in DST"
  );

  const diff =
    ((d.findNext(doc, seedTime.toJSDate())?.getTime() ?? 0) -
      time.toJSDate().getTime()) /
    1000;
  t.is(diff, 90000, "is 25 hours because of DST");
});

test("findNext with cron duration and Los Angeles (DST)", (t) => {
  const time = laxTime;
  const doc = makeCronDoc(time, IANA_LA);
  const d = new BaseDriver("none");

  t.is(
    d.findNext(doc, seedTime.toJSDate())?.getTime(),
    time.plus({ days: 1 }).toJSDate().getTime(),
    "Respects change in DST"
  );

  const diff =
    ((d.findNext(doc, seedTime.toJSDate())?.getTime() ?? 0) -
      time.toJSDate().getTime()) /
    1000;
  t.is(diff, 90000, "is 25 hours because of DST");
});

test("findNext with ISO duration and Phoenix time (no DST)", (t) => {
  const time = phxTime;
  const doc = makeIsoDoc(time, IANA_PHX);
  const d = new BaseDriver("none");

  t.is(
    d.findNext(doc, seedTime.toJSDate())?.getTime(),
    time.plus({ days: 1 }).toJSDate().getTime(),
    "Does not attempt to apply DST changes"
  );

  const diff =
    ((d.findNext(doc, seedTime.toJSDate())?.getTime() ?? 0) -
      time.toJSDate().getTime()) /
    1000;
  t.is(diff, 86400, "is 24 hours because there is no DST");
});

test("findNext with cron duration and Phoenix time (no DST)", (t) => {
  const time = phxTime;
  const doc = makeCronDoc(time, IANA_PHX);
  const d = new BaseDriver("none");

  t.is(
    d.findNext(doc, seedTime.toJSDate())?.getTime(),
    time.plus({ days: 1 }).toJSDate().getTime(),
    "Does not attempt to apply DST changes"
  );

  const diff =
    ((d.findNext(doc, seedTime.toJSDate())?.getTime() ?? 0) -
      time.toJSDate().getTime()) /
    1000;
  t.is(diff, 86400, "is 24 hours because there is no DST");
});
