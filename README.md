<p align="center">
  <img src="https://cdn.jsdelivr.net/gh/jakobo/docmq@main/docmq.svg" alt="DocMQ" width="100" height="100"><br/>
  <span style="font-size:24px"><strong>DocMQ</strong></span><br/>
  <sub><i>DB Backed Messaging Queue</i></sub>
</p>

- [📚 Documentation](#-documentation)
- [🔧 Custom Driver Support](#-custom-driver-support)
- [⚖️ License (MIT)](#-license)
- [❤️ Sponsor](#-sponsor)
- [✨ Contributors](#-contributors)

> **Why Choose This** :grey_question:
>
> DocMQ is a good choice if a persistance layer for your messaging queue is a deciding factor. Backed by a database, DocMQ makes it easy to schedule, query, and debug jobs using an adapter layer tuned for your database type. This also allows DocMQ to choose the _no database_ option and run in-memory for local environments where it's unnecessary to set up a local mongo / postgres / etc. DocMQ works with anything that holds and queries documents or document-like objects.
>
> DocMQ is also a strong choice when your messaging queue needs to care about time and time zones. When you want to "send a message at 4:00 am", it matters if you mean 4am in Los Angeles or 4am in Phoenix because only one of those locations implements Daylight Savings Time. DocMQ reduces the pain associated with figuring out if one day is `86400`, `90000`, or `85800` seconds in the future.
>
> **Why AVOID This** :grey_question:
>
> Simple. Performance. This kind of solution will never be as fast as an in-memory Redis queue or an event bus. If fast FIFO is your goal, you should consider BullMQ, Kue, Bee, Owl, and others.

# DocMQ

- An adaptable DB layer with ready-made drivers for MongoDB and Postgres
- An in-memory driver for faster local development
- Concurrent job execution, job repitition, scheduling, and persistence of results
- Queue-level hooks for managing side effects of job completions and failures
- Timezone aware scheduling and recurrence

**Comparison by Feature**

| Feature            | [BullMQ](https://github.com/taskforcesh/bullmq) | [Agenda](https://github.com/agenda/agenda) | [DocMQ](https://github.com/jakobo/docmq) |
| :----------------- | :---------------------------------------------: | :----------------------------------------: | :--------------------------------------: |
| Backend            |                      redis                      |                   mongo                    |                   any                    |
| Parent/Child       |                        ✓                        |                                            |                                          |
| Priorities         |                        ✓                        |                     ✓                      |                                          |
| Concurrency        |                        ✓                        |                     ✓                      |                    ✓                     |
| Delayed jobs       |                        ✓                        |                     ✓                      |                    ✓                     |
| Global events      |                        ✓                        |                                            |                                          |
| Rate Limiter       |                        ✓                        |                                            |                                          |
| Pause/Resume       |                        ✓                        |                                            |                    ✓                     |
| Sandboxed worker   |                        ✓                        |                                            |                                          |
| Repeatable jobs    |                        ✓                        |                     ✓                      |                    ✓                     |
| Atomic ops         |                        ✓                        |                                            |                    ✓                     |
| Persistence        |                        ✓                        |                     ✓                      |                    ✓                     |
| UI                 |                        ✓                        |                     ✓                      |                                          |
| REST API           |                                                 |                     ✓                      |                                          |
| Run In-memory      |                                                 |                                            |                    ✓                     |
| Timezone awareness |                                                 |                                            |                    ✓                     |
| New job polling    |                        ✓                        |                     ✓                      |                    ✓                     |
| Queue subscription |                        ✓                        |                                            |                    ✓                     |
| Optimized for      |                 Jobs / Messages                 |                    Jobs                    |                   Jobs                   |

If you're not concerned about timezone awareness or an in-memory driver, BullMQ and Agenda are excellent alternatives. _Thank you [BullMQ](https://github.com/taskforcesh/bullmq#feature-comparison) for making the original comparison chart_

:warning: **EARLY DEVELOPMENT** - This software is currently used in Production as part of [Taskless](https://taskless.io), but is still a newer project. This project follows semver spec for versions < 1.

```
0.y.x
  ^ ^- fixes, features
   \-- breaking changes
```

## Installation

```sh
# npm
npm i docmq

# yarn
yarn add docmq

# pnpm
pnpm add docmq
```

DocMQ comes with an in-memory driver `MemoryDriver`, along with [several other adapters](https://github.com/jakobo/docmq/tree/main/src/driver) for various DBs.

## 📚 Documentation

### Creating a Queue

```ts
import { Queue, MemoryDriver } from "docmq";

interface SimpleJob {
  success: boolean;
}

const queue = new Queue<SimpleJob>(new MemoryDriver("default"), "docmq");
```

#### `new Queue()` options

`new Queue<T>(driver: Driver, name: string, options?: QueueOptions)`

- `driver` a Driver implementation to use such as the `MemoryDriver`
- `name` a string for the queue's name
- `options?` additional options
  - `retention.jobs?` number of seconds to retain jobs with no further work. Default `3600` (1 hour)
  - `statInterval?` number of seconds between emitting a `stat` event with queue statistics, defaults to `5`

### Adding a Job to the Queue

```ts
await queue.enqueue({
  ref: "sample-id",
  /* SimpleJob */ payload: {
    success: true,
  },
});
```

#### `enqueue()` Options

`queue.enqueue(job: JobDefinition<T> | JobDefinition<T>[])`

- `job` the JSON Job object, consisting of
  - `ref?: string` an identifier for the job, allowing future `enqueue()` calls to replace the job with new data. Defaults to a v4 UUID
  - `payload: T` the job's payload which will be saved and sent to the handler
  - `runAt?: Date` a date object describing when the job should run. Defaults to `now()`
  - `runEvery?: string | null` Either a cron interval or an ISO-8601 duration, or `null` to remove recurrence
  - `timezone?: string | null` When using `runEvery`, you can specify a timezone to make DocMQ aware of durations that cross date-modifying thresholds such as Daylight Savings Time; recommended when using cron and duration values outside of UTC.
  - `retries?: number` a number of tries for this job, defaults to `5`
  - `retryStrategy?: RetryStrategy` a retry strategy, defaults to `exponential`

**Retry Strategies**

```ts
interface FixedRetryStrategy {
  type: "fixed";
  amount: number;
  jitter?: number;
}

interface ExponentialRetryStrategy {
  type: "exponential";
  min: number;
  max: number;
  factor: number;
  jitter?: number;
}

export interface LinearRetryStrategy {
  type: "linear";
  min: number;
  max: number;
  factor: number;
  jitter?: number;
}
```

### Handling Work (Processing)

```ts
queue.process(
  async (job /* SampleJob */, api) => {
    await api.ack();
  },
  {
    /* options */
  }
);
```

#### `process()` Options

`queue.process(handler: JobHandler<T, A, F>, config?: ProcessorConfig)`

- `handler` the job handler function, taking the job `T` and the api as arguments, returns a promise
- `config?: ProcessorConfig` an optional configuration for the processor including
  - `pause?: boolean` should the processor wait to be started, default `false`
  - `concurrency?: number` the number of concurrent processor loops to run, default `1`
  - `visibility?: number` specify the visibility window (how long a job is held for by default) in seconds, default `30`
  - `pollInterval?: number` as a fallback, define how often to check for new jobs in the event that driver does not support evented notifications. Defaults to `5`

#### `api` Methods and Members

- `api.ref` (string) the ref value of the job
- `api.attempt` (number) the attempt number for this job
- `api.visible` (number) the number of seconds this job was originally reserved for
- `api.ack(result: A)` acknowlegde the job, marking it complete, and scheduling future work
- `api.fail(reason: string | F)` fail the job and emit the `reason`, scheduling a retry if required
- `api.ping(extendBy: number)` on a long running job, extend the runtime by `extendBy` seconds

### Events

The `Queue` object has a large number of emitted events available through `queue.events`. It extends `EventEmitter`, and the most common events are below:

- `ack` when a job was acked successfully
- `fail` when a job was failed
- `dead` when a job has exceeded its retries and was moved to the dead letter queue
- `stats` an interval ping containing information about the queue's processing load
- `start`, `stop` when the queue starts and stops processing
- `log`, `warn`, `error`, `halt` logging events from the queue

### Managing Fatal Errors With `halt`

A fatal event occurs when DocMQ does not believe it can receover from a connection or processing issue. For example, if using the MongoDB driver and the Change Stream disconnects and the process cannot reconnect. To minimize the likelyhood of repeatedly processing jobs in this scenario, DocMQ will call its `destroy` method and no longer accept jobs for processing. In addition to emitting a standard `error` event, DocMQ will also emit a `halt` event. In most environments, it's recommended to kill your service, allowing your PaaS provider to mark your service as unhealthy and restart it.

```ts
queue.on("halt", () => {
  console.error("Received HALT from DocMQ");
  process.exit(1); // exit code for termination of node.js
});
```

## 🔧 Custom Driver Support

DocMQ works with several drivers, many of which are included in the `/drivers` directory. For development or non-production scenarios, we recommend the `MemoryDriver`, an in-memory driver that supports all of DocMQ's apis. When transitioning to production, you can pass a production driver in and DocMQ will work with no additional changes.

```ts
import { Queue, MemoryDriver } from "docmq";
import { MongoDriver } from "docmq/driver/mongo";

// for example, using the MongoDriver in production, while using
// the less resource-intensive in-memory driver for development
const driver =
  process.env.NODE_ENV === "production"
    ? new MongoDriver(process.env.MONGO_URI)
    : new MemoryDriver("default");

const queue = new Queue(driver, "queueName");
```

| Driver        | `import`                                           | Notes                                                                                                                                                                                                                                                                              |
| :------------ | :------------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **In Memory** | `import { MemoryDriver} from "docmq"`              | The default in-memory driver and (currently) a re-export of `LokiDriver`                                                                                                                                                                                                           |
| LokiJS        | `import { LokiDriver } from "docmq/driver/loki"`   | A fast in-memory driver designed for non-production instances.                                                                                                                                                                                                                     |
| MongoDB       | `import { MongoDriver } from "docmq/driver/mongo"` | Currently, DocMQ requires a Mongo Client >= 4.2 for transaction support, and the mongo instance must be running in a Replica Set. This is because MongoDriver uses the OpLog to reduce polling. Requires [mongodb](https://www.npmjs.com/package/mongodb) peer dependency if using |
| Postgres      | `import { PGDriver } from "docmq/driver/postgres"` | We are slowly expanding our PG Test Matrix based on what GitHub allows. `LISTEN`/`NOTIFY` support is not available, and the driver will fall back to polling. Requires [pg](https://www.npmjs.com/package/pg) as a peer dependency if using                                        |

If you need to write a custom driver, the core `BaseDriver` is available in the core `docmq` package.

## :pencil2: Contributing

We would love you to contribute to [jakobo/docmq](https://github.com/jakobo/docmq), pull requests are welcome! Please see the [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

## ⚖️ License

DocMQ source is made available under the [MIT license](./LICENSE)

## ❤️ Sponsor

This project is made possible thanks to [Taskless](https://taskless.io). The in-memory driver of DocMQ enables Taskless to offer a local version of their service, eliminating the hassle of forwarding webhooks around.

## ✨ Contributors

<!-- ALL-CONTRIBUTORS-BADGE:START - Do not remove or modify this section -->

[![All Contributors](https://img.shields.io/badge/all_contributors-2-orange.svg?style=flat-square)](#contributors-)

<!-- ALL-CONTRIBUTORS-BADGE:END -->

Thanks goes to these wonderful people ([emoji key](https://allcontributors.org/docs/en/emoji-key)):

<!-- ALL-CONTRIBUTORS-LIST:START - Do not remove or modify this section -->
<!-- prettier-ignore-start -->
<!-- markdownlint-disable -->
<table>
  <tr>
    <td align="center"><a href="https://codedrift.com"><img src="https://avatars.githubusercontent.com/u/1795?v=4?s=100" width="100px;" alt=""/><br /><sub><b>Jakob Heuser</b></sub></a><br /><a href="https://github.com/jakobo/docmq/commits?author=jakobo" title="Code">💻</a> <a href="https://github.com/jakobo/docmq/commits?author=jakobo" title="Documentation">📖</a> <a href="https://github.com/jakobo/docmq/commits?author=jakobo" title="Tests">⚠️</a></td>
    <td align="center"><a href="https://github.com/TAKANOME-DEV"><img src="https://avatars.githubusercontent.com/u/79809121?v=4?s=100" width="100px;" alt=""/><br /><sub><b>takanome_dev</b></sub></a><br /><a href="https://github.com/jakobo/docmq/commits?author=TAKANOME-DEV" title="Documentation">📖</a></td>
  </tr>
</table>

<!-- markdownlint-restore -->
<!-- prettier-ignore-end -->

<!-- ALL-CONTRIBUTORS-LIST:END -->

This project follows the [all-contributors](https://github.com/all-contributors/all-contributors) specification. Contributions of any kind welcome!
