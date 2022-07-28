# DocMQ

_Messaging Queue for any document-friendly architectures (DocumentDB, Mongo, Postgres + JSONB, etc)._

> **Why Choose This** :grey_question:
>
> DocMQ is a good choice if you're looking for a document based message queue built around the visibility window. If you're someone who's frustrated that Amazon's SQS has a 15 minute maximum delay, or are trying to query Redis like it's a database, then this is probably the kind of solution you're looking for. DocMQ works with anything that holds and queries documents or document-like objects.
>
> **Why AVOID This** :grey_question:
>
> Simple. Performance. This kind of solution will never be as fast as an in-memory Redis queue or an event bus. If fast FIFO is your goal, you should consider BullMQ, Kue, Bee, Owl, and others.

:warning: **ALPHA SOFTWARE** - This is still in active development, there will be bugs. As of this writing, the basic queue/process/recurrence/delay pieces work, but there's still work to be done smoothing APIs, cleaning up typescript definitions etc.

## Installation

You'll want to install DocMQ along with the mongodb client driver. This allows you to bring your own version of the mongo client in the event the MongoDB node driver changes in a material way.

Currently, DocMQ requires a Mongo Client >= 4.2 for transaction support.

```sh
# npm
npm i docmq mongodb

# yarn
yarn add docmq mongodb

# pnpm
pnpm add docmq mongodb
```

- [📚 Documentation](#-documentation)
- [🔧 Custom Driver Support](#-custom-driver-support)
- [License](#license)

## 📚 Documentation

### Creating a Queue

```ts
import { Queue, MongoDriver } from "docmq";

interface SimpleJob {
  success: boolean;
}

const queue = new Queue<SimpleJob>(
  new MongoDriver(process.env.DOC_DB_URL),
  "docmq"
);
```

#### `new Queue()` options

`new Queue<T>(driver: Driver, name: string, options?: QueueOptions)`

- `driver` a Driver implementation to use such as the `MongoDriver`
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
- `log`, `warn`, `error` logging events from the queue

## 🔧 Custom Driver Support

## :pencil2: Contributing

We would love you to contribute to [jakobo/docmq](https://github.com/jakobo/docmq), pull requests are welcome!
Please see the [CONTRIBUTING.md](CONTRIBUTING.md) for more information.

## ⚖️ License

DocMQ source is made available under the [MIT license](./LICENSE)
