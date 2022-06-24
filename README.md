# DocMQ

_Messaging Queue for document based architectures (DocumentDB, Mongo, etc)._

> **Why Choose This**
> DocMQ is a good choice if you're looking for a document based message queue that emphasizes the visibility window. If you're someonme who's frustrated that Amazon's SQS has a 15 minute maximum delay, or wishes retaining messages wouldn't cause you monetary pain, then this is probably the kind of solution you're looking for.
>
> **Why AVOID This**
> Simple. Performance. Mongo & DocDB solutions will never be as fast as an in-memory Redis queue. If what you're looking for is centered around as-fast-as-possible FIFO operations, you should consider BullMQ, Kue, Bee, Owl, and others.

**ALPHA SOFTWARE** - This is still in active development, there will be bugs. As of this writing, the basic queue/process/recurrence/delay pieces work, but there's still work to be done smoothing APIs, cleaning up typescript definitions etc.

## Installation

You'll want to install DocMQ along with the mongodb client driver. This allows you to bring your own version as long as it satifies DocMQ's peer dependency.

```sh
# npm
npm i docmq mongodb

# yarn
yarn add docmq mongodb

# pnpm
pnpm add docmq mongodb
```

- [📚 Documentation](#-documentation)
- [License](#license)

## 📚 Documentation

### Creating a Queue

```ts
import { Queue } from "docmq";

interface SimpleJob {
  success: boolean;
}

const queue = new Queue<SimpleJob>(process.env.DOC_DB_URL, "docmq");
```

### Adding a Job to the Queue

```ts
await queue.enqueue({
  success: true,
} as T);
```

#### `enqueue()` Options

#### `enqueueMany()` Bulk Add

### Handling Work (Processing)

```ts
queue.process(async (job, api) => {
  await api.ack();
});
```

#### `process()` Options

### Events

## License

DocMQ source is made available under the [MIT license](./LICENSE)
