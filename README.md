# DocMQ

_Messaging Queue for any document-friendly architectures (DocumentDB, Mongo, Postgres + JSONB, etc)._

> **Why Choose This**
> DocMQ is a good choice if you're looking for a document based message queue built around the visibility window. If you're someonme who's frustrated that Amazon's SQS has a 15 minute maximum delay, or are trying to query Redis like it's a database, then this is probably the kind of solution you're looking for. DocMQ works with anything that holds and queries documents or document-like objects.
>
> **Why AVOID This**
> Simple. Performance. This kind of solution will never be as fast as an in-memory Redis queue or an event bus. If fast FIFO is your goal, you should consider BullMQ, Kue, Bee, Owl, and others.

:warning: **ALPHA SOFTWARE** - This is still in active development, there will be bugs. As of this writing, the basic queue/process/recurrence/delay pieces work, but there's still work to be done smoothing APIs, cleaning up typescript definitions etc.

## Installation

You'll want to install DocMQ along with the mongodb client driver. This allows you to bring your own version in the event the MongoDB node driver changes in a material way.

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

### Adding a Job to the Queue

```ts
await queue.enqueue({
  success: true,
} as T);
```

#### `enqueue()` Options

### Handling Work (Processing)

```ts
queue.process(async (job, api) => {
  await api.ack();
});
```

#### `process()` Options

### Events

## 🔧 Custom Driver Support

## License

DocMQ source is made available under the [MIT license](./LICENSE)
