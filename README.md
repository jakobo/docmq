# DocMQ

_Messaging Queue for document based architectures (DocumentDB, Mongo, etc)._

> **Why Choose This**
> DocMQ is a good choice if you're looking for a document based message queue that emphasizes the visibility window. If you're someonme who's frustrated that Amazon's SQS has a 15 minute maximum delay, or wishes retaining messages wouldn't cause you monetary pain, then this is probably the kind of solution you're looking for.
>
> **Why AVOID This**
> Simple. Performance. Mongo & DocDB solutions will never be as fast as an in-memory Redis queue. If what you're looking for is centered around as-fast-as-possible FIFO operations, you should consider BullMQ, Kue, Bee, Owl, and others.

**ALPHA SOFTWARE** - This is still in active development, there will be bugs. As of this writing, the basic queue/process/recurrence/delay pieces work, but there's still work to be done smoothing APIs, cleaning up typescript definitions etc.

## Installation

```sh
# npm
npm i docmq

# yarn
yarn add docmq

# pnpm
pnpm add docmq
```

- [📚 Documentation](#-documentation)
- [License](#license)

## 📚 Documentation

### Creating a Queue

### Adding a Job to the Queue

### Handling Work (Processing)

## License

DocMQ source is made available under the [MIT license](./LICENSE)
