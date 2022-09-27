import { BaseDriver, QueueDoc } from "../../src/index.js";

export interface Context<T extends BaseDriver> {
  createDriver: () => Promise<T>;
  driver: T;
  insert: (doc: QueueDoc) => Promise<unknown>;
  dump: () => Promise<QueueDoc[]>;
  end: () => Promise<void> | void;
}
