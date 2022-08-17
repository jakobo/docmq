import { BaseDriver, QueueDoc } from "../../src/index.js";

export interface Context {
  hasDriver: boolean;
  createDriver: () => Promise<BaseDriver>;
  driver: BaseDriver;
  insert: (doc: QueueDoc) => Promise<unknown>;
  dump: () => Promise<QueueDoc[]>;
  end: () => Promise<void> | void;
}
