import { BaseDriver, QueueDoc } from "../../src/index.js";
import { type DriverOptions } from "../../src/types.js";

export interface Context<T extends BaseDriver> {
  createDriver: (options?: DriverOptions) => Promise<T>;
  setDriver: (driver: T) => Promise<void>;
  driver: T;
  insert: (doc: QueueDoc) => Promise<unknown>;
  dump: () => Promise<QueueDoc[]>;
  end: () => Promise<void> | void;
}
