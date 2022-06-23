import { type Collections, type ConfigDoc } from "../types.js";

const CURRENT = 1;

export const updateIndexes = async (collections: Collections) => {
  await collections.config.createIndex({ version: -1 }, { unique: true });
  const cfg = await collections.config.findOne({
    version: {
      $gte: 0,
    },
  });

  // extract config
  const current: ConfigDoc =
    cfg !== null
      ? cfg
      : {
          version: 0,
          config: null,
        };

  if (current.version >= CURRENT) {
    return true;
  }
  // const config = current.config ? JSON.parse(current.config) : {};

  // migrations are required if a non-additive change is introduced
  if (current.version === 0) {
    // create jobs indexes
    await collections.jobs.createIndex({ ref: 1 }, { sparse: true });
    await collections.jobs.createIndex(
      { ack: 1 },
      { unique: true, sparse: true }
    );
    await collections.jobs.createIndex({ deleted: 1, visible: 1, _id: 1 });

    // create deadletter indexes
    await collections.deadLetterQueue.createIndex({ ref: 1 }, { sparse: true });
    await collections.deadLetterQueue.createIndex({ created: 1 });

    current.version = 1;
  }

  // save new config
  await collections.config.replaceOne({ version: current.version }, current);

  return true;
};
