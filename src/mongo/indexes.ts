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

  // -----
  // Reminder, use ESR rule for indexes: Equality, Sort, Range
  // name indexes, as these are dependent on node-mongo's convention otherwise
  // -----

  // Migration to config 1
  if (current.version <= 1) {
    const indexes: Promise<string>[] = [];

    // jobs by ref
    indexes.push(
      collections.jobs.createIndex([["ref", 1]], {
        name: "ref_1",
        sparse: true,
      })
    );

    // jobs by ack
    indexes.push(
      collections.jobs.createIndex([["ack", 1]], {
        name: "ack_1",
        unique: true,
        sparse: true,
      })
    );

    // jobs by next available
    // e = deleted, s = _id, r = visible
    indexes.push(
      collections.jobs.createIndex(
        [
          ["deleted", 1],
          ["_id", 1],
          ["visible", 1],
        ],
        { name: "deleted_1_id_1_visible_1" }
      )
    );

    // jobs by refs in future
    // e = deleted & ref, r = visible
    indexes.push(
      collections.jobs.createIndex(
        [
          ["deleted", 1],
          ["ref", 1],
          ["visible", 1],
        ],
        {
          name: "deleted_1_ref_1_visible_1",
        }
      )
    );

    // upsert jobs by ref
    indexes.push(
      collections.jobs.createIndex(
        [
          ["ref", 1],
          ["deleted", 1],
          ["ack", 1],
          ["visible", 1],
        ],
        {
          name: "ref_1_deleted_1_ack_1_visible_1",
        }
      )
    );

    // a unique index that prevents multiple un-acked jobs of the same ref
    indexes.push(
      collections.jobs.createIndex(
        [
          ["ref", 1],
          ["deleted", 1],
          ["ack", 1],
        ],
        {
          name: "ref_1_deleted_1_ack_1",
          unique: true,
          sparse: true,
        }
      )
    );

    // deadletter index by ref
    indexes.push(
      collections.deadLetterQueue.createIndex(
        [
          ["ref", 1],
          ["created", 1],
        ],
        {
          name: "ref_1_created_1",
        }
      )
    );

    await Promise.allSettled(indexes);
    current.version = 1;
  }

  // save new config
  await collections.config.replaceOne({ version: current.version }, current);

  return true;
};
