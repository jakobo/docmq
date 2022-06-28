import { Db } from "mongodb";
import { type Collections, type ConfigDoc } from "../types.js";

const CURRENT = 1;

export const updateIndexes = async (db: Db, collections: Collections) => {
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

  // migrations are required if a non-additive change is introduced
  if (current.version === 0) {
    // jobs by ref
    await collections.jobs.createIndex([["ref", 1]], {
      name: "ref_1",
      sparse: true,
    });
    // jobs by ack
    await collections.jobs.createIndex([["ack", 1]], {
      name: "ack_1",
      unique: true,
      sparse: true,
    });
    // jobs by next available
    // e = deleted, s = _id, r = visible
    await collections.jobs.createIndex(
      [
        ["deleted", 1],
        ["_id", 1],
        ["visible", 1],
      ],
      { name: "deleted_1_id_1_visible_1" }
    );
    // jobs by refs in future
    // e = deleted & ref, r = visible
    await collections.jobs.createIndex(
      [
        ["deleted", 1],
        ["ref", 1],
        ["visible", 1],
      ],
      {
        name: "deleted_1_ref_1_visible_1",
      }
    );

    // create deadletter indexes
    await collections.deadLetterQueue.createIndex(
      [
        ["ref", 1],
        ["created", 1],
      ],
      {
        name: "ref_1_created_1",
      }
    );

    current.version = 1;
  }

  // save new config
  await collections.config.replaceOne({ version: current.version }, current);

  return true;
};
