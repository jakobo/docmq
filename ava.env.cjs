/* eslint-disable */

// https://github.com/nodejs/node/issues/30810

const originalEmit = process.emit;
process.emit = function (name, data, ...args) {
  if (
    name === "warning" &&
    typeof data === "object" &&
    data.name === "ExperimentalWarning" &&
    (data.message.includes("--experimental-loader") ||
      data.message.includes("Custom ESM Loaders is an experimental feature") ||
      data.message.includes(
        "The Node.js specifier resolution flag is experimental"
      ) ||
      data.message.includes(
        "Importing JSON modules is an experimental feature"
      ))
  )
    return false;

  return originalEmit.apply(process, arguments);
};
