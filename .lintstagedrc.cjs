const json = require("./package.json");
const circularChecks = Object.entries(json.tsup.entry).map(([k, path]) => {
  return path;
});

module.exports = {
  "*.(md|json)": "prettier --write",
  "package.json": [() => "syncpack format", "prettier --write"],
  "**/*.ts?(x)": [
    () => "pnpm test",
    "eslint --fix",
    () =>
      `dpdm --no-warning --no-tree --exit-code circular:1 ${circularChecks.join(
        " "
      )}`,
    "prettier --write",
  ],
};
