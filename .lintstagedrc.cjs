module.exports = {
  "*.(md|json)": "prettier --write",
  "package.json": [() => "syncpack format", "prettier --write"],
  "**/*.ts?(x)": [
    () => "pnpm test",
    "eslint --fix",
    "madge --circular",
    "prettier --write",
  ],
};
