module.exports = {
  "*.(md|json)": "prettier --write",
  "package.json": [
    () => "syncpack format",
    "prettier --write",
  ],
  "**/*.ts?(x)": [
    () => "yarn test",
    "eslint --fix",
    "madge --circular",
    "prettier --write",
  ],
};
