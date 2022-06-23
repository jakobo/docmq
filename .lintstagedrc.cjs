module.exports = {
  "*.(md|json)": "prettier --write",
  "package.json": [
    () => "syncpack format",
    "prettier --write",
  ],
};
