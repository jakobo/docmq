/* eslint-disable */
module.exports = {
  root: true,
  parser: "@typescript-eslint/parser",
  parserOptions: {
    extraFileExtensions: [".cjs", ".mjs"],
    tsconfigRootDir: __dirname,
    // project: ["./tsconfig.json"],
  },
  plugins: ["@typescript-eslint", "import", "node"],
  settings: {
    "import/parsers": {
      "@typescript-eslint/parser": [".ts", ".tsx"],
    },
    "import/resolver": {
      typescript: {
        alwaysTryTypes: true,
        project: ["./tsconfig.json"],
      },
    },
  },
  extends: [
    "eslint:recommended",
    "plugin:@typescript-eslint/recommended",
    "plugin:@typescript-eslint/recommended-requiring-type-checking",
    "prettier",
  ],
  rules: {
    // enable import rules
    "import/no-unresolved": "error",

    // default node.js behaviors (which are also fine for browsers!)
    "node/prefer-promises/fs": "error",
    "node/prefer-global/url-search-params": ["error", "always"],
    "node/prefer-global/url": ["error", "always"],

    // False positives. There be dragons here. Most common culprits are
    // optional chaining and nullish coalesce.
    // example: @ packages/client/queue/queue.ts
    // this.queueOptions.baseUrl is of type (string | undefined)
    // Using it causes typescript-eslint to believe it is
    // of type "any" and trigger these eslint errors.
    // https://github.com/typescript-eslint/typescript-eslint/issues/2728
    // https://github.com/typescript-eslint/typescript-eslint/issues/4912
    "@typescript-eslint/no-unsafe-argument": "off",
    "@typescript-eslint/no-unsafe-assignment": "off",
    "@typescript-eslint/no-unsafe-call": "off",
    "@typescript-eslint/no-unsafe-member-access": "off",
    "@typescript-eslint/no-unsafe-return": "off",
    "@typescript-eslint/restrict-template-expressions": "off",

    // https://typescript-eslint.io/rules/no-unused-vars/
    // https://eslint.org/docs/rules/no-unused-vars
    "no-unused-vars": "off",
    "@typescript-eslint/no-unused-vars": [
      "error",
      { ignoreRestSiblings: true },
    ],
  },
};
