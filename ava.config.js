const config = {
  extensions: {
    ts: "module",
  },
  files: ["test/*.spec.ts", "test/**/*.spec.ts"],
  nodeArguments: [
    "--require=./ava.env.cjs",
    "--experimental-specifier-resolution=node"
  ],
};

export default config;
