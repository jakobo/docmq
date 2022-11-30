const config = {
  extensions: {
    ts: "module",
  },
  files: ["test/*.spec.ts", "test/**/*.spec.ts"],
  require: ["dotenv/config"],
  nodeArguments: [
    "--loader=testdouble",
    "--loader=ts-node/esm",
    "--require=./ava.env.cjs",
    "--trace-warnings",
  ],
};

export default config;
