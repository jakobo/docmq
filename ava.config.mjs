const config = {
  extensions: {
    ts: "module",
  },
  files: ["test/*.spec.ts", "test/**/*.spec.ts"],
  environmentVariables: {
    ESM_MULTI_LOADER: "testdouble,ts-node/esm",
  },
  require: ["dotenv/config"],
  nodeArguments: [
    "--loader=esm-multi-loader",
    "--require=./ava.env.cjs",
    "--trace-warnings",
  ],
};

export default config;
