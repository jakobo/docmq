const config = {
  extensions: {
    ts: "module",
  },
  files: ["test/*.spec.ts", "test/**/*.spec.ts"],
  environmentVariables: {
    ESM_MULTI_LOADER: "testdouble,ts-node/esm",
  },
  nodeArguments: ["--loader=esm-multi-loader", "--require=./ava.env.cjs"],
};

export default config;
