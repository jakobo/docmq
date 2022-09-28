import { defineBuildConfig } from "unbuild";

export default defineBuildConfig({
  clean: true,
  declaration: true,
  rollup: {
    cjsBridge: true,
    emitCJS: true,
  },
});
