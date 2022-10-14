/**
 * Used for importing ESM only modules in a CommonJS/ESM Hybrid Environment
 * When Node14 is no longer LTS and Node16 is the norm, this module will become
 * ESM-only, at which point this function and associated imports can be safely
 * removed.
 *
 * How we got here...
 * ref: https://esbuild.github.io/content-types/#direct-eval
 * ref: https://github.com/microsoft/TypeScript/issues/43329#issuecomment-922544562
 * ref: https://github.com/microsoft/TypeScript/issues/43329#issuecomment-1003472451
 * ref: https://github.com/microsoft/TypeScript/issues/46452
 */
export const load = async <T = unknown>(name: string) => {
  return Promise.resolve((0, eval)(`import("${name}")`) as T);
};
