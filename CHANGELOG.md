Packages in this repository are synchronized on release, with a single changelog across all pacakges. This changelog is specifically limited to items in the `packages/` folder.

# 2.1.3 - released June 2, 2022

#### 🔧 Fixes

- **@taskless/dev** Moves mongod and cron initialization to code that runs outside of the next.js handler. Fixes `serverRuntimeConfig.mongod is not a function` errors
- **@taskless/client** Improved detection of the development environment, checking `TASKLESS_ENV`, followed by `NODE_ENV`. This allows us to run production code from next.js in the Dev Server while also running @taskless/client in development mode for improved debugging and error messages

# 2.1.2 - released June 2, 2022

#### 🔧 Fixes

- **@taskless/dev** Fixes issue where the commonjs `server` directory was not included in the new distribution package

# 2.1.1 - released June 2, 2022

#### 🔧 Fixes

- **@taskless/dev** Fixes issue where the custom `server.js` was not included in the new distribution package

# 2.1.0 - released June 2, 2022

#### 🎉Features

- **@taskless/client** Now formally supports unverified signatures in production scenarios. There are some instances where you do not want to check signatures (for example via a webhook), or you might be checking the authenticity of the payload in some other manner. In these cases, you can explicitly override the signature checking behavior of the Taskless client on a per-queue basis. To enable this, add `{ __dangerouslyAllowUnverifiedSignatures: { allowed: true } }` to your `QueueOptions`. After looking at a variety of APIs, we felt the `__dangerously` is both easy to project search for and requires opting in via a manner that does not have ambiguity. In development, the behavior remains unchanged.
- **@taskless/client** Added support for serialized error messages. Previously, only the error's `message` property was transmitted. We now serialize the whole error (as best we can) to improve the logging and debugging experience.

#### 🔧 Fixes

- **@taskless/dev** Previously, the mongod implementation required a hack to create a globally shared in-memory instance. There were corner cases that could arise where next.js could create multiple mongos or multiple crons. To simplify the code, we're now using [runtime configuration](https://nextjs.org/docs/api-reference/next.config.js/runtime-configuration) to create singleton instances of the mongod and cron tools. We now also use a [custom server.js](https://nextjs.org/docs/advanced-features/custom-server) to start our workers, removing the original code that required an incoming job to start the job infrastructure in development.

#### 🎒 Misc

- **@taskless/client** Reduced dependencies needed by manually merging queue and job options
- **@taskless/dev** Removed unused dependencies
- **@taskless/express** Removed unused dependencies
- **@taskless/root** Fixed missing optional dependency for cosmicconfig until [this fix](https://github.com/EndemolShineGroup/cosmiconfig-typescript-loader/issues/147) lands in an upstream dependency

# 2.0.2 - released May 27, 2022

#### 🎒 Misc

- **@taskless/dev** Updated @headless/react to latest version, changed tailwind colors to use `-primary-` instead of `-brand-`. Now checks the UI module for additional rendering styles at build time
- **@taskless/dev** Fixes mongoose typings to be more correct
- **@taskless/root** Now using [syncpack](https://github.com/JamieMason/syncpack) to keep dependencies consistent between modules and additional formatting in package.json that prettier cannot handle such as ordering the keys
- **@taskless/ui** Created a common module for UI components, ensuring a consistent experience between taskless.io and the Taskless Dev server (@taskless/dev) for some of the most common blocks such as `DataTable`, `Modal`, and form controls

# 2.0.1 - released May 24, 2022

#### 🔧 Fixes

- **@taskless/client** In some cases, the default header of `content-type = application/json` was not being set. This could result in situations where a JSON middleware such as `express.json()` was not correctly parsing the body

# 2.0.0 - released May 20, 2022

#### 💥 BREAKING CHANGES

- **@taskless/client** Integrations were split out to avoid conflicting namespace issues. Next and Express users can now reference `@taskless/next` and `@taskless/express` respectively. The `@taskless/client` contains only the raw Taskless client.

#### 🎉Features

- **@taskless/client**, **@taskless/next**, **@taskless/express** Added the ability to specify an array as a job identifier instead of just a string key, making namespacing identifiers require less cognitive overhead
- **@taskless/dev** Added the ability to create jobs via the Taskless dev dashboard
- **@taskless/express** Added a `mount` method for working with Taskless when it's attached to a sub-router.

#### 🔧 Fixes

- **@taskless/client** Fixed default export of `Queue` in CJS environments
- **@taskless/client** Fixed issue in development where a mismatched signature would throw instead of logging an error

#### 🎒 Misc

- **@taskless/client** Updated node specific modules to import from the `node:` namespace
- **@taskless/client** Moved to home in `/packages` matching its package name to reduce confusion
- **@taskless/dev** Switched PouchDB for [mongo-memory-server](https://www.npmjs.com/package/mongodb-memory-server). While it adds a bit more overhead to start up a Mongo server in development, it makes it much easier to use Mango queries for querying task and job information.
- **@taskless/root** Added a `dev` script that gets every integration up and running in dev mode for fast debugging

# 1.1.0 - released May 2, 2022

#### 🎉Features

- **@taskless/dev** New devlopment server pages at `/` and `/logs` to mirror Taskless.io, replacing the old completed/scheduled structure
- **@taskless/dev** Common UI components. Imported Taskless' DataTable, Logo, and Slash components

# 1.0.0 - released April 25, 2022

Initial 1.0 release of @taskless/client and @taskless/dev
