# Contributing to DocMQ

- [Contributing to DocMQ](#contributing-to-docmq)
  - [🗺 Repository Layout](#-repository-layout)
  - [📦 Download and Setup](#-download-and-setup)
  - [⏱ Testing Your Changes](#-testing-your-changes)
    - [✅ Unit Testing](#-unit-testing)
    - [🏁 E2E Testing](#-e2e-testing)
  - [📚 Updating Documentation](#-updating-documentation)
  - [📝 Writing a Commit Message](#-writing-a-commit-message)
  - [🔎 Before Submitting](#-before-submitting)

Thanks for the help! We currently review PRs for `**/*`.

DocMQ is a Messaging Queue built for Document based databases such as Mongo or DocumentDB. We welcome all contributions to it. This file is designed to help you find your way around.

## 🗺 Repository Layout

The repository has the bulk of its code in the `src` directory.

In the root of the repository, we have a few common files that are effectively "global". Some (eslint, prettier) affect formatting, while others (tsconfig) affect build scripts. If you're inside of a package and see it extending `../../something`, it's relying on the common config.

## 📦 Download and Setup

> 💽 The development environment for this repository does not support Windows. To contribute from Windows you must use WSL.

1. [Fork](https://help.github.com/articles/fork-a-repo/) this repository to your own GitHub account and then [clone](https://help.github.com/articles/cloning-a-repository/) it to your local device. (`git remote add upstream git@github.com:jakobo/docmq.git` 😉). You can use `git clone --depth 1 --single-branch --branch main git@github.com:jakobo/docmq.git`, discarding most of branches and history to clone it faster.
2. Ensure [Node 14](https://nodejs.org/) is installed on your computer. (Check version with `node -v`). We have [Volta](https://volta.sh) defined on the root package.json to help out.
3. Install the dependencies using yarn with `yarn install`

> If this didn't work for you as described, please [open an issue.](https://github.com/jakobo/docmq/issues/new/choose)

## ⏱ Testing Your Changes

This repo is set up to run `yarn test` on commit. You can also run the command at any time to recheck your changes with AVA. We are not requiring new code to have corresponding tests at this time.

### ✅ Unit Testing

Written in AVA. Please see /test/\*.spec.ts for examples. When writing driver tests, the driver suite expects at test time for `t.context.driver` to contain an instance of the driver being tested, `t.context.insert` to take a DocMQ document and insert it into the database, and `t.context.dump` to return all records in the database.

- By default, only the Loki driver tests will run. This is because not every architecture can run every DB
- Setting `process.env.MONGO_URI` will enable MongoDB tests using an external MongoDB instance. Please ensure it supports Replica Sets
- Setting `process.env.POSTGRES_URI` will enable Postgres tests using an external Postgres instance

Until [this ava issue](https://github.com/avajs/ava/issues/2979) is resolved, we work around this by selecting `test`/`test.skip` as a runtime evaluation.

### 🏁 E2E Testing

End to End tests are accepted. Please use the `LokiAdapter` for any tests, as it does not mandate the external dependencies to be loaded.

## 📚 Updating Documentation

Coming soon. As we learn what documentation people need, we'll undertake a docs project. It may be as simple as the README, or may use Github's inbuilt wiki.

## 📝 Writing a Commit Message

> If this is your first time committing to a large public repo, you could look through this neat tutorial: ["How to Write a Git Commit Message"](https://chris.beams.io/posts/git-commit/)

For consistency, this repository uses [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/), making it easier to identify what changed, its impact, and if is a breaking change. You can see our supported [types](./commitlint.config.js), though the majority of changes are likely to be `feat`, `fix`, or `docs`.

## 🔎 Before Submitting

To help land your contribution, please make sure of the following:

- Remember to be concise in your Conventional Commit. These will eventually be automatically rolled up into an auto-generated CHANGELOG file
- If you modified anything in `src/`:
  - You verified the transpiled TypeScript with `yarn build` in the directory of whichever package you modified. This will also verify your CJS/ESM exports
  - Run `yarn test` to ensure all existing tests pass for that package, along with any new tests you would've written.

Thank you! 💕
