# Releasing using `release-it`

This repository releases code using [release-it](https://github.com/release-it/release-it). New releases can be triggered from the root package via `yarn rel` which offers a guided process.

As part of our build command, we perform a local npm install and verify that our CJS/ESM imports are working as expected.

## Common Commands

- Begin a new for the next version `next` with `yarn rel <major|minor|patch> --preRelease=next`
- Continue an existing `major`, `minor`, or `patch` pre release with `yarn rev`
- Change the pre release tag with a new `--preRelease=` flag
- Ultimately release with `yarn rel <major|minor|patch>`
