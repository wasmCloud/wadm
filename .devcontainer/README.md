# wadm devcontainer

A simple devcontainer that has `rust` installed. Try it out with `devcontainer open` at the root of this repository!

As a `postCreateCommand`, we run `cargo install` steps for you to install the proper versions of wash & wadm for experimenting. In the future these will be simplified and much quicker once we release actual artifacts for both projects with support for wadm 0.4.

## Prerequisites

- [devcontainer CLI](https://code.visualstudio.com/docs/devcontainers/devcontainer-cli#_installation)
- VSCode
