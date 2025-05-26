[![Build](https://github.com/gabor-boros/rulodb/actions/workflows/build.yml/badge.svg)](https://github.com/gabor-boros/rulodb/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/license-Apache%202.0-green.svg)](./LICENSE)
[![codecov](https://codecov.io/gh/gabor-boros/rulodb/graph/badge.svg?token=9NVFTUXUEV)](https://codecov.io/gh/gabor-boros/rulodb)

<br />
<div align="center">
  <img style="width:100%;" src="./.github/assets/github-header.png" alt="GitHub Header for RuloDB" />
</div>

# RuloDB

> **Active development**
> RuloDB is in **active development**. APIs and internals are evolving rapidly. Contributions and feedback are welcome!
> It is not even remotely ready for any production environment.

**RuloDB** is a document database inspired by [RethinkDB](https://rethinkdb.com/), built for developer ergonomics.

It features a lightweight query engine and **Rulo**, a simplified query language derived from ReQL.

## Build

To build RuloDB, you need to install system dependencies. For the latest Ubuntu, you can use the following commands:

```shell
sudo apt update && \
sudo apt install -y \
  build-essential \
  clang \
  libclang-dev \
  libc6-dev
```

To build the project, you need to have Rust and Cargo installed. You can install them
using [rustup](https://rustup.rs/):

```shell
$ cargo build --release
```

## Contributing

Contributions are highly welcomed, whether it is source code, documentation, bug reports, feature requests or feedback.
To get started with contributing:

1. Have a look through GitHub issues labeled "good first issue". Read the contributing guide.
2. For details on building RuloDB, see the "Build" section above.
3. Create a fork of RuloDB and submit a pull request with your proposed changes.
