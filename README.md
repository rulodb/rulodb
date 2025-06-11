[![Build](https://github.com/rulodb/rulodb/actions/workflows/build.yml/badge.svg)](https://github.com/rulodb/rulodb/actions/workflows/build.yml)
[![License](https://img.shields.io/badge/license-AGPL%203.0-green.svg)](./LICENSE)
[![codecov](https://codecov.io/gh/rulodb/rulodb/graph/badge.svg?token=9NVFTUXUEV)](https://codecov.io/gh/rulodb/rulodb)

<br />
<div align="center">
  <img style="width:100%;" src="./.github/assets/github-header.png" alt="GitHub Header for RuloDB" />

  <p align="center">
    NoSQL database with a ReQL-like query language.
    <br />
    <a href="https://rulodb.io"><strong>Explore the docs</strong></a>
    ·
    <a href="https://discord.gg/RzeAMwSM5R"><strong>Join us on Discord</strong></a>
    <br />
    <a href="https://github.com/rulodb/rulodb/issues/new?assignees=&labels=bug%2Ctriage-needed&projects=&template=BUG-REPORT.yml">Bug report</a>
    ·
    <a href="https://github.com/rulodb/rulodb/issues/new?assignees=&labels=question%2Cenhancement%2Ctriage-needed&projects=&template=FEATURE-REQUEST.yml">Feature request</a>
  </p>
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

## License

RuloDB is dual-licensed under the AGPLv3 and a commercial license:

- AGPL-3.0 for open source use
- Commercial license available for closed-source or proprietary use

See [`LICENSE`](./LICENSE) and [`LICENSE-COMMERCIAL`](./LICENSE-COMMERCIAL) for details.

To purchase a license or for compliance questions, send an email to [info@opcotech.com](mailto:info@opcotech.com).
