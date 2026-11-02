# ![Alfa Morf](https://github.com/alfasoftware/morf/wiki/morf.png)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.com/alfasoftware/morf.svg?branch=main)](https://app.travis-ci.com/alfasoftware/morf)
[![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=org.alfasoftware%3Amorf-parent&metric=alert_status)](https://sonarcloud.io/project/overview?id=org.alfasoftware:morf-parent)
[![Changelog](https://img.shields.io/badge/-Changelog-grey.svg)](https://github.com/alfasoftware/morf/releases)

**Morf makes databases less of a headache.** It's a Java library for evolving your schema, talking to your database, and moving data between platforms, all without writing platform-specific SQL.

We've been building it at [Alfa](https://www.alfasystems.com) for many years, and it manages every database-backed application we ship. Now we're sharing it with everyone else.

## What can it do?

- **Evolve your schema safely.** Write upgrade steps in plain Java. Morf checks them against your target schema, handles data migration alongside schema changes, and can even tidy up after branches that merge out of order.
- **Start from nothing.** Empty database? Morf bootstraps it for you, with no extra code.
- **Work on your database of choice.** Oracle, PostgreSQL, MySQL, SQL Server and H2 are supported. Prefer a different one? Adding a platform is designed to be easy.
- **Hand DBAs what they want.** Export upgrade scripts as SQL in any supported dialect.
- **Snapshot and clone.** Dump a MySQL database to XML and restore it into Oracle. Copy a live PostgreSQL database into in-memory H2. Run your JUnit tests against a snapshot of a real environment, and let Morf apply any missing upgrades afterwards.
- **Query with the same DSL.** The Java API you use for upgrades also works for run-time database access. There's no XML, JSON or YAML to wrangle.

## Getting started

Morf is published to Maven Central under `org.alfasoftware`. Start with `morf-core`, then add the module for your database (for example `morf-postgresql` or `morf-h2`).

Want to see it work first? [`HelloMorf`](morf-examples/src/main/java/org/alfasoftware/morf/examples/HelloMorf.java) is a single file that builds an in-memory H2 database, then upgrades it. There's nothing to install.

Our [Start Here](https://github.com/alfasoftware/morf/wiki/Start-Here) wiki page is the best place to begin, and we're adding to it all the time. If something is unclear, that's our gap to fill, so please [open an issue](https://github.com/alfasoftware/morf/issues). The [roadmap](https://github.com/alfasoftware/morf/wiki/Roadmap) shows where we're headed.

## Come and help

We're developing Morf in the open, and we'd love your help, whether that's a bug report, a doc fix, a new database platform, or just an idea. Take a look at the [contribution guide](https://github.com/alfasoftware/morf/wiki/Contributing) and say hello.

Morf is licensed under [Apache 2.0](LICENSE).
