# ![Alfa Morf](https://github.com/alfasoftware/morf/wiki/morf.png)

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://travis-ci.com/alfasoftware/morf.svg?branch=main)](https://app.travis-ci.com/alfasoftware/morf)
[![Quality Gate](https://sonarcloud.io/api/project_badges/measure?project=org.alfasoftware%3Amorf-parent&metric=alert_status)](https://sonarcloud.io/project/overview?id=org.alfasoftware:morf-parent)
[![Changelog](https://img.shields.io/badge/-Changelog-grey.svg)](https://github.com/alfasoftware/morf/releases)

Morf is a library for cross-platform evolutionary relational database mechanics, database access and database imaging/cloning.  It has been developed over many years at Alfa and is used to manage all of our RDBMS-backed applications.

## Status

Morf is proven technology and has been used in mission-critical production applications for many years as part of our flagship product.  **However**, as an open source project, it is in an early **pre-alpha** state, as we go through the process of disentangling it from parts of the rest of our stack, tidying up the code for long-term support, and making it easier and more convenient to use in other projects. Until then, consider the API unstable.

We are maintaining Morf in the open, because we believe this is the right thing to do. Please [get involved](https://github.com/alfasoftware/morf/wiki/Contributing) if you would like to help steer the project.

See the [Roadmap](https://github.com/alfasoftware/morf/wiki/Roadmap) for more information.

## Getting Started
Documentation is somewhat sparse, not least because the APIs are not quite _ready_ to document.  One of the first items on the [roadmap](https://github.com/alfasoftware/morf/wiki/Roadmap) is to agree our new user "hello world" user stories, write them up and make sure the necessary APIs are in place to support them.

As we work on those, we will be expanding the [Start here](https://github.com/alfasoftware/morf/wiki/Start-Here) part of the Wiki.

## Key features

### Pure Java API
- No XML/JSON/YAML etc
- Clean and easy to integrate with existing applications

### Evolutionary Database Design
- Implements bidirectional run-time verification of upgrades against a known target schema.
- Bootstraps empty databases with no additional coding
- Manages complex data migration and fixing alongside schema changes
- Targets multiple database platforms without platform-specific scripting and without sacrificing low level control
- Cleanly integrates disordered branches
- Exports SQL upgrade scripts in any SQL dialect for DBA use

### Extensible
- Easy to add new database platform support

### Cross-platform Snapshots and Cloning
- Clone or dump a full database, including schema and data
- Create a dump of a MySQL database as XML, restore to Oracle
- Copy a live PostgreSQL database directly into in-memory H2
- Run a JUnit test over an in-memory H2 database using a snapshot from a live Oracle environment
- Automatically run missing upgrades after restoring from a snapshot

### Database Access API
- Database access at run-time using the same DSL used in upgrades

## Contributing
If you're interested in contributing to the project, please read the [contribution guide](https://github.com/alfasoftware/morf/wiki/Contributing).
