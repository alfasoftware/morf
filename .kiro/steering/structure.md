# Project Structure

## Multi-Module Organization

Morf is organized as a Maven multi-module project with the following modules:

### Core Modules

- **morf-core**: Core library containing the main API and functionality
  - SQL DSL and statement builders
  - Metadata model (Schema, Table, Column, Index, View, Sequence)
  - Dataset API (producers, consumers, connectors)
  - Database upgrade framework
  - XML-based dataset serialization
  - JDBC abstractions and utilities

- **morf-testsupport**: Testing utilities and helpers
  - Test rules and annotations
  - Database schema managers for testing
  - Mock implementations
  - Testing data source modules

### Database-Specific Modules

Each database platform has its own module implementing platform-specific SQL dialects:

- **morf-h2**: H2 database support (in-memory and file-based)
- **morf-mysql**: MySQL/MariaDB support
- **morf-oracle**: Oracle database support
- **morf-postgresql**: PostgreSQL support
- **morf-sqlserver**: Microsoft SQL Server support

### Additional Modules

- **morf-excel**: Excel spreadsheet dataset support (read/write)
- **morf-integration-test**: Integration tests across all database platforms

## Package Structure

Standard Java package hierarchy under `org.alfasoftware.morf`:

- `metadata`: Schema metadata model and utilities
- `sql`: SQL DSL, statement builders, and SQL utilities
- `sql.element`: SQL expression elements (functions, operators, literals)
- `dataset`: Dataset API for data import/export
- `jdbc`: JDBC abstractions and database-specific implementations
- `xml`: XML-based dataset serialization
- `upgrade`: Database upgrade and migration framework
- `changelog`: Change tracking and audit
- `guicesupport`: Google Guice integration modules
- `util`: General utilities and helpers

## Standard Module Layout

Each module follows Maven standard directory layout:

```
module-name/
├── pom.xml
├── src/
│   ├── main/
│   │   ├── java/           # Production code
│   │   └── resources/      # Resources, config files
│   └── test/
│       ├── java/           # Test code
│       └── resources/      # Test resources
└── target/                 # Build output (gitignored)
```

## Configuration Files

- **etc/**: Shared configuration files
  - `checkstyle.xml`: Checkstyle rules
  - `spotbugs-exclude.xml`: SpotBugs exclusions
  - `log4j-maven.properties`: Maven test logging config

- **Root level**:
  - `pom.xml`: Parent POM with dependency management
  - `.gitignore`: Git ignore patterns
  - `.travis.yml`: CI/CD configuration
  - `LICENSE`: Apache 2.0 license
  - `CONTRIBUTING.md`: Contribution guidelines

## Service Provider Interface (SPI)

Database-specific modules use Java SPI for registration:
- Location: `src/main/resources/META-INF/services/`
- File: `org.alfasoftware.morf.jdbc.DatabaseType`
- Contains fully qualified class name of the database type implementation

## Naming Conventions

- **Interfaces**: Descriptive nouns (e.g., `Schema`, `Table`, `DataSetProducer`)
- **Implementations**: Often suffixed with `Bean` or `Impl` (e.g., `SchemaBean`, `TableBean`)
- **Builders**: Suffixed with `Builder` (e.g., `SelectStatementBuilder`)
- **Tests**: Prefixed with `Test` (e.g., `TestSchema`, `TestSqlDialect`)
- **Abstract classes**: Prefixed with `Abstract` (e.g., `AbstractSqlDialect`)
