# Requirements Document

## Introduction

This feature adds support for PostgreSQL 15's native MERGE command to the Morf library. Currently, Morf uses `INSERT INTO...ON CONFLICT` as an equivalent to MERGE statements for PostgreSQL, which has sub-optimal performance. PostgreSQL 15 introduced a dedicated MERGE command that provides better performance and more standard SQL compliance. This feature will implement support for the native MERGE command while maintaining backward compatibility with older PostgreSQL versions.

## Glossary

- **Morf**: A Java library for cross-platform evolutionary relational database mechanics
- **MERGE Statement**: A SQL statement that performs INSERT or UPDATE operations based on whether a matching record exists
- **PostgreSQL Dialect**: The database-specific implementation of SQL generation for PostgreSQL in Morf
- **INSERT...ON CONFLICT**: PostgreSQL's upsert syntax used as a MERGE equivalent in versions prior to 15
- **SqlDialect**: The abstract base class in Morf that defines database-specific SQL generation behavior
- **MergeStatement**: The Morf DSL representation of a MERGE operation
- **Database Version Detection**: The mechanism to determine which PostgreSQL version is being used

## Requirements

### Requirement 1

**User Story:** As a developer using Morf with PostgreSQL 15+, I want MERGE statements to use the native MERGE command, so that I can benefit from improved performance and standard SQL compliance.

#### Acceptance Criteria

1. WHEN the PostgreSQL version is 15 or higher THEN the system SHALL generate SQL using the native MERGE command syntax
2. WHEN generating a MERGE statement THEN the system SHALL include the INTO clause with the target table name
3. WHEN generating a MERGE statement THEN the system SHALL include the USING clause with the source query
4. WHEN generating a MERGE statement THEN the system SHALL include the ON clause with the join condition based on table unique keys
5. WHEN generating a MERGE statement with non-key fields THEN the system SHALL include a WHEN MATCHED clause with UPDATE SET assignments

### Requirement 2

**User Story:** As a developer using Morf with PostgreSQL versions prior to 15, I want MERGE statements to continue using INSERT...ON CONFLICT, so that my existing code continues to work without modification.

#### Acceptance Criteria

1. WHEN the PostgreSQL version is below 15 THEN the system SHALL generate SQL using INSERT INTO...ON CONFLICT syntax
2. WHEN the PostgreSQL version is below 15 THEN the system SHALL maintain the current behavior for all MERGE operations
3. WHEN switching between PostgreSQL versions THEN the system SHALL automatically select the appropriate SQL syntax without code changes

### Requirement 3

**User Story:** As a developer using Morf, I want MERGE statements to handle the WHEN MATCHED clause correctly, so that conditional updates work as expected.

#### Acceptance Criteria

1. WHEN a MergeStatement contains a whenMatchedAction with UPDATE and a WHERE clause THEN the system SHALL include the WHERE condition in the WHEN MATCHED clause
2. WHEN a MergeStatement contains only key fields THEN the system SHALL include a WHEN NOT MATCHED clause with INSERT
3. WHEN a MergeStatement has no non-key fields to update THEN the system SHALL omit the WHEN MATCHED clause

### Requirement 4

**User Story:** As a developer using Morf, I want field references in MERGE updates to work correctly, so that I can reference both source and target values.

#### Acceptance Criteria

1. WHEN generating UPDATE assignments in a MERGE statement THEN the system SHALL reference source values correctly
2. WHEN an InputField is used in a MERGE statement THEN the system SHALL generate the appropriate source table reference
3. WHEN generating a MERGE statement THEN the system SHALL ensure all field names are properly qualified

### Requirement 5

**User Story:** As a developer, I want the system to detect the PostgreSQL version at runtime, so that the correct MERGE syntax is used automatically.

#### Acceptance Criteria

1. WHEN connecting to a PostgreSQL database THEN the system SHALL retrieve the database major version number
2. WHEN the database major version is available THEN the system SHALL store it for SQL generation decisions
3. WHEN the database major version cannot be determined THEN the system SHALL default to the INSERT...ON CONFLICT syntax

### Requirement 6

**User Story:** As a developer, I want comprehensive test coverage for the MERGE command implementation, so that I can be confident the feature works correctly across different scenarios.

#### Acceptance Criteria

1. WHEN testing MERGE statement generation THEN the system SHALL verify correct SQL syntax for PostgreSQL 15+
2. WHEN testing MERGE statement generation THEN the system SHALL verify backward compatibility with INSERT...ON CONFLICT
3. WHEN testing with different MergeStatement configurations THEN the system SHALL verify all clauses are generated correctly
4. WHEN testing field references THEN the system SHALL verify source and target field qualification
5. WHEN testing version detection THEN the system SHALL verify correct behavior for different PostgreSQL versions
