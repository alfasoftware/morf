# Design Document

## Overview

This design implements native MERGE command support for PostgreSQL 15+ in the Morf library. The implementation will detect the PostgreSQL version at runtime and generate either the native MERGE syntax (for version 15+) or the existing INSERT...ON CONFLICT syntax (for earlier versions). This approach ensures optimal performance on newer PostgreSQL versions while maintaining backward compatibility.

The key insight is that PostgreSQL 15 introduced a standard SQL MERGE command that is more performant and feature-complete than the INSERT...ON CONFLICT workaround. By detecting the database version and conditionally generating the appropriate SQL, we can provide the best experience for all users.

## Architecture

The implementation follows Morf's existing architecture pattern where database-specific SQL generation is handled by dialect classes that extend `SqlDialect`. The PostgreSQL-specific logic resides in `PostgreSQLDialect`.

### Component Interaction

```
MergeStatement (DSL)
       ↓
PostgreSQLDialect.getSqlFrom(MergeStatement)
       ↓
Version Detection (via PostgreSQLMetaDataProvider)
       ↓
       ├─→ PostgreSQL 15+: Generate native MERGE syntax
       └─→ PostgreSQL <15: Generate INSERT...ON CONFLICT syntax
```

### Version Detection Strategy

The PostgreSQL version will be accessed through the `PostgreSQLMetaDataProvider` which already provides database version information via the `getDatabaseInformation()` method. This information is populated from JDBC metadata when the connection is established.

## Components and Interfaces

### Modified Components

#### PostgreSQLDialect

The `PostgreSQLDialect` class will be modified to:
1. Accept an optional `PostgreSQLMetaDataProvider` reference for version detection
2. Implement version-aware MERGE SQL generation in the `getSqlFrom(MergeStatement)` method
3. Maintain the existing INSERT...ON CONFLICT generation as a fallback

Key methods:
- `getSqlFrom(MergeStatement statement)` - Main entry point for MERGE SQL generation
- `generateNativeMergeSql(MergeStatement statement)` - New method for PostgreSQL 15+ MERGE syntax
- `generateInsertOnConflictSql(MergeStatement statement)` - Extracted existing logic
- `shouldUseNativeMerge()` - Version detection logic

### New Helper Methods

#### generateNativeMergeSql

Generates SQL in the format:
```sql
MERGE INTO target_table AS t
USING (source_query) AS s
ON (join_condition)
WHEN MATCHED [AND condition] THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT (...) VALUES (...)
```

#### generateInsertOnConflictSql

Encapsulates the existing INSERT...ON CONFLICT logic (currently inline in `getSqlFrom`):
```sql
INSERT INTO target_table (columns)
source_query
ON CONFLICT (key_columns)
DO UPDATE SET ...
[WHERE condition]
```

## Data Models

No new data models are required. The existing `MergeStatement` class already contains all necessary information:
- `table`: Target table reference
- `selectStatement`: Source data query
- `tableUniqueKey`: Key fields for matching
- `ifUpdating`: Update expressions
- `whenMatchedAction`: Optional conditional update clause

## Correctness Properties

*A property is a characteristic or behavior that should hold true across all valid executions of a system-essentially, a formal statement about what the system should do. Properties serve as the bridge between human-readable specifications and machine-verifiable correctness guarantees.*

After analyzing the acceptance criteria, several properties are redundant or can be combined. The property reflection identified the following consolidations:

- Properties 1.2, 1.3, and 1.4 can be combined into a single comprehensive property about MERGE statement structure
- Properties 4.1, 4.2, and 4.3 can be combined into a single property about field reference correctness
- Property 2.2 is redundant with 2.1

### Property 1: Version-based syntax selection

*For any* MergeStatement, when the PostgreSQL version is 15 or higher, the generated SQL should use native MERGE syntax (containing "MERGE INTO"), and when the version is below 15, the generated SQL should use INSERT...ON CONFLICT syntax (containing "INSERT INTO" and "ON CONFLICT")

**Validates: Requirements 1.1, 2.1, 2.3**

### Property 2: Native MERGE structure completeness

*For any* MergeStatement when using PostgreSQL 15+, the generated SQL should contain all required clauses: INTO clause with the target table name, USING clause with the source query, and ON clause with join conditions based on the table unique keys

**Validates: Requirements 1.2, 1.3, 1.4**

### Property 3: Conditional WHEN MATCHED clause

*For any* MergeStatement with non-key fields, the generated SQL should include a WHEN MATCHED clause with UPDATE SET assignments, and if a whenMatchedAction with a WHERE clause is present, that WHERE condition should appear in the WHEN MATCHED clause

**Validates: Requirements 1.5, 3.1**

### Property 4: WHEN NOT MATCHED clause presence

*For any* MergeStatement, the generated SQL should include a WHEN NOT MATCHED clause with INSERT for all fields from the source query

**Validates: Requirements 3.2**

### Property 5: Field reference correctness

*For any* MergeStatement, all field references in the generated SQL should be properly qualified, with source values correctly referenced (as "EXCLUDED.field" for INSERT...ON CONFLICT or "s.field" for native MERGE), and all field names should be unambiguous

**Validates: Requirements 4.1, 4.2, 4.3**

## Error Handling

### Version Detection Failures

When the database version cannot be determined (e.g., connection issues, missing metadata), the system will default to the INSERT...ON CONFLICT syntax. This ensures backward compatibility and prevents failures in edge cases.

### Invalid MergeStatement Configurations

The existing validation in `MergeStatement` will continue to apply:
- Empty table names will throw `IllegalArgumentException`
- SELECT statements with ORDER BY will throw `IllegalArgumentException` (SQL Server limitation)
- Missing unique keys will result in SQL generation errors at execution time

### SQL Generation Errors

If SQL generation fails for any reason, the existing error handling mechanisms in `SqlDialect` will apply, propagating exceptions to the caller.

## Testing Strategy

### Unit Testing

Unit tests will verify specific scenarios:

1. **Version Detection Tests**
   - Test with PostgreSQL version 15 returns native MERGE syntax
   - Test with PostgreSQL version 14 returns INSERT...ON CONFLICT syntax
   - Test with missing version information defaults to INSERT...ON CONFLICT

2. **SQL Structure Tests**
   - Test native MERGE contains all required clauses (INTO, USING, ON, WHEN MATCHED, WHEN NOT MATCHED)
   - Test INSERT...ON CONFLICT maintains existing structure
   - Test field qualification in both syntaxes

3. **Edge Case Tests**
   - Test MERGE with only key fields (no updates)
   - Test MERGE with whenMatchedAction and WHERE clause
   - Test MERGE with InputField references

### Property-Based Testing

Property-based tests will verify universal properties across many randomly generated inputs. We will use **JUnit-Quickcheck** as the property-based testing library for Java, which integrates well with JUnit 4 (already used in Morf).

Each property-based test will:
- Run a minimum of 100 iterations
- Generate random MergeStatements with varying configurations
- Verify the correctness property holds for all generated inputs
- Be tagged with a comment explicitly referencing the design document property

**Property test tagging format:** `// Feature: postgres-merge-command, Property N: [property text]`

Property-based tests will cover:

1. **Property 1: Version-based syntax selection**
   - Generate random MergeStatements
   - Test with both version 15+ and version <15
   - Verify correct syntax is used based on version

2. **Property 2: Native MERGE structure completeness**
   - Generate random MergeStatements with various table and field configurations
   - Verify all required clauses are present in PostgreSQL 15+ output

3. **Property 3: Conditional WHEN MATCHED clause**
   - Generate random MergeStatements with and without non-key fields
   - Generate random whenMatchedAction configurations
   - Verify WHEN MATCHED clause appears correctly

4. **Property 4: WHEN NOT MATCHED clause presence**
   - Generate random MergeStatements
   - Verify WHEN NOT MATCHED clause is always present

5. **Property 5: Field reference correctness**
   - Generate random MergeStatements with various field configurations
   - Verify all field references are properly qualified
   - Verify InputField references use correct syntax for each version

### Integration Testing

Integration tests will verify the feature works end-to-end with actual PostgreSQL databases:
- Test against PostgreSQL 15+ with native MERGE
- Test against PostgreSQL 14 with INSERT...ON CONFLICT
- Verify data correctness after MERGE operations
- Verify performance improvements with native MERGE

## Implementation Notes

### Backward Compatibility

The implementation maintains 100% backward compatibility:
- Existing code using MergeStatement will continue to work unchanged
- PostgreSQL versions prior to 15 will use the existing INSERT...ON CONFLICT syntax
- No API changes are required

### Performance Considerations

The native MERGE command in PostgreSQL 15+ provides:
- Better query planning and optimization
- Reduced overhead compared to INSERT...ON CONFLICT
- More efficient handling of large datasets
- Standard SQL compliance

### Future Enhancements

Potential future improvements:
- Support for WHEN NOT MATCHED BY SOURCE clause (PostgreSQL 15+)
- Support for multiple WHEN MATCHED clauses with different conditions
- Performance benchmarking and optimization
- Support for MERGE with CTEs (Common Table Expressions)

## Dependencies

- JUnit 4: Existing test framework
- JUnit-Quickcheck: Property-based testing library (new dependency)
- Mockito: For mocking PostgreSQLMetaDataProvider in tests
- Existing Morf core dependencies (no changes)

## Migration Path

No migration is required. The feature is automatically enabled when:
1. Using PostgreSQL 15 or higher
2. Using MergeStatement in Morf code

Users on PostgreSQL 14 or earlier will continue to use INSERT...ON CONFLICT with no code changes r