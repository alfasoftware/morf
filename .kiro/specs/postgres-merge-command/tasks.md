# Implementation Plan

- [x] 1. Extract existing INSERT...ON CONFLICT logic into separate method




  - [x] 1.1 Refactor current getSqlFrom(MergeStatement) implementation


    - Extract existing logic into `generateInsertOnConflictSql(MergeStatement)` method
    - Ensure no behavioral changes to existing functionality
    - _Requirements: 2.1, 2.2_
  - [x] 1.2 Write unit tests for extracted method


    - Test various MergeStatement configurations
    - Verify output matches current behavior
    - _Requirements: 2.1, 2.2_

- [x] 2. Implement version detection helper method in PostgreSQLDialect




  - [x] 2.1 Add method to check if native MERGE should be used based on PostgreSQL version


    - Create `shouldUseNativeMerge()` method that checks version >= 15
    - Handle case when version information is unavailable (default to false)
    - Access version via PostgreSQLMetaDataProvider from SchemaResource
    - _Requirements: 5.1, 5.2, 5.3_
  - [x] 2.2 Write unit tests for version detection


    - Test with version 15+ returns true
    - Test with version <15 returns false
    - Test with missing version returns false
    - _Requirements: 5.1, 5.2, 5.3_

- [x] 3. Implement native MERGE SQL generation method








  - [x] 3.1 Create generateNativeMergeSql(MergeStatement) method


    - Generate MERGE INTO clause with target table
    - Generate USING clause with source query and alias "s"
    - Generate ON clause with join conditions from tableUniqueKey
    - Generate WHEN MATCHED clause with UPDATE SET assignments (if non-key fields exist)
    - Generate WHEN NOT MATCHED clause with INSERT
    - Handle whenMatchedAction with WHERE clause
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 3.1, 3.2, 3.3_
  - [x] 3.2 Write unit tests for native MERGE SQL generation


    - Test basic MERGE structure with all clauses
    - Test MERGE with whenMatchedAction and WHERE clause
    - Test MERGE with only key fields (no WHEN MATCHED)
    - Test field name qualification
    - _Requirements: 1.2, 1.3, 1.4, 1.5, 3.1, 3.2_

- [ ] 4. Update getSqlFrom(MergeStatement) to use version-aware generation
  - [ ] 4.1 Modify main getSqlFrom(MergeStatement) method
    - Call shouldUseNativeMerge() to determine which method to use
    - Route to generateNativeMergeSql() or generateInsertOnConflictSql()
    - Ensure proper error handling
    - _Requirements: 1.1, 2.1, 2.3, 5.1, 5.2, 5.3_
  - [ ] 4.2 Write unit tests for routing logic
    - Test with version 15+ routes to native MERGE
    - Test with version <15 routes to INSERT...ON CONFLICT
    - Test with missing version defaults to INSERT...ON CONFLICT
    - _Requirements: 1.1, 2.1, 2.3, 5.3_

- [ ] 5. Handle InputField references for native MERGE
  - [ ] 5.1 Add context tracking for MERGE syntax mode
    - Add instance variable or thread-local to track current MERGE mode
    - Set mode before generating MERGE SQL
    - _Requirements: 4.1, 4.2, 4.3_
  - [ ] 5.2 Update getSqlFrom(MergeStatement.InputField) for native MERGE
    - Check current MERGE mode
    - Return "s.fieldName" for native MERGE
    - Return "EXCLUDED.fieldName" for INSERT...ON CONFLICT
    - _Requirements: 4.1, 4.2, 4.3_
  - [ ] 5.3 Write unit tests for InputField handling
    - Test InputField generates "s.field" in native MERGE mode
    - Test InputField generates "EXCLUDED.field" in INSERT...ON CONFLICT mode
    - _Requirements: 4.1, 4.2, 4.3_

- [ ] 6. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 7. Update existing PostgreSQLDialect tests
  - [ ] 7.1 Update TestPostgreSQLDialect
    - Add test cases for PostgreSQL 15+ scenarios
    - Ensure existing MERGE tests still pass
    - Add tests for version-specific behavior
    - _Requirements: 1.1, 2.1, 2.2, 2.3_

- [ ] 8. Add integration tests
  - [ ] 8.1 Create integration tests for native MERGE
    - Test actual MERGE execution against PostgreSQL 15+ (if available in CI)
    - Verify data correctness after MERGE operations
    - Test backward compatibility with PostgreSQL 14
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [ ] 9. Update documentation
  - [ ] 9.1 Add JavaDoc comments
    - Document new methods in PostgreSQLDialect
    - Document version detection behavior
    - Document fallback behavior
    - _Requirements: 5.3_

- [ ] 10. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.
