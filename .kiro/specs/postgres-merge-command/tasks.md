# Implementation Plan

- [ ] 1. Add JUnit-Quickcheck dependency to morf-postgresql module
  - Add JUnit-Quickcheck dependency to morf-postgresql/pom.xml
  - Verify the dependency is compatible with existing JUnit 4 setup
  - _Requirements: 6.1, 6.2, 6.3, 6.4, 6.5_

- [ ] 2. Implement version detection helper method in PostgreSQLDialect
  - [ ] 2.1 Add method to check if native MERGE should be used based on PostgreSQL version
    - Create `shouldUseNativeMerge()` method that checks version >= 15
    - Handle case when version information is unavailable (default to false)
    - _Requirements: 5.1, 5.2, 5.3_
  - [ ]* 2.2 Write property test for version detection
    - **Property 1: Version-based syntax selection**
    - **Validates: Requirements 1.1, 2.1, 2.3**

- [ ] 3. Extract existing INSERT...ON CONFLICT logic into separate method
  - [ ] 3.1 Refactor current getSqlFrom(MergeStatement) implementation
    - Extract existing logic into `generateInsertOnConflictSql(MergeStatement)` method
    - Ensure no behavioral changes to existing functionality
    - _Requirements: 2.1, 2.2_
  - [ ]* 3.2 Write unit tests for extracted method
    - Test various MergeStatement configurations
    - Verify output matches current behavior
    - _Requirements: 2.1, 2.2_

- [ ] 4. Implement native MERGE SQL generation method
  - [ ] 4.1 Create generateNativeMergeSql(MergeStatement) method
    - Generate MERGE INTO clause with target table
    - Generate USING clause with source query and alias
    - Generate ON clause with join conditions from tableUniqueKey
    - Generate WHEN MATCHED clause with UPDATE SET assignments
    - Generate WHEN NOT MATCHED clause with INSERT
    - Handle whenMatchedAction with WHERE clause
    - Handle case with no non-key fields (DO NOTHING equivalent)
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5, 3.1, 3.2, 3.3_
  - [ ]* 4.2 Write property test for native MERGE structure
    - **Property 2: Native MERGE structure completeness**
    - **Validates: Requirements 1.2, 1.3, 1.4**
  - [ ]* 4.3 Write property test for WHEN MATCHED clause
    - **Property 3: Conditional WHEN MATCHED clause**
    - **Validates: Requirements 1.5, 3.1**
  - [ ]* 4.4 Write property test for WHEN NOT MATCHED clause
    - **Property 4: WHEN NOT MATCHED clause presence**
    - **Validates: Requirements 3.2**

- [ ] 5. Implement field reference handling for native MERGE
  - [ ] 5.1 Update getSqlFrom(MergeStatement.InputField) for native MERGE
    - Modify to return "s.fieldName" for native MERGE (instead of "EXCLUDED.fieldName")
    - Maintain "EXCLUDED.fieldName" for INSERT...ON CONFLICT
    - Add context awareness to determine which syntax to use
    - _Requirements: 4.1, 4.2, 4.3_
  - [ ]* 5.2 Write property test for field reference correctness
    - **Property 5: Field reference correctness**
    - **Validates: Requirements 4.1, 4.2, 4.3**

- [ ] 6. Update getSqlFrom(MergeStatement) to use version-aware generation
  - [ ] 6.1 Modify main getSqlFrom(MergeStatement) method
    - Call shouldUseNativeMerge() to determine which method to use
    - Route to generateNativeMergeSql() or generateInsertOnConflictSql()
    - Ensure proper error handling
    - _Requirements: 1.1, 2.1, 2.3, 5.1, 5.2, 5.3_
  - [ ]* 6.2 Write unit tests for routing logic
    - Test with version 15+ routes to native MERGE
    - Test with version <15 routes to INSERT...ON CONFLICT
    - Test with missing version defaults to INSERT...ON CONFLICT
    - _Requirements: 1.1, 2.1, 2.3, 5.3_

- [ ] 7. Add comprehensive unit tests for edge cases
  - [ ]* 7.1 Write unit tests for edge cases
    - Test MERGE with only key fields
    - Test MERGE with whenMatchedAction and WHERE clause
    - Test MERGE with InputField references in both syntaxes
    - Test MERGE with empty/null scenarios
    - Test MERGE with complex SELECT statements
    - _Requirements: 3.1, 3.2, 3.3, 4.1, 4.2, 4.3_

- [ ] 8. Update existing PostgreSQLDialect tests
  - [ ]* 8.1 Update TestPostgreSQLDialect
    - Add test cases for PostgreSQL 15+ scenarios
    - Ensure existing tests still pass
    - Add parameterized tests for version-specific behavior
    - _Requirements: 1.1, 2.1, 2.2, 2.3_

- [ ] 9. Checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.

- [ ] 10. Add integration tests
  - [ ]* 10.1 Create integration tests for native MERGE
    - Test actual MERGE execution against PostgreSQL 15+ (if available in CI)
    - Verify data correctness after MERGE operations
    - Compare performance between native MERGE and INSERT...ON CONFLICT
    - _Requirements: 1.1, 1.2, 1.3, 1.4, 1.5_

- [ ] 11. Update documentation
  - [ ]* 11.1 Add JavaDoc comments
    - Document new methods in PostgreSQLDialect
    - Document version detection behavior
    - Document fallback behavior
    - _Requirements: 5.3_
  - [ ]* 11.2 Update README or migration guide
    - Document PostgreSQL 15+ MERGE support
    - Explain automatic version detection
    - Note performance improvements
    - _Requirements: 1.1, 2.3_

- [ ] 12. Final checkpoint - Ensure all tests pass
  - Ensure all tests pass, ask the user if questions arise.
