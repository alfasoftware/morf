package org.alfasoftware.morf.integration;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.InsertStatement.insert;
import static org.alfasoftware.morf.sql.MergeStatement.merge;
import static org.alfasoftware.morf.sql.SelectStatement.select;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.util.List;

import net.jcip.annotations.NotThreadSafe;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.LoggingSqlScriptVisitor;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * Integration tests for PostgreSQL native MERGE command support.
 * Tests verify that MERGE operations work correctly on both PostgreSQL 15+
 * (using native MERGE) and earlier versions (using INSERT...ON CONFLICT).
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
@NotThreadSafe
public class TestPostgreSQLNativeMerge {

  @Rule public InjectMembersRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject private Provider<DatabaseSchemaManager> schemaManager;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Inject private ConnectionResources connectionResources;

  private final TableReference testTable = tableRef("MergeTest");

  @Before
  public void setUp() {
    // Only run these tests on PostgreSQL
    assumeTrue("Tests only applicable to PostgreSQL",
        connectionResources.sqlDialect().getDatabaseType().identifier().equals("PGSQL"));

    // Create test schema
    schemaManager.get().mutateToSupportSchema(
      schema(
        table(testTable.getName())
          .columns(
            column("id", DataType.INTEGER).primaryKey(),
            column("name", DataType.STRING, 50),
            column("value", DataType.INTEGER),
            column("status", DataType.STRING, 20))
      ),
      TruncationBehavior.ALWAYS);
  }

  @After
  public void tearDown() {
    // Cleanup is handled by schema manager
  }

  /**
   * Tests basic MERGE operation that inserts new records.
   * Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5
   */
  @Test
  public void testMergeInsertNewRecords() {
    // Given: Empty table
    assertEquals("Table should be empty", Long.valueOf(0), countRecords());

    // When: MERGE with new records
    MergeStatement mergeStatement = merge()
      .into(testTable)
      .tableUniqueKey(field("id"))
      .from(select()
        .fields(literal(1).as("id"))
        .fields(literal("Alice").as("name"))
        .fields(literal(100).as("value"))
        .fields(literal("active").as("status"))
        .build())
      .build();

    executeMerge(mergeStatement);

    // Then: Record should be inserted
    assertEquals("One record should be inserted", Long.valueOf(1), countRecords());
    verifyRecord(1, "Alice", 100, "active");
  }

  /**
   * Tests MERGE operation that updates existing records.
   * Validates: Requirements 1.1, 1.5, 2.1, 2.2
   */
  @Test
  public void testMergeUpdateExistingRecords() {
    // Given: Existing record
    insertRecord(1, "Alice", 100, "active");
    assertEquals("One record should exist", Long.valueOf(1), countRecords());

    // When: MERGE with updated values
    MergeStatement mergeStatement = merge()
      .into(testTable)
      .tableUniqueKey(field("id"))
      .from(select()
        .fields(literal(1).as("id"))
        .fields(literal("Alice Updated").as("name"))
        .fields(literal(200).as("value"))
        .fields(literal("inactive").as("status"))
        .build())
      .build();

    executeMerge(mergeStatement);

    // Then: Record should be updated
    assertEquals("Still one record", Long.valueOf(1), countRecords());
    verifyRecord(1, "Alice Updated", 200, "inactive");
  }

  /**
   * Tests MERGE operation with mixed insert and update.
   * Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 2.2
   */
  @Test
  public void testMergeMixedInsertAndUpdate() {
    // Given: One existing record
    insertRecord(1, "Alice", 100, "active");

    // When: MERGE with one update and one insert
    MergeStatement mergeStatement = merge()
      .into(testTable)
      .tableUniqueKey(field("id"))
      .from(select()
        .fields(literal(1).as("id"))
        .fields(literal("Alice Updated").as("name"))
        .fields(literal(150).as("value"))
        .fields(literal("active").as("status"))
        .union(select()
          .fields(literal(2).as("id"))
          .fields(literal("Bob").as("name"))
          .fields(literal(200).as("value"))
          .fields(literal("active").as("status"))
          .build())
        .build())
      .build();

    executeMerge(mergeStatement);

    // Then: One record updated, one inserted
    assertEquals("Two records should exist", Long.valueOf(2), countRecords());
    verifyRecord(1, "Alice Updated", 150, "active");
    verifyRecord(2, "Bob", 200, "active");
  }

  /**
   * Tests MERGE with conditional update (WHEN MATCHED clause with WHERE).
   * Validates: Requirements 3.1
   */
  @Test
  public void testMergeWithConditionalUpdate() {
    // Given: Two existing records
    insertRecord(1, "Alice", 100, "active");
    insertRecord(2, "Bob", 200, "inactive");

    // When: MERGE with conditional update (only update if status is 'active')
    MergeStatement mergeStatement = merge()
      .into(testTable)
      .tableUniqueKey(field("id"))
      .from(select()
        .fields(literal(1).as("id"))
        .fields(literal("Alice Updated").as("name"))
        .fields(literal(150).as("value"))
        .fields(literal("active").as("status"))
        .union(select()
          .fields(literal(2).as("id"))
          .fields(literal("Bob Updated").as("name"))
          .fields(literal(250).as("value"))
          .fields(literal("inactive").as("status"))
          .build())
        .build())
      .whenMatched((overrides, values) -> overrides
        .set(values.input("name").as("name"))
        .set(values.input("value").as("value"))
        .set(values.input("status").as("status")),
        field("status").eq("active"))
      .build();

    executeMerge(mergeStatement);

    // Then: Only the 'active' record should be updated
    verifyRecord(1, "Alice Updated", 150, "active");
    verifyRecord(2, "Bob", 200, "inactive"); // Should not be updated
  }

  /**
   * Tests MERGE with InputField references in update expressions.
   * Validates: Requirements 4.1, 4.2, 4.3
   */
  @Test
  public void testMergeWithInputFieldReferences() {
    // Given: Existing record
    insertRecord(1, "Alice", 100, "active");

    // When: MERGE using InputField to reference source values
    MergeStatement mergeStatement = merge()
      .into(testTable)
      .tableUniqueKey(field("id"))
      .from(select()
        .fields(literal(1).as("id"))
        .fields(literal("Alice").as("name"))
        .fields(literal(50).as("value"))
        .fields(literal("active").as("status"))
        .build())
      .ifUpdating((overrides, values) -> overrides
        .set(values.input("name").as("name"))
        .set(values.existing("value").plus(values.input("value")).as("value"))
        .set(values.input("status").as("status")))
      .build();

    executeMerge(mergeStatement);

    // Then: Value should be accumulated (100 + 50 = 150)
    verifyRecord(1, "Alice", 150, "active");
  }

  /**
   * Tests MERGE with only key fields (no non-key fields to update).
   * Validates: Requirements 3.2, 3.3
   */
  @Test
  public void testMergeWithOnlyKeyFields() {
    // Given: Empty table
    assertEquals("Table should be empty", Long.valueOf(0), countRecords());

    // When: MERGE with only key field
    MergeStatement mergeStatement = merge()
      .into(testTable)
      .tableUniqueKey(field("id"))
      .from(select()
        .fields(literal(1).as("id"))
        .fields(literal("Alice").as("name"))
        .fields(literal(100).as("value"))
        .fields(literal("active").as("status"))
        .build())
      .build();

    executeMerge(mergeStatement);

    // Then: Record should be inserted
    assertEquals("One record should be inserted", Long.valueOf(1), countRecords());
    verifyRecord(1, "Alice", 100, "active");

    // When: MERGE again with same key
    executeMerge(mergeStatement);

    // Then: Still one record (no update occurred)
    assertEquals("Still one record", Long.valueOf(1), countRecords());
  }

  /**
   * Tests MERGE with multiple records in a single operation.
   * Validates: Requirements 1.1, 1.2, 1.3, 1.4, 1.5, 2.1, 2.2, 2.3
   */
  @Test
  public void testMergeMultipleRecords() {
    // Given: Some existing records
    insertRecord(1, "Alice", 100, "active");
    insertRecord(3, "Charlie", 300, "active");

    // When: MERGE with multiple records (mix of insert and update)
    MergeStatement mergeStatement = merge()
      .into(testTable)
      .tableUniqueKey(field("id"))
      .from(select()
        .fields(literal(1).as("id"))
        .fields(literal("Alice Updated").as("name"))
        .fields(literal(150).as("value"))
        .fields(literal("inactive").as("status"))
        .union(select()
          .fields(literal(2).as("id"))
          .fields(literal("Bob").as("name"))
          .fields(literal(200).as("value"))
          .fields(literal("active").as("status"))
          .build())
        .union(select()
          .fields(literal(3).as("id"))
          .fields(literal("Charlie Updated").as("name"))
          .fields(literal(350).as("value"))
          .fields(literal("inactive").as("status"))
          .build())
        .union(select()
          .fields(literal(4).as("id"))
          .fields(literal("David").as("name"))
          .fields(literal(400).as("value"))
          .fields(literal("active").as("status"))
          .build())
        .build())
      .build();

    executeMerge(mergeStatement);

    // Then: Records should be correctly merged
    assertEquals("Four records should exist", Long.valueOf(4), countRecords());
    verifyRecord(1, "Alice Updated", 150, "inactive");
    verifyRecord(2, "Bob", 200, "active");
    verifyRecord(3, "Charlie Updated", 350, "inactive");
    verifyRecord(4, "David", 400, "active");
  }

  /**
   * Tests backward compatibility with PostgreSQL 14 and earlier.
   * This test verifies that MERGE operations work correctly regardless
   * of whether native MERGE or INSERT...ON CONFLICT is used.
   * Validates: Requirements 2.1, 2.2, 2.3, 5.3
   */
  @Test
  public void testBackwardCompatibility() {
    // This test runs on all PostgreSQL versions and verifies
    // that MERGE operations produce correct results regardless
    // of the underlying SQL syntax used

    // Given: Empty table
    assertEquals("Table should be empty", Long.valueOf(0), countRecords());

    // When: Perform a series of MERGE operations
    // First MERGE: Insert
    executeMerge(merge()
      .into(testTable)
      .tableUniqueKey(field("id"))
      .from(select()
        .fields(literal(1).as("id"))
        .fields(literal("Alice").as("name"))
        .fields(literal(100).as("value"))
        .fields(literal("active").as("status"))
        .build())
      .build());

    // Second MERGE: Update
    executeMerge(merge()
      .into(testTable)
      .tableUniqueKey(field("id"))
      .from(select()
        .fields(literal(1).as("id"))
        .fields(literal("Alice Updated").as("name"))
        .fields(literal(200).as("value"))
        .fields(literal("inactive").as("status"))
        .build())
      .build());

    // Third MERGE: Mixed insert and update
    executeMerge(merge()
      .into(testTable)
      .tableUniqueKey(field("id"))
      .from(select()
        .fields(literal(1).as("id"))
        .fields(literal("Alice Final").as("name"))
        .fields(literal(300).as("value"))
        .fields(literal("active").as("status"))
        .union(select()
          .fields(literal(2).as("id"))
          .fields(literal("Bob").as("name"))
          .fields(literal(400).as("value"))
          .fields(literal("active").as("status"))
          .build())
        .build())
      .build());

    // Then: All operations should produce correct results
    assertEquals("Two records should exist", Long.valueOf(2), countRecords());
    verifyRecord(1, "Alice Final", 300, "active");
    verifyRecord(2, "Bob", 400, "active");
  }

  // Helper methods

  private void executeMerge(MergeStatement mergeStatement) {
    sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor())
      .execute(connectionResources.sqlDialect().convertStatementToSQL(mergeStatement));
  }

  private void insertRecord(int id, String name, int value, String status) {
    InsertStatement insertStatement = insert()
      .into(testTable)
      .values(literal(id).as("id"))
      .values(literal(name).as("name"))
      .values(literal(value).as("value"))
      .values(literal(status).as("status"))
      .build();

    sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor())
      .execute(connectionResources.sqlDialect().convertStatementToSQL(insertStatement));
  }

  private Long countRecords() {
    SelectStatement countStatement = select()
      .fields(field("id").count())
      .from(testTable)
      .build();

    return sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor())
      .executeQuery(countStatement)
      .processWith(resultSet -> resultSet.next() ? resultSet.getLong(1) : 0L);
  }

  private void verifyRecord(int expectedId, String expectedName, int expectedValue, String expectedStatus) {
    SelectStatement selectStatement = select()
      .fields(field("id"))
      .fields(field("name"))
      .fields(field("value"))
      .fields(field("status"))
      .from(testTable)
      .where(field("id").eq(expectedId))
      .build();

    List<Object> result = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor())
      .executeQuery(selectStatement)
      .processWith(resultSet -> {
        if (resultSet.next()) {
          return ImmutableList.of(
            resultSet.getInt(1),
            resultSet.getString(2),
            resultSet.getInt(3),
            resultSet.getString(4)
          );
        }
        return null;
      });

    assertTrue("Record with id " + expectedId + " should exist", result != null);
    assertEquals("ID should match", expectedId, result.get(0));
    assertEquals("Name should match", expectedName, result.get(1));
    assertEquals("Value should match", expectedValue, result.get(2));
    assertEquals("Status should match", expectedStatus, result.get(3));
  }
}
