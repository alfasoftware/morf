package org.alfasoftware.morf.jdbc.postgresql;

import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.sql.MergeMatchClause;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.TableReference;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Comprehensive tests for PostgreSQL MERGE statement compatibility across versions.
 * Tests verify that:
 * - PostgreSQL 15+ uses native MERGE syntax
 * - PostgreSQL <15 uses INSERT...ON CONFLICT syntax
 * - Existing MERGE tests still pass with default behavior
 * - Version-specific behavior works correctly
 *
 * Requirements: 1.1, 2.1, 2.2, 2.3
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
public class TestPostgreSQLDialectMergeCompatibility {

  /**
   * Test that MERGE statements use native MERGE syntax for PostgreSQL 15+
   * Requirements: 1.1, 2.1, 2.2, 2.3
   */
  @Test
  public void testMergeUsesNativeSyntaxForPostgreSQL15() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "15")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialectV15 = new PostgreSQLDialect("testschema");
    dialectV15.setSchemaResource(schemaResource);

    TableReference foo = tableRef("foo");
    TableReference somewhere = tableRef("somewhere");
    
    MergeStatement stmt = MergeStatement.merge()
        .into(foo)
        .tableUniqueKey(field("id"))
        .from(new SelectStatement(
            somewhere.field("newId").as("id"),
            somewhere.field("newBar").as("bar")
        ).from(somewhere))
        .build();

    // When
    String sql = dialectV15.convertStatementToSQL(stmt);

    // Then
    assertTrue("Should use native MERGE syntax for PostgreSQL 15", 
        sql.contains("MERGE INTO"));
    assertFalse("Should not use INSERT...ON CONFLICT syntax", 
        sql.contains("INSERT INTO"));
  }


  /**
   * Test that MERGE statements use INSERT...ON CONFLICT for PostgreSQL 14
   * Requirements: 1.1, 2.1, 2.2, 2.3
   */
  @Test
  public void testMergeUsesInsertOnConflictForPostgreSQL14() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "14")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialectV14 = new PostgreSQLDialect("testschema");
    dialectV14.setSchemaResource(schemaResource);

    TableReference foo = tableRef("foo");
    TableReference somewhere = tableRef("somewhere");
    
    MergeStatement stmt = MergeStatement.merge()
        .into(foo)
        .tableUniqueKey(field("id"))
        .from(new SelectStatement(
            somewhere.field("newId").as("id"),
            somewhere.field("newBar").as("bar")
        ).from(somewhere))
        .build();

    // When
    String sql = dialectV14.convertStatementToSQL(stmt);

    // Then
    assertTrue("Should use INSERT...ON CONFLICT syntax for PostgreSQL 14", 
        sql.contains("INSERT INTO"));
    assertTrue("Should contain ON CONFLICT clause", 
        sql.contains("ON CONFLICT"));
    assertFalse("Should not use native MERGE syntax", 
        sql.contains("MERGE INTO"));
  }


  /**
   * Test that MERGE defaults to INSERT...ON CONFLICT when version is not available
   * Requirements: 1.1, 2.1, 2.2, 2.3
   */
  @Test
  public void testMergeDefaultsToInsertOnConflictWhenVersionUnavailable() {
    // Given - dialect without version information
    PostgreSQLDialect dialectNoVersion = new PostgreSQLDialect("testschema");

    TableReference foo = tableRef("foo");
    TableReference somewhere = tableRef("somewhere");
    
    MergeStatement stmt = MergeStatement.merge()
        .into(foo)
        .tableUniqueKey(field("id"))
        .from(new SelectStatement(
            somewhere.field("newId").as("id"),
            somewhere.field("newBar").as("bar")
        ).from(somewhere))
        .build();

    // When
    String sql = dialectNoVersion.convertStatementToSQL(stmt);

    // Then
    assertTrue("Should default to INSERT...ON CONFLICT when version unavailable", 
        sql.contains("INSERT INTO"));
    assertTrue("Should contain ON CONFLICT clause", 
        sql.contains("ON CONFLICT"));
    assertFalse("Should not use native MERGE syntax", 
        sql.contains("MERGE INTO"));
  }


  /**
   * Test that native MERGE syntax includes all required clauses for PostgreSQL 15+
   * Requirements: 1.1, 1.2, 1.3, 1.4, 1.5
   */
  @Test
  public void testNativeMergeSyntaxStructureForPostgreSQL15() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "15")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialectV15 = new PostgreSQLDialect("testschema");
    dialectV15.setSchemaResource(schemaResource);

    TableReference foo = tableRef("foo");
    TableReference somewhere = tableRef("somewhere");
    
    MergeStatement stmt = MergeStatement.merge()
        .into(foo)
        .tableUniqueKey(field("id"))
        .from(new SelectStatement(
            somewhere.field("newId").as("id"),
            somewhere.field("newBar").as("bar")
        ).from(somewhere))
        .build();

    // When
    String sql = dialectV15.convertStatementToSQL(stmt);

    // Then
    assertTrue("Should contain MERGE INTO clause", sql.contains("MERGE INTO"));
    assertTrue("Should contain USING clause", sql.contains("USING"));
    assertTrue("Should contain ON clause", sql.contains("ON"));
    assertTrue("Should contain WHEN MATCHED", sql.contains("WHEN MATCHED"));
    assertTrue("Should contain WHEN NOT MATCHED", sql.contains("WHEN NOT MATCHED"));
  }


  /**
   * Test that existing MERGE behavior is preserved for backward compatibility
   * This verifies that the default behavior (without version info) produces INSERT...ON CONFLICT
   * Requirements: 2.1, 2.2, 2.3
   */
  @Test
  public void testExistingMergeTestsStillPassWithDefaultBehavior() {
    // Given - using a dialect without version information (default behavior)
    PostgreSQLDialect dialect = new PostgreSQLDialect("testschema");

    TableReference foo = tableRef("foo");
    TableReference somewhere = tableRef("somewhere");
    
    MergeStatement stmt = MergeStatement.merge()
        .into(foo)
        .tableUniqueKey(field("id"))
        .from(new SelectStatement(
            somewhere.field("newId").as("id"),
            somewhere.field("newBar").as("bar")
        ).from(somewhere))
        .build();

    // When
    String sql = dialect.convertStatementToSQL(stmt);

    // Then - should match the expected INSERT...ON CONFLICT syntax
    String expected = "INSERT INTO testschema.foo (id, bar)"
        + " SELECT somewhere.newId AS id, somewhere.newBar AS bar FROM testschema.somewhere"
        + " ON CONFLICT (id)"
        + " DO UPDATE SET bar = EXCLUDED.bar";
    
    assertEquals("Existing MERGE tests should still produce INSERT...ON CONFLICT syntax", 
        expected, sql);
  }


  /**
   * Test complex MERGE with whenMatchedAction for PostgreSQL 15+
   * Requirements: 1.1, 1.5, 3.1
   */
  @Test
  public void testComplexMergeWithWhenMatchedActionForPostgreSQL15() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "15")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialectV15 = new PostgreSQLDialect("testschema");
    dialectV15.setSchemaResource(schemaResource);

    TableReference foo = tableRef("foo");
    
    SelectStatement sourceStmt = new SelectStatement(
        literal(1).as("id"),
        literal(100).as("bar")
    ).from(tableRef("dual"));
    
    Criterion whereCondition = field("bar").isNotNull();
    
    MergeStatement stmt = MergeStatement.merge()
        .into(foo)
        .tableUniqueKey(field("id"))
        .from(sourceStmt)
        .whenMatched(MergeMatchClause.update().onlyWhere(whereCondition).build())
        .build();

    // When
    String sql = dialectV15.convertStatementToSQL(stmt);

    // Then
    assertTrue("Should use native MERGE syntax", sql.contains("MERGE INTO"));
    assertTrue("Should contain WHEN MATCHED with condition", 
        sql.contains("WHEN MATCHED") && (sql.contains("AND") || sql.contains("bar IS NOT NULL")));
  }


  /**
   * Test MERGE with multiple key fields for PostgreSQL 15+
   * Requirements: 1.1, 1.4
   */
  @Test
  public void testMergeWithMultipleKeyFieldsForPostgreSQL15() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "15")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialectV15 = new PostgreSQLDialect("testschema");
    dialectV15.setSchemaResource(schemaResource);

    TableReference foo = tableRef("foo");
    TableReference somewhere = tableRef("somewhere");
    
    MergeStatement stmt = MergeStatement.merge()
        .into(foo)
        .tableUniqueKey(field("id"), field("type"))
        .from(new SelectStatement(
            somewhere.field("newId").as("id"),
            somewhere.field("newType").as("type"),
            somewhere.field("newBar").as("bar")
        ).from(somewhere))
        .build();

    // When
    String sql = dialectV15.convertStatementToSQL(stmt);

    // Then
    assertTrue("Should use native MERGE syntax", sql.contains("MERGE INTO"));
    assertTrue("ON clause should contain both key fields", 
        sql.contains("t.id = s.id") && sql.contains("t.type = s.type"));
  }


  /**
   * Test that PostgreSQL 16+ also uses native MERGE syntax
   * Requirements: 1.1, 2.1, 2.3
   */
  @Test
  public void testMergeUsesNativeSyntaxForPostgreSQL16() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "16")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialectV16 = new PostgreSQLDialect("testschema");
    dialectV16.setSchemaResource(schemaResource);

    TableReference foo = tableRef("foo");
    TableReference somewhere = tableRef("somewhere");
    
    MergeStatement stmt = MergeStatement.merge()
        .into(foo)
        .tableUniqueKey(field("id"))
        .from(new SelectStatement(
            somewhere.field("newId").as("id"),
            somewhere.field("newBar").as("bar")
        ).from(somewhere))
        .build();

    // When
    String sql = dialectV16.convertStatementToSQL(stmt);

    // Then
    assertTrue("Should use native MERGE syntax for PostgreSQL 16", 
        sql.contains("MERGE INTO"));
    assertFalse("Should not use INSERT...ON CONFLICT syntax", 
        sql.contains("INSERT INTO"));
  }


  /**
   * Test that PostgreSQL 13 uses INSERT...ON CONFLICT syntax
   * Requirements: 2.1, 2.2, 2.3
   */
  @Test
  public void testMergeUsesInsertOnConflictForPostgreSQL13() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "13")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialectV13 = new PostgreSQLDialect("testschema");
    dialectV13.setSchemaResource(schemaResource);

    TableReference foo = tableRef("foo");
    TableReference somewhere = tableRef("somewhere");
    
    MergeStatement stmt = MergeStatement.merge()
        .into(foo)
        .tableUniqueKey(field("id"))
        .from(new SelectStatement(
            somewhere.field("newId").as("id"),
            somewhere.field("newBar").as("bar")
        ).from(somewhere))
        .build();

    // When
    String sql = dialectV13.convertStatementToSQL(stmt);

    // Then
    assertTrue("Should use INSERT...ON CONFLICT syntax for PostgreSQL 13", 
        sql.contains("INSERT INTO"));
    assertTrue("Should contain ON CONFLICT clause", 
        sql.contains("ON CONFLICT"));
    assertFalse("Should not use native MERGE syntax", 
        sql.contains("MERGE INTO"));
  }
}
