package org.alfasoftware.morf.jdbc.postgresql;

import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.merge;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Tests for MERGE statement routing logic in {@link PostgreSQLDialect}.
 * Verifies that the correct SQL syntax (native MERGE vs INSERT...ON CONFLICT)
 * is used based on PostgreSQL version.
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
public class TestPostgreSQLDialectMergeRouting {

  private PostgreSQLDialect dialect;
  private MergeStatement testMergeStatement;

  @Before
  public void setUp() {
    dialect = new PostgreSQLDialect("testschema");

    // Create a simple MERGE statement for testing
    SelectStatement selectStatement = select(field("id"), field("value"))
        .from(tableRef("source"));

    testMergeStatement = merge()
        .into(tableRef("target"))
        .tableUniqueKey(field("id"))
        .from(selectStatement);
  }

  /**
   * Test that MERGE routes to native MERGE syntax for PostgreSQL 15+
   */
  @Test
  public void testMergeRoutesToNativeMergeForVersion15() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "15")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    dialect.setSchemaResource(schemaResource);

    // When
    String sql = dialect.convertStatementToSQL(testMergeStatement);

    // Then
    assertTrue("Should use native MERGE syntax for PostgreSQL 15", sql.contains("MERGE INTO"));
    assertFalse("Should not use INSERT...ON CONFLICT syntax", sql.contains("INSERT INTO"));
  }

  /**
   * Test that MERGE routes to native MERGE syntax for PostgreSQL 16+
   */
  @Test
  public void testMergeRoutesToNativeMergeForVersion16() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "16")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    dialect.setSchemaResource(schemaResource);

    // When
    String sql = dialect.convertStatementToSQL(testMergeStatement);

    // Then
    assertTrue("Should use native MERGE syntax for PostgreSQL 16", sql.contains("MERGE INTO"));
    assertFalse("Should not use INSERT...ON CONFLICT syntax", sql.contains("INSERT INTO"));
  }

  /**
   * Test that MERGE routes to INSERT...ON CONFLICT for PostgreSQL 14
   */
  @Test
  public void testMergeRoutesToInsertOnConflictForVersion14() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "14")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    dialect.setSchemaResource(schemaResource);

    // When
    String sql = dialect.convertStatementToSQL(testMergeStatement);

    // Then
    assertTrue("Should use INSERT...ON CONFLICT syntax for PostgreSQL 14", sql.contains("INSERT INTO"));
    assertTrue("Should use ON CONFLICT clause", sql.contains("ON CONFLICT"));
    assertFalse("Should not use native MERGE syntax", sql.contains("MERGE INTO"));
  }

  /**
   * Test that MERGE routes to INSERT...ON CONFLICT for PostgreSQL 13
   */
  @Test
  public void testMergeRoutesToInsertOnConflictForVersion13() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "13")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    dialect.setSchemaResource(schemaResource);

    // When
    String sql = dialect.convertStatementToSQL(testMergeStatement);

    // Then
    assertTrue("Should use INSERT...ON CONFLICT syntax for PostgreSQL 13", sql.contains("INSERT INTO"));
    assertTrue("Should use ON CONFLICT clause", sql.contains("ON CONFLICT"));
    assertFalse("Should not use native MERGE syntax", sql.contains("MERGE INTO"));
  }

  /**
   * Test that MERGE defaults to INSERT...ON CONFLICT when version is missing
   */
  @Test
  public void testMergeDefaultsToInsertOnConflictWhenVersionMissing() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    dialect.setSchemaResource(schemaResource);

    // When
    String sql = dialect.convertStatementToSQL(testMergeStatement);

    // Then
    assertTrue("Should default to INSERT...ON CONFLICT when version is missing", sql.contains("INSERT INTO"));
    assertTrue("Should use ON CONFLICT clause", sql.contains("ON CONFLICT"));
    assertFalse("Should not use native MERGE syntax", sql.contains("MERGE INTO"));
  }

  /**
   * Test that MERGE defaults to INSERT...ON CONFLICT when SchemaResource is not set
   */
  @Test
  public void testMergeDefaultsToInsertOnConflictWhenSchemaResourceNotSet() {
    // Given - dialect without SchemaResource set

    // When
    String sql = dialect.convertStatementToSQL(testMergeStatement);

    // Then
    assertTrue("Should default to INSERT...ON CONFLICT when SchemaResource not set", sql.contains("INSERT INTO"));
    assertTrue("Should use ON CONFLICT clause", sql.contains("ON CONFLICT"));
    assertFalse("Should not use native MERGE syntax", sql.contains("MERGE INTO"));
  }

  /**
   * Test that MERGE defaults to INSERT...ON CONFLICT when metadata provider is not available
   */
  @Test
  public void testMergeDefaultsToInsertOnConflictWhenMetadataProviderNotAvailable() {
    // Given
    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.empty());

    dialect.setSchemaResource(schemaResource);

    // When
    String sql = dialect.convertStatementToSQL(testMergeStatement);

    // Then
    assertTrue("Should default to INSERT...ON CONFLICT when metadata provider not available", sql.contains("INSERT INTO"));
    assertTrue("Should use ON CONFLICT clause", sql.contains("ON CONFLICT"));
    assertFalse("Should not use native MERGE syntax", sql.contains("MERGE INTO"));
  }
}
