package org.alfasoftware.morf.jdbc.postgresql;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Tests for PostgreSQL version detection in {@link PostgreSQLDialect}.
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
public class TestPostgreSQLDialectVersionDetection {

  /**
   * Test that shouldUseNativeMerge returns true for PostgreSQL 15+
   */
  @Test
  public void testShouldUseNativeMergeWithVersion15() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_PRODUCT_VERSION, "15.0")
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "15")
        .put(DatabaseMetaDataProvider.DATABASE_MINOR_VERSION, "0")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialect = new PostgreSQLDialect("testschema");

    // When
    boolean result = dialect.shouldUseNativeMerge(schemaResource);

    // Then
    Assert.assertTrue("Should use native MERGE for PostgreSQL 15", result);
  }


  /**
   * Test that shouldUseNativeMerge returns true for PostgreSQL 16+
   */
  @Test
  public void testShouldUseNativeMergeWithVersion16() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_PRODUCT_VERSION, "16.1")
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "16")
        .put(DatabaseMetaDataProvider.DATABASE_MINOR_VERSION, "1")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialect = new PostgreSQLDialect("testschema");

    // When
    boolean result = dialect.shouldUseNativeMerge(schemaResource);

    // Then
    Assert.assertTrue("Should use native MERGE for PostgreSQL 16", result);
  }


  /**
   * Test that shouldUseNativeMerge returns false for PostgreSQL 14
   */
  @Test
  public void testShouldUseNativeMergeWithVersion14() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_PRODUCT_VERSION, "14.5")
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "14")
        .put(DatabaseMetaDataProvider.DATABASE_MINOR_VERSION, "5")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialect = new PostgreSQLDialect("testschema");

    // When
    boolean result = dialect.shouldUseNativeMerge(schemaResource);

    // Then
    Assert.assertFalse("Should not use native MERGE for PostgreSQL 14", result);
  }


  /**
   * Test that shouldUseNativeMerge returns false for PostgreSQL 13
   */
  @Test
  public void testShouldUseNativeMergeWithVersion13() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_PRODUCT_VERSION, "13.8")
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "13")
        .put(DatabaseMetaDataProvider.DATABASE_MINOR_VERSION, "8")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialect = new PostgreSQLDialect("testschema");

    // When
    boolean result = dialect.shouldUseNativeMerge(schemaResource);

    // Then
    Assert.assertFalse("Should not use native MERGE for PostgreSQL 13", result);
  }


  /**
   * Test that shouldUseNativeMerge returns false when version information is missing
   */
  @Test
  public void testShouldUseNativeMergeWithMissingVersion() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_PRODUCT_VERSION, "Unknown")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialect = new PostgreSQLDialect("testschema");

    // When
    boolean result = dialect.shouldUseNativeMerge(schemaResource);

    // Then
    Assert.assertFalse("Should not use native MERGE when version is missing", result);
  }


  /**
   * Test that shouldUseNativeMerge returns false when metadata provider is not available
   */
  @Test
  public void testShouldUseNativeMergeWithNoMetadataProvider() {
    // Given
    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.empty());

    PostgreSQLDialect dialect = new PostgreSQLDialect("testschema");

    // When
    boolean result = dialect.shouldUseNativeMerge(schemaResource);

    // Then
    Assert.assertFalse("Should not use native MERGE when metadata provider is not available", result);
  }


  /**
   * Test that shouldUseNativeMerge returns false when version string is not a valid number
   */
  @Test
  public void testShouldUseNativeMergeWithInvalidVersionFormat() {
    // Given
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_PRODUCT_VERSION, "15.0")
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, "invalid")
        .put(DatabaseMetaDataProvider.DATABASE_MINOR_VERSION, "0")
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialect = new PostgreSQLDialect("testschema");

    // When
    boolean result = dialect.shouldUseNativeMerge(schemaResource);

    // Then
    Assert.assertFalse("Should not use native MERGE when version format is invalid", result);
  }
}
