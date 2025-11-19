package org.alfasoftware.morf.jdbc.postgresql;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Optional;

import org.alfasoftware.morf.jdbc.DatabaseMetaDataProvider;
import org.alfasoftware.morf.metadata.SchemaResource;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.TableReference;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

/**
 * Tests for InputField handling in PostgreSQL MERGE statements.
 */
public class TestPostgreSQLInputField {

  /**
   * Test that InputField generates "s.field" in native MERGE mode (PostgreSQL 15+).
   */
  @Test
  public void testInputFieldInNativeMergeMode() {
    // Setup PostgreSQL 15+ metadata
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_PRODUCT_VERSION, "15.0")
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, String.valueOf(15))
        .put(DatabaseMetaDataProvider.DATABASE_MINOR_VERSION, String.valueOf(0))
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialect = new PostgreSQLDialect("testschema");
    dialect.setSchemaResource(schemaResource);

    // Create a MERGE statement with InputField
    org.alfasoftware.morf.sql.MergeStatement mergeStatement = org.alfasoftware.morf.sql.MergeStatement.merge()
      .into(new TableReference("foo"))
      .tableUniqueKey(new FieldReference("id"))
      .from(org.alfasoftware.morf.sql.SelectStatement.select(
        new FieldLiteral(1).as("id"),
        new FieldLiteral(2).as("bar")
      ).build())
      .ifUpdating((overrides, values) -> overrides
        .set(values.input("bar").as("bar")))
      .build();

    // Generate SQL
    String sql = dialect.convertStatementToSQL(mergeStatement);

    // Verify that InputField uses "s." prefix in native MERGE mode
    Assert.assertTrue("SQL should contain 's.bar' for InputField in native MERGE mode", 
                      sql.contains("s.bar"));
    Assert.assertFalse("SQL should not contain 'EXCLUDED.bar' in native MERGE mode", 
                       sql.contains("EXCLUDED.bar"));
  }


  /**
   * Test that InputField generates "EXCLUDED.field" in INSERT...ON CONFLICT mode (PostgreSQL < 15).
   */
  @Test
  public void testInputFieldInInsertOnConflictMode() {
    // Setup PostgreSQL 14 metadata
    PostgreSQLMetaDataProvider metaDataProvider = mock(PostgreSQLMetaDataProvider.class);
    when(metaDataProvider.getDatabaseInformation()).thenReturn(
      ImmutableMap.<String, String>builder()
        .put(DatabaseMetaDataProvider.DATABASE_PRODUCT_VERSION, "14.0")
        .put(DatabaseMetaDataProvider.DATABASE_MAJOR_VERSION, String.valueOf(14))
        .put(DatabaseMetaDataProvider.DATABASE_MINOR_VERSION, String.valueOf(0))
        .build());

    SchemaResource schemaResource = mock(SchemaResource.class);
    when(schemaResource.getAdditionalMetadata()).thenReturn(Optional.of(metaDataProvider));

    PostgreSQLDialect dialect = new PostgreSQLDialect("testschema");
    dialect.setSchemaResource(schemaResource);

    // Create a MERGE statement with InputField
    org.alfasoftware.morf.sql.MergeStatement mergeStatement = org.alfasoftware.morf.sql.MergeStatement.merge()
      .into(new TableReference("foo"))
      .tableUniqueKey(new FieldReference("id"))
      .from(org.alfasoftware.morf.sql.SelectStatement.select(
        new FieldLiteral(1).as("id"),
        new FieldLiteral(2).as("bar")
      ).build())
      .ifUpdating((overrides, values) -> overrides
        .set(values.input("bar").as("bar")))
      .build();

    // Generate SQL
    String sql = dialect.convertStatementToSQL(mergeStatement);

    // Verify that InputField uses "EXCLUDED." prefix in INSERT...ON CONFLICT mode
    Assert.assertTrue("SQL should contain 'EXCLUDED.bar' for InputField in INSERT...ON CONFLICT mode", 
                      sql.contains("EXCLUDED.bar"));
    Assert.assertFalse("SQL should not contain 's.bar' in INSERT...ON CONFLICT mode", 
                       sql.contains("s.bar"));
  }
}
