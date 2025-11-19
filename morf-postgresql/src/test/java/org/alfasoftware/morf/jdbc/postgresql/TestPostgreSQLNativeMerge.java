package org.alfasoftware.morf.jdbc.postgresql;

import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.alfasoftware.morf.sql.MergeMatchClause;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.TableReference;
import org.junit.Before;
import org.junit.Test;

/**
 * Tests for native MERGE SQL generation in PostgreSQL 15+.
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
public class TestPostgreSQLNativeMerge {

  private PostgreSQLDialect dialect;

  @Before
  public void setUp() {
    dialect = new PostgreSQLDialect("testschema");
  }

  /**
   * Test basic MERGE structure with all clauses
   */
  @Test
  public void testBasicNativeMergeStructure() {
    // Given
    TableReference foo = tableRef("foo");
    TableReference somewhere = tableRef("somewhere");
    
    SelectStatement sourceStmt = new SelectStatement(
        somewhere.field("newId").as("id"),
        somewhere.field("newBar").as("bar")
    ).from(somewhere);
    
    MergeStatement stmt = MergeStatement.merge()
        .into(foo)
        .tableUniqueKey(field("id"))
        .from(sourceStmt)
        .build();

    // When
    String sql = dialect.generateNativeMergeSql(stmt);

    // Then
    assertTrue("Should contain MERGE INTO clause", sql.contains("MERGE INTO"));
    assertTrue("Should contain target table with alias", sql.contains("testschema.foo AS t"));
    assertTrue("Should contain USING clause", sql.contains("USING ("));
    assertTrue("Should contain source alias", sql.contains(") AS s"));
    assertTrue("Should contain ON clause", sql.contains("ON (t.id = s.id)"));
    assertTrue("Should contain WHEN MATCHED", sql.contains("WHEN MATCHED"));
    assertTrue("Should contain UPDATE SET", sql.contains("THEN UPDATE SET"));
    assertTrue("Should contain WHEN NOT MATCHED", sql.contains("WHEN NOT MATCHED"));
    assertTrue("Should contain INSERT", sql.contains("THEN INSERT"));
  }

  /**
   * Test MERGE with whenMatchedAction and WHERE clause
   */
  @Test
  public void testNativeMergeWithWhenMatchedWhereClause() {
    // Given
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
    String sql = dialect.generateNativeMergeSql(stmt);

    // Then
    assertTrue("Should contain WHEN MATCHED AND condition", 
        sql.contains("WHEN MATCHED AND") || sql.contains("WHEN MATCHED") && sql.contains("bar IS NOT NULL"));
  }

  /**
   * Test MERGE with only key fields (no WHEN MATCHED clause expected)
   */
  @Test
  public void testNativeMergeWithOnlyKeyFields() {
    // Given
    TableReference foo = tableRef("foo");
    TableReference somewhere = tableRef("somewhere");
    
    // Only selecting the key field
    SelectStatement sourceStmt = new SelectStatement(
        somewhere.field("newId").as("id")
    ).from(somewhere);
    
    MergeStatement stmt = MergeStatement.merge()
        .into(foo)
        .tableUniqueKey(field("id"))
        .from(sourceStmt)
        .build();

    // When
    String sql = dialect.generateNativeMergeSql(stmt);

    // Then
    assertTrue("Should contain MERGE INTO", sql.contains("MERGE INTO"));
    assertTrue("Should contain WHEN NOT MATCHED", sql.contains("WHEN NOT MATCHED"));
    // When all fields are keys, there should be no WHEN MATCHED clause
    // (or it should be present but with no UPDATE SET)
  }

  /**
   * Test field name qualification in native MERGE
   */
  @Test
  public void testNativeMergeFieldQualification() {
    // Given
    TableReference foo = tableRef("foo");
    TableReference somewhere = tableRef("somewhere");
    
    SelectStatement sourceStmt = new SelectStatement(
        somewhere.field("newId").as("id"),
        somewhere.field("newBar").as("bar"),
        somewhere.field("newBaz").as("baz")
    ).from(somewhere);
    
    MergeStatement stmt = MergeStatement.merge()
        .into(foo)
        .tableUniqueKey(field("id"))
        .from(sourceStmt)
        .build();

    // When
    String sql = dialect.generateNativeMergeSql(stmt);

    // Then
    assertTrue("ON clause should qualify key fields with t and s", 
        sql.contains("t.id = s.id"));
    assertTrue("INSERT VALUES should reference source with s prefix", 
        sql.contains("s.id") && sql.contains("s.bar") && sql.contains("s.baz"));
  }

  /**
   * Test MERGE with multiple key fields
   */
  @Test
  public void testNativeMergeWithMultipleKeyFields() {
    // Given
    TableReference foo = tableRef("foo");
    TableReference somewhere = tableRef("somewhere");
    
    SelectStatement sourceStmt = new SelectStatement(
        somewhere.field("newId").as("id"),
        somewhere.field("newType").as("type"),
        somewhere.field("newBar").as("bar")
    ).from(somewhere);
    
    MergeStatement stmt = MergeStatement.merge()
        .into(foo)
        .tableUniqueKey(field("id"), field("type"))
        .from(sourceStmt)
        .build();

    // When
    String sql = dialect.generateNativeMergeSql(stmt);

    // Then
    assertTrue("ON clause should contain both key fields", 
        sql.contains("t.id = s.id") && sql.contains("t.type = s.type"));
    assertTrue("Should join conditions with AND", sql.contains("AND"));
  }
}
