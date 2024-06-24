package org.alfasoftware.morf.jdbc.h2;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import java.util.List;

import org.alfasoftware.morf.sql.DeleteStatement;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectFirstStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.TruncateStatement;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;
import org.mockito.junit.MockitoJUnitRunner;

/**
 * Tests class for H2Sql and H2DialectExt.
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
@RunWith(MockitoJUnitRunner.class)
public class TestH2Sql {

  @Spy
  private H2DialectExt h2DialectExt = new H2DialectExt();

  private H2Sql h2Sql;

  @Before
  public void setUp() {
    MockitoAnnotations.openMocks(this);
    h2Sql = h2DialectExt.createH2Sql();
  }

  /**
   * Test for the creation of H2Sql instance.
   * Verifies that the H2Sql instance is not null.
   */
  @Test
  public void testCreateH2Sql() {
    // Given
    H2DialectExt h2DialectExt = new H2DialectExt();

    // When
    H2Sql result = h2DialectExt.createH2Sql();

    // Then
    assertNotNull(result);
  }

  /**
   * Test for converting SelectStatement to SQL.
   * Verifies that the correct SQL string is returned for a given SelectStatement.
   */
  @Test
  public void testSqlFromSelectStatement() {
    // Given
    SelectStatement statement = mock(SelectStatement.class);
    String expectedSql = "SELECT * FROM table";
    doReturn(expectedSql).when(h2DialectExt).convertStatementToSQL(statement);

    // When
    String result = h2Sql.sqlFrom(statement);

    // Then
    assertEquals(expectedSql, result);
    verify(h2DialectExt, times(1)).convertStatementToSQL(statement);
  }

  /**
   * Test for converting SelectFirstStatement to SQL.
   * Verifies that the correct SQL string is returned for a given SelectFirstStatement.
   */
  @Test
  public void testSqlFromSelectFirstStatement() {
    // Given
    SelectFirstStatement statement = mock(SelectFirstStatement.class);
    String expectedSql = "SELECT FIRST * FROM table";
    doReturn(expectedSql).when(h2DialectExt).convertStatementToSQL(statement);

    // When
    String result = h2Sql.sqlFrom(statement);

    // Then
    assertEquals(expectedSql, result);
    verify(h2DialectExt, times(1)).convertStatementToSQL(statement);
  }

  /**
   * Test for converting InsertStatement to SQL.
   * Verifies that the correct list of SQL strings is returned for a given InsertStatement.
   */
  @Test
  public void testSqlFromInsertStatement() {
    // Given
    InsertStatement statement = mock(InsertStatement.class);
    List<String> expectedSql = List.of("INSERT INTO table (column1) VALUES ('value1')");
    doReturn(expectedSql).when(h2DialectExt).convertStatementToSQL(statement);

    // When
    List<String> result = h2Sql.sqlFrom(statement);

    // Then
    assertEquals(expectedSql, result);
    verify(h2DialectExt, times(1)).convertStatementToSQL(statement);
  }

  /**
   * Test for converting UpdateStatement to SQL.
   * Verifies that the correct SQL string is returned for a given UpdateStatement.
   */
  @Test
  public void testSqlFromUpdateStatement() {
    // Given
    UpdateStatement statement = mock(UpdateStatement.class);
    String expectedSql = "UPDATE table SET column1 = 'value1' WHERE condition";
    doReturn(expectedSql).when(h2DialectExt).convertStatementToSQL(statement);

    // When
    String result = h2Sql.sqlFrom(statement);

    // Then
    assertEquals(expectedSql, result);
    verify(h2DialectExt, times(1)).convertStatementToSQL(statement);
  }

  /**
   * Test for converting MergeStatement to SQL.
   * Verifies that the correct SQL string is returned for a given MergeStatement.
   */
  @Test
  public void testSqlFromMergeStatement() {
    // Given
    MergeStatement statement = mock(MergeStatement.class);
    String expectedSql = "MERGE INTO table USING source ON condition WHEN MATCHED THEN UPDATE SET column1 = 'value1' WHEN NOT MATCHED THEN INSERT (column1) VALUES ('value1')";
    doReturn(expectedSql).when(h2DialectExt).convertStatementToSQL(statement);

    // When
    String result = h2Sql.sqlFrom(statement);

    // Then
    assertEquals(expectedSql, result);
    verify(h2DialectExt, times(1)).convertStatementToSQL(statement);
  }

  /**
   * Test for converting DeleteStatement to SQL.
   * Verifies that the correct SQL string is returned for a given DeleteStatement.
   */
  @Test
  public void testSqlFromDeleteStatement() {
    // Given
    DeleteStatement statement = mock(DeleteStatement.class);
    String expectedSql = "DELETE FROM table WHERE condition";
    doReturn(expectedSql).when(h2DialectExt).convertStatementToSQL(statement);

    // When
    String result = h2Sql.sqlFrom(statement);

    // Then
    assertEquals(expectedSql, result);
    verify(h2DialectExt, times(1)).convertStatementToSQL(statement);
  }

  /**
   * Test for converting TruncateStatement to SQL.
   * Verifies that the correct SQL string is returned for a given TruncateStatement.
   */
  @Test
  public void testSqlFromTruncateStatement() {
    // Given
    TruncateStatement statement = mock(TruncateStatement.class);
    String expectedSql = "TRUNCATE TABLE table";
    doReturn(expectedSql).when(h2DialectExt).convertStatementToSQL(statement);

    // When
    String result = h2Sql.sqlFrom(statement);

    // Then
    assertEquals(expectedSql, result);
    verify(h2DialectExt, times(1)).convertStatementToSQL(statement);
  }

  /**
   * Test for sqlFrom method when the Statement is not an InsertStatement.
   * Verifies that convertStatementToSQL is called directly and the correct list of SQL strings is returned.
   */
  @Test
  public void testSqlFromNonInsertStatement() {
    // Given
    Statement updateStatement = mock(Statement.class);
    List<String> expectedSql = List.of("UPDATE table SET column1 = 'value1' WHERE condition");

    doReturn(expectedSql).when(h2DialectExt).convertStatementToSQL(updateStatement, null, null);

    // When
    List<String> result = h2Sql.sqlFrom(updateStatement);

    // Then
    assertEquals(expectedSql, result);
    verify(h2DialectExt, times(1)).convertStatementToSQL(updateStatement, null, null);
  }
}
