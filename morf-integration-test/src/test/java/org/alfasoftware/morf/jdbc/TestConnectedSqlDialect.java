/* Copyright 2017 Alfa Financial Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.alfasoftware.morf.jdbc;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.view;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;

import javax.annotation.concurrent.NotThreadSafe;
import javax.sql.DataSource;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.sql.element.FieldLiteral;
import org.alfasoftware.morf.sql.element.FieldReference;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Provider;


/**
 * Tests for {@link SqlDialect}.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
@NotThreadSafe
public class TestConnectedSqlDialect {
  private static final Log log = LogFactory.getLog(TestConnectedSqlDialect.class);

  private Connection connection;
  private AliasedField columnARef;
  private AliasedField columnBRef;
  private AliasedField columnCRef;
  private AliasedField columnDRef;
  private TableReference tableARef;
  private TableReference tableBRef;
  private View view;
  private Table tableA;
  private Table tableB;


  /**
   * Get connection details and data source
   */
  @Rule
  public InjectMembersRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  /**
   * Inject a DB connection
   *
   */
  @Inject
  Provider<ConnectionResources> connectionProvider;

  @Inject DataSource dataSource;

  /**
   * Inject a schema manager
   *
   */
  @Inject
  Provider<DatabaseSchemaManager> schemaManagerProvider;

  /**
   * Inject a sql script executor
   *
   */
  @Inject
  SqlScriptExecutorProvider sqlScriptExecutorProvider;

  /**
   * set up
   */
  @Before
  public void before() throws SQLException {
    SqlDialect dialect = connectionProvider.get().sqlDialect();
    SqlScriptExecutor sqlExecutor = sqlScriptExecutorProvider.get();
    connection = dataSource.getConnection();

    // Ensure the tables we need are created
    tableA = table("TableA").columns(
        column("ColumnA", DataType.STRING, 1),
        column("ColumnB", DataType.STRING, 1)
      );
    tableB = table("TableB").columns(
        column("ColumnC", DataType.STRING, 1),
        column("ColumnD", DataType.STRING, 1)
      );
    Schema schema = schema(tableA, tableB);
    schemaManagerProvider.get().mutateToSupportSchema(schema, TruncationBehavior.ONLY_ON_TABLE_CHANGE);

    // Set up references
    tableARef  = new TableReference("TableA");
    tableBRef  = new TableReference("TableB");
    columnARef = new FieldReference("ColumnA");
    columnBRef = new FieldReference("ColumnB");
    columnCRef = new FieldReference("ColumnC");
    columnDRef = new FieldReference("ColumnD");

    // Populate them with some data
    sqlExecutor.execute(dialect.deleteAllFromTableStatements(tableA));
    sqlExecutor.execute(dialect.deleteAllFromTableStatements(tableB));
    sqlExecutor.execute(dialect.convertStatementToSQL(new InsertStatement()
      .into(tableARef)
      .values(new FieldLiteral("0").as("ColumnA"), new FieldLiteral("1").as("ColumnB")), schema, tableA));
    sqlExecutor.execute(dialect.convertStatementToSQL(new InsertStatement()
      .into(tableARef)
      .values(new FieldLiteral("2").as("ColumnA"), new FieldLiteral("3").as("ColumnB")), schema, tableA));
    sqlExecutor.execute(dialect.convertStatementToSQL(new InsertStatement()
      .into(tableBRef)
      .values(new FieldLiteral("0").as("ColumnC"), new FieldLiteral("1").as("ColumnD")), schema, tableB));
    sqlExecutor.execute(dialect.convertStatementToSQL(new InsertStatement()
      .into(tableBRef)
      .values(new FieldLiteral("2").as("ColumnC"), new FieldLiteral("3").as("ColumnD")), schema, tableB));
  }



  /**
   * tidy up
   */
  @After
  public void tearDown() throws SQLException {
    SqlDialect dialect = connectionProvider.get().sqlDialect();
    SqlScriptExecutor sqlExecutor = sqlScriptExecutorProvider.get();

    // Must clean up and get rid of the view if we've created it, otherwise we'll break other tests
    // that try and drop the tables it uses
    if (view != null) {
      sqlExecutor.execute(new ArrayList<>(dialect.dropStatements(view)));
    }

    // And drop the tables too, just to be tidy
    if (tableA != null) {
      sqlExecutor.execute(new ArrayList<>(dialect.dropStatements(tableA)));
    }
    if (tableB != null) {
      sqlExecutor.execute(new ArrayList<>(dialect.dropStatements(tableB)));
    }

    connection.close();
  }



  /**
   * Create a view and make sure we can access it
   */
  @Test
  public void testCreateView() throws SQLException {
    SqlDialect dialect = connectionProvider.get().sqlDialect();

    // Define the view
    view = view("ViewA", select(columnARef, columnBRef, columnCRef, columnDRef).
                           from(tableARef).
                           innerJoin(tableBRef, Criterion.eq(columnARef, columnCRef)));

    // Create it
    Schema schema = schema(view);
    schemaManagerProvider.get().mutateToSupportSchema(schema, TruncationBehavior.ONLY_ON_TABLE_CHANGE);

    // Do it again, to make sure that we handle the view already existing
    schemaManagerProvider.get().mutateToSupportSchema(schema, TruncationBehavior.ONLY_ON_TABLE_CHANGE);

    // Fetch the contents
    TableReference viewRef = new TableReference("ViewA");

    Connection connection = dataSource.getConnection();
    try {
      String sql = dialect.convertStatementToSQL(new SelectStatement(columnARef, columnBRef, columnCRef, columnDRef).from(viewRef).orderBy(columnARef));

      Statement statement = connection.createStatement();
      try {

        if (log.isDebugEnabled())
          log.debug("Executing SQL query [" + sql + "]");

        ResultSet result = statement.executeQuery(sql);

        // Confirm that we've got exactly what we expect
        result.next();
        assertEquals("Column A row 1 matches", "0", result.getString("ColumnA"));
        assertEquals("Column B row 1 matches", "1", result.getString("ColumnB"));
        assertEquals("Column C row 1 matches", "0", result.getString("ColumnC"));
        assertEquals("Column D row 1 matches", "1", result.getString("ColumnD"));
        result.next();
        assertEquals("Column A row 2 matches", "2", result.getString("ColumnA"));
        assertEquals("Column B row 2 matches", "3", result.getString("ColumnB"));
        assertEquals("Column C row 2 matches", "2", result.getString("ColumnC"));
        assertEquals("Column D row 2 matches", "3", result.getString("ColumnD"));
        assertTrue("Only two records", !result.next());

      } catch (SQLException e) {
        throw new RuntimeSqlException("Error executing SQL query [" + sql + "]", e);
      } finally {
        statement.close();
      }
    } finally {
      connection.close();
    }


  }


}
