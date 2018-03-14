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

package org.alfasoftware.morf.dataset;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.alfasoftware.morf.sql.element.TableReference;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;


/**
 * Loads a list of {@link Record}s into a database table, using native load if it is available.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class TableLoader {

  private static final Log log = LogFactory.getLog(TableLoader.class);

  private final Connection connection;
  private final SqlDialect sqlDialect;
  private final boolean explicitCommit;
  private final SqlScriptExecutor sqlExecutor;
  private final Table table;
  private final boolean insertingWithPresetAutonums;
  private final boolean insertingUnderAutonumLimit;
  private final boolean truncateBeforeLoad;
  private final String insertStatement;
  private final int batchSize;


  /**
   * @return A new {@link TableLoaderBuilder}.
   */
  public static TableLoaderBuilder builder() {
    return new TableLoaderBuilderImpl();
  }


  /**
   * Constructor.
   */
  TableLoader(Connection connection,
                     SqlScriptExecutor sqlScriptExecutor,
                     SqlDialect dialect,
                     boolean explicitCommit,
                     Table table,
                     boolean insertingWithPresetAutonums,
                     boolean insertingUnderAutonumLimit,
                     boolean truncateBeforeLoad,
                     int batchSize) {
    super();
    this.connection = connection;
    this.sqlExecutor = sqlScriptExecutor;
    this.sqlDialect = dialect;
    this.explicitCommit = explicitCommit;
    this.table = table;
    this.insertingWithPresetAutonums = insertingWithPresetAutonums;
    this.insertingUnderAutonumLimit = insertingUnderAutonumLimit;
    this.truncateBeforeLoad = truncateBeforeLoad;
    this.batchSize = batchSize;
    this.insertStatement = sqlDialect.convertStatementToSQL(
      new InsertStatement().into(new TableReference(table.getName())),
      SchemaUtils.schema(table)
    );
  }


  /**
   * Load the specified records into the table.
   *
   * @param records the records to load.
   */
  public void load(final Iterable<Record> records) {

    if (truncateBeforeLoad) {
      truncate(table, connection);
    }

    insertRecords(records);
  }


  /**
   * Empties the specified table.
   *
   * @param table
   */
  private void truncate(Table table, Connection connection) {
    // Get our own table definition based on the name to avoid case sensitivity bugs on some database vendors
    log.debug("Clearing table [" + table.getName() + "]");

    // Try to use a truncate
    try {
      sqlExecutor.execute(sqlDialect.truncateTableStatements(table), connection);
    } catch (RuntimeSqlException e) {
      // If that has failed try to use a delete
      log.debug("Failed to truncate table, attempting a delete", e);
      sqlExecutor.execute(sqlDialect.deleteAllFromTableStatements(table), connection);
    }
  }


  /**
   * Insert the records into the database using a {@link PreparedStatement}.
   *
   * @param table The table metadata
   * @param records The records to insert
   * @param bulk Indicates if we should call {@link SqlDialect#preInsertStatements(Table)} and {@link SqlDialect#postInsertStatements(Table)}
   */
  private void insertRecords(Iterable<Record> records) {

    if (insertingWithPresetAutonums) {
      sqlExecutor.execute(sqlDialect.preInsertWithPresetAutonumStatements(table, insertingUnderAutonumLimit), connection);
    }

    sqlInsertLoad(table, records, connection);

    if (insertingWithPresetAutonums) {
      sqlDialect.postInsertWithPresetAutonumStatements(table, sqlExecutor, connection,insertingUnderAutonumLimit);
    }
  }


  /**
   * Insert the data using an SQL insert.
   *
   * @param table The table we are inserting into.
   * @param records The data to insert.
   * @param connection The connection.
   */
  private void sqlInsertLoad(Table table, Iterable<Record> records, Connection connection) {
    try {
      sqlExecutor.executeStatementBatch(insertStatement, SqlParameter.parametersFromColumns(table.columns()), records, connection, explicitCommit, batchSize);
    } catch (Exception e) {
      throw new RuntimeException(String.format("Failure in batch insert for table [%s]", table.getName()), e);
    }
  }
}
