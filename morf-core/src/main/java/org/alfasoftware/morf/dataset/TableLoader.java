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

import static java.util.stream.Collectors.toList;
import static org.alfasoftware.morf.sql.SqlUtils.parameter;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.alfasoftware.morf.dataset.TableLoaderBuilder.TableLoaderBuilderImpl;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.SqlParameter;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.FluentIterable;

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
  private final boolean merge;
  private final SqlScriptExecutor sqlExecutor;
  private final Table table;
  private final boolean insertingWithPresetAutonums;
  private final boolean insertingUnderAutonumLimit;
  private final boolean truncateBeforeLoad;
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
              boolean merge,
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
    this.merge = merge;
    this.table = table;
    this.insertingWithPresetAutonums = insertingWithPresetAutonums;
    this.insertingUnderAutonumLimit = insertingUnderAutonumLimit;
    this.truncateBeforeLoad = truncateBeforeLoad;
    this.batchSize = batchSize;

    if (truncateBeforeLoad && !explicitCommit) {
      // This is because PostgreSQL needs to be able to commit/rollback, for TRUNCATE fallback to DELETE to work
      // It could potentially be relaxed, if the DELETE fallback after a failed TRUNCATE is not needed.
      // It also could potentially be relaxed, if running on PostgreSQL is of no concern to the caller.
      throw new UnsupportedOperationException("Cannot TRUNCATE before load without permission to commit explicitly");
    }
  }


  /**
   * Load the specified records into the table.
   *
   * @param records the records to load.
   */
  public void load(final Iterable<Record> records) {

    if (truncateBeforeLoad) {
      truncate(table);
    }

    insertOrMergeRecords(records);
  }


  /**
   * Empties the specified table.
   *
   * @param table
   */
  private void truncate(Table table) {
    // Get our own table definition based on the name to avoid case sensitivity bugs on some database vendors
    log.debug("Clearing table [" + table.getName() + "]");

    // Try to use a truncate
    try {
      runRecoverably(() ->
        sqlExecutor.execute(sqlDialect.truncateTableStatements(table), connection)
      );
    } catch (SQLException | RuntimeSqlException e) {
      // If that has failed try to use a delete
      log.debug("Failed to truncate table, attempting a delete", e);
      sqlExecutor.execute(sqlDialect.deleteAllFromTableStatements(table), connection);
    }
  }


  /**
   * PostgreSQL does not like failing commands, and marks connections as "dirty" after errors:
   * <q>ERROR: current transaction is aborted, commands ignored until end of transaction block.</q>
   *
   * <p>To recover a connection, one has to issue a rollback.</p>
   *
   * @param runnable
   */
  private void runRecoverably(Runnable runnable) throws SQLException {
    // make sure we commit if we can
    if (explicitCommit) {
      connection.commit();
    }

    try {
      runnable.run();
    }
    catch (Exception e) {
      // make sure we rollback if we can
      if (explicitCommit) {
        connection.rollback();
      }
      throw e;
    }
  }


  /**
   * Insert the records into the database using a {@link PreparedStatement}.
   *
   * @param table The table metadata
   * @param records The records to insert
   * @param bulk Indicates if we should call {@link SqlDialect#preInsertStatements(Table)} and {@link SqlDialect#postInsertStatements(Table)}
   */
  private void insertOrMergeRecords(Iterable<Record> records) {
    Iterable<Record> fixedRecords = FluentIterable.from(records)
        .transform(this::transformRecord);

    if (insertingWithPresetAutonums) {
      sqlExecutor.execute(sqlDialect.preInsertWithPresetAutonumStatements(table, insertingUnderAutonumLimit), connection);
    }

    sqlInsertOrMergeLoad(table, fixedRecords, connection);

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
  private void sqlInsertOrMergeLoad(Table table, Iterable<Record> records, Connection connection) {
    try {
      if (merge && !table.primaryKey().isEmpty()) { // if the table has no primary we don't have a merge ON criterion
        SelectStatement selectStatement = SelectStatement.select()
            .fields(table.columns().stream().filter(c -> !c.isAutoNumbered()).map(c -> parameter(c)).collect(toList()))
            .build();
        MergeStatement mergeStatement = MergeStatement.merge()
            .from(selectStatement)
            .into(tableRef(table.getName()))
            .tableUniqueKey(table.primaryKey().stream().map(c -> parameter(c)).collect(toList()))
            .build();
        String mergeSQL = sqlDialect.convertStatementToSQL(mergeStatement);
        sqlExecutor.executeStatementBatch(mergeSQL, SqlParameter.parametersFromColumns(table.columns()), records, connection, explicitCommit, batchSize);
      } else {
        InsertStatement insertStatement = InsertStatement.insert().into(tableRef(table.getName())).build();
        String insertSQL = sqlDialect.convertStatementToSQL(insertStatement, SchemaUtils.schema(table));
        sqlExecutor.executeStatementBatch(insertSQL, SqlParameter.parametersFromColumns(table.columns()), records, connection, explicitCommit, batchSize);
      }
    } catch (Exception exceptionOnBatch) {
      throw new RuntimeException(String.format("Failure in batch insert for table [%s]", table.getName()), exceptionOnBatch);
    }
  }


  private Record transformRecord(Record record) {
    return new Record() {
      @Override
      @SuppressWarnings("deprecation")
      public String getValue(String name) {
        return replaceSingleNulCharacter(record.getValue(name));
      }

      private String replaceSingleNulCharacter(String value) {
        return "\u0000".equals(value) ? "\u2400" : value;
      }
    };
  }
}
