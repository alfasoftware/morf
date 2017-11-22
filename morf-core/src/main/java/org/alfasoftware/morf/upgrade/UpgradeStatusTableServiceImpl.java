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

package org.alfasoftware.morf.upgrade;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.alfasoftware.morf.upgrade.UpgradeStatus.IN_PROGRESS;
import static org.alfasoftware.morf.upgrade.UpgradeStatus.NONE;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.SqlUtils;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.alfasoftware.morf.sql.element.TableReference;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.inject.Inject;

/**
 * Service to manage or generate SQL for the transient table that stores the upgrade status.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
class UpgradeStatusTableServiceImpl implements UpgradeStatusTableService {

  private static final Log log = LogFactory.getLog(UpgradeStatusTableServiceImpl.class);

  /**
   * Name of the column in {@value UpgradeStatusTableService#UPGRADE_STATUS} table.
   */
  static final String STATUS_COLUMN = "status";

  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private final SqlDialect sqlDialect;


  /**
   * DI constructor.
   */
  @Inject
  UpgradeStatusTableServiceImpl(SqlScriptExecutorProvider sqlScriptExecutor, SqlDialect sqlDialect) {
    super();
    this.sqlScriptExecutorProvider = sqlScriptExecutor;
    this.sqlDialect = sqlDialect;
  }


  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStatusTableService#writeStatusFromStatus(org.alfasoftware.morf.upgrade.UpgradeStatus, org.alfasoftware.morf.upgrade.UpgradeStatus)
   */
  @Override
  public int writeStatusFromStatus(UpgradeStatus fromStatus, UpgradeStatus toStatus) {
    List<String> script = updateTableScript(fromStatus, toStatus);
    try {

      return sqlScriptExecutorProvider.get().execute(script);

    } catch (RuntimeSqlException e) {
      UpgradeStatus currentStatus = getStatus();
      log.debug("Caught exception trying to move from [" + fromStatus + "] to [" + toStatus + "]; current status = [" + currentStatus + "]", e);
      if (currentStatus.equals(toStatus)) {
        return 0;

      } else if (currentStatus.equals(fromStatus)) {
        // This might throw an exception if it fails again
        return sqlScriptExecutorProvider.get().execute(script);

      } else {
        // No point trying again, so throw the original exception
        throw e;
      }
    }
  }


  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStatusTableService#updateTableScript(org.alfasoftware.morf.upgrade.UpgradeStatus,
   *      org.alfasoftware.morf.upgrade.UpgradeStatus)
   */
  @Override
  public List<String> updateTableScript(UpgradeStatus fromStatus, UpgradeStatus toStatus) {
    List<String> statements = new ArrayList<>();
    TableReference table = tableRef(UpgradeStatusTableService.UPGRADE_STATUS);
    if (fromStatus == NONE && toStatus == IN_PROGRESS) {
      // Create upgradeStatus table and insert
      statements.addAll(sqlDialect.tableDeploymentStatements(
        table(UpgradeStatusTableService.UPGRADE_STATUS)
          .columns(column(STATUS_COLUMN, DataType.STRING, 255).defaultValue(fromStatus.name()))));

      statements.addAll(sqlDialect.convertStatementToSQL(
        insert().into(table)
          .values(literal(toStatus.name()).as(STATUS_COLUMN))));

    } else {
      UpdateStatement update = update(table)
                                 .set(literal(toStatus.name()).as(STATUS_COLUMN))
                                 .where(table.field(STATUS_COLUMN).eq(fromStatus.name()));
      statements.add(sqlDialect.convertStatementToSQL(update));
    }

    return statements;
  }


  /**
   * @see org.alfasoftware.morf.upgrade.UpgradeStatusTableService#getStatus()
   */
  @Override
  public UpgradeStatus getStatus() {
    SelectStatement select = SqlUtils.select(SqlUtils.field(STATUS_COLUMN)).from(tableRef(UpgradeStatusTableService.UPGRADE_STATUS));
    try {
      return sqlScriptExecutorProvider.get().executeQuery(sqlDialect.convertStatementToSQL(select), new ResultSetProcessor<UpgradeStatus>() {
        @Override
        public UpgradeStatus process(ResultSet resultSet) throws SQLException {
          resultSet.next();
          return UpgradeStatus.valueOf(resultSet.getString(1));
        }
      });
    } catch (RuntimeSqlException e) {
      log.debug("Unable to read column " + STATUS_COLUMN + " for upgrade status", e);
      return UpgradeStatus.NONE;
    }
  }


 /**
  *
  * @see org.alfasoftware.morf.upgrade.UpgradeStatusTableService#tidyUp(javax.sql.DataSource)
  */
  @Override
  public void tidyUp(DataSource dataSource) {
    try {
      new SqlScriptExecutorProvider(dataSource, sqlDialect).get().execute(sqlDialect.dropStatements(table(UpgradeStatusTableService.UPGRADE_STATUS)));
    } catch (RuntimeSqlException e) {
      //Throw exception only if the table still exists
      if (getStatus() != NONE) {
        throw e;
      }
    }
  }
}
