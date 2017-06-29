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

package org.alfasoftware.morf.diagnostics;

import static org.alfasoftware.morf.sql.SqlUtils.select;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor.ResultSetProcessor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.Function;
import com.google.inject.ImplementedBy;
import com.google.inject.Inject;
import com.google.inject.Singleton;

/**
 * {@link DatabaseAndSystemTimeConsistencyChecker} queries the database for its
 * current time and based on that checks the difference between database time
 * and system time.
 * 
 * @author Copyright (c) Alfa Financial Software 2015
 */
@Singleton
public class DatabaseAndSystemTimeConsistencyChecker {

  private static final Log                LOG                              = LogFactory
                                                                               .getLog(DatabaseAndSystemTimeConsistencyChecker.class);

  private final ConnectionResources       connectionResources;
  private final SqlScriptExecutorProvider sqlScriptExecutorProvider;
  private final CurrentSystemTime         currentSystemTime;

  // now() returns time with 1s resolution right now so the tolerance has to be
  // that large. It is subject to change once now function gets improved.
  // See WEB-41129.
  static final long                       WARN_DIFF_VALUE                  = 2000;
  static final String                     SUPPRESS_EXCEPTION_JAVA_PROPERTY = "CHP.DisableDatabaseTimeConsistencyCheckException";


  @Inject
  DatabaseAndSystemTimeConsistencyChecker(ConnectionResources connectionResources,
      SqlScriptExecutorProvider sqlScriptExecutorProvider, CurrentSystemTime currentSystemTime) {
    this.connectionResources = connectionResources;
    this.sqlScriptExecutorProvider = sqlScriptExecutorProvider;
    this.currentSystemTime = currentSystemTime;
  }


  /**
   * Verifies if the system time and database time is the same. Logs warn/error
   * and may throw {@link IllegalStateException}.
   * 
   * @throws IllegalStateException if the difference is too large to be
   *           acceptable
   */
  public void verifyDatabaseAndSystemTimeConsitency() {
    final SelectStatement statement = select(Function.now());
    final String sql = connectionResources.sqlDialect().convertStatementToSQL(statement);
    final long systemTimeBefore = currentSystemTime.currentSystemTime();
    final long databaseTime = sqlScriptExecutorProvider.get().executeQuery(sql, new ResultSetProcessor<Long>() {
      @Override
      public Long process(ResultSet resultSet) throws SQLException {
        resultSet.next();
        return resultSet.getTimestamp(1).getTime();
      }
    });
    final long systemTimeAfter = currentSystemTime.currentSystemTime();
    if (LOG.isDebugEnabled()) {
      LOG.debug(reportTimes(systemTimeBefore, databaseTime, systemTimeAfter));
    }

    lookForSignificantTimeDifferenceProblems(systemTimeBefore, databaseTime, systemTimeAfter);
  }


  /**
   * Detect significant time differences
   * 
   * @param systemTimeBefore
   * @param databaseTime
   * @param systemTimeAfter
   */
  private void lookForSignificantTimeDifferenceProblems(long systemTimeBefore, long databaseTime, long systemTimeAfter) {
    long difference = Math.abs(systemTimeAfter - databaseTime);
    if (difference >= WARN_DIFF_VALUE) {
      LOG.warn("Database and system times are different by: [" + difference + "] "
          + reportTimes(systemTimeBefore, databaseTime, systemTimeAfter));
    }
  }


  private String reportTimes(long systemTimeBefore, long databaseTime, long systemTimeAfter) {
    return "system-time-before-query: [" + systemTimeBefore + "], database-time: [" + databaseTime
        + "], system-time-after-query: [" + systemTimeAfter + "]";
  }


  /**
   * Provides current system time.
   * 
   * @author Copyright (c) Alfa Financial Software 2015
   */
  @ImplementedBy(CurrentSystemTimeImpl.class)
  interface CurrentSystemTime {

    /**
     * @return current system time
     */
    long currentSystemTime();
  }

  /**
   * Default implementation of {@link CurrentSystemTime}
   * 
   * @author Copyright (c) Alfa Financial Software 2015
   */
  static class CurrentSystemTimeImpl implements CurrentSystemTime {

    /**
     * @see org.alfasoftware.morf.diagnostics.DatabaseAndSystemTimeConsistencyChecker.CurrentSystemTime#currentSystemTime()
     */
    @Override
    public long currentSystemTime() {
      return System.currentTimeMillis();
    }

  }
}
