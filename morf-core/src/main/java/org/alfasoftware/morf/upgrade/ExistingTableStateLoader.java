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

import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

/**
 * Loads all UUIDs which have previously been applied.
 *
 * @author Copyright (c) Alfa Financial Software 2011 - 2013
 */
class ExistingTableStateLoader {
  private static final Log log = LogFactory.getLog(ExistingTableStateLoader.class);

  private final DataSource dataSource;
  private final SqlDialect dialect;

  /**
   * @param dataSource DataSource.
   * @param dialect SqlDialect.
   */
  ExistingTableStateLoader(DataSource dataSource, SqlDialect dialect) {
    super();
    this.dataSource = dataSource;
    this.dialect = dialect;
  }


  /**
   * @return The set of all UUIDs which have previously been applied to this database.
   */
  Set<java.util.UUID> loadAppliedStepUUIDs() {

    Set<java.util.UUID> results = new HashSet<java.util.UUID>();

    // Query the database to see if the UpgradeAudit
    SelectStatement upgradeAuditSelect = select(field("upgradeUUID")).from(tableRef(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME));
    String sql = dialect.convertStatementToSQL(upgradeAuditSelect);

    if (log.isDebugEnabled()) log.debug("Loading UpgradeAudit with SQL [" + sql + "]");

    try {
      Connection connection = dataSource.getConnection();
      try {
        java.sql.Statement statement = connection.createStatement();
        try {
          ResultSet resultSet = statement.executeQuery(sql);
          try {
            while (resultSet.next()) {
              String uuidString = fixUUIDs(resultSet.getString(1));

              results.add(java.util.UUID.fromString(uuidString));
            }
          } finally {
            resultSet.close();
          }
        } finally {
          statement.close();
        }
      } finally {
        connection.close();
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException("Failed to load applied UUIDs. SQL: [" + sql + "]", e);
    }

    return Collections.unmodifiableSet(results);
  }


  /**
   * Fix the loaded UUID to deal with problems introduced earlier.
   *
   * @param uuid The UUID to tweak.
   * @return Almost always the same uuid - although it is very occasionally tweaked.
   */
  private String fixUUIDs(String uuid) {
    // This is necessary because AddSegregationTypeToLetterClass had an illegal UUID (with a z in it)
    // This can be removed once the 5_1_0 upgrade is deprecated.
    if (uuid.equals("5222ez70-513d-11e0-b8af-0800200c9a66")) {
      return "5222ef70-513d-11e0-b8af-0800200c9a66";
    }
    return uuid;
  }
}
