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

import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

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

    Set<java.util.UUID> results = new HashSet<>();

    // Query the database to see if the UpgradeAudit
    SelectStatement upgradeAuditSelect = select(field("upgradeUUID"))
            .from(tableRef(DatabaseUpgradeTableContribution.UPGRADE_AUDIT_NAME));
    String sql = dialect.convertStatementToSQL(upgradeAuditSelect);

    if (log.isDebugEnabled()) log.debug("Loading UpgradeAudit with SQL [" + sql + "]");

    try (Connection connection = dataSource.getConnection();
         java.sql.Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        try {
          results.add(java.util.UUID.fromString(resultSet.getString(1)));
        } catch (IllegalArgumentException e) {
           // If we have a historic malformed UUID, ignore it
           log.warn("Malformed UpgradeAudit Table upgradeUUID column record ["+
                   resultSet.getString(1)+
                   "]. Skipping.");
        }
      }


    } catch (SQLException e) {
      throw new RuntimeSqlException("Failed to load applied UUIDs. SQL: [" + sql + "]", e);
    }

    return Collections.unmodifiableSet(results);
  }

}
