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
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import javax.sql.DataSource;

import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.sql.SelectStatement;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Maps;



/**
 * Loads the hashes for the deployed views.
 *
 * @author Copyright (c) Alfa Financial Software 2011 - 2013
 */
class ExistingViewHashLoader {
  private static final Log log = LogFactory.getLog(ExistingViewHashLoader.class);

  private final DataSource dataSource;
  private final SqlDialect dialect;


  /**
   * Constructor.
   *
   * @param dataSource DataSource.
   * @param dialect SqlDialect.
   */
  ExistingViewHashLoader(DataSource dataSource, SqlDialect dialect) {
    super();
    this.dataSource = dataSource;
    this.dialect = dialect;
  }


  /**
   * Loads the hashes for the deployed views, or empty if the hashes cannot be loaded
   * (e.g. if the deployed views table does not exist in the existing schema).
   *
   * @param schema The existing database schema.
   * @return The deployed view hashes.
   */
  Optional<Map<String, String>> loadViewHashes(Schema schema) {

    if (!schema.tableExists(DEPLOYED_VIEWS_NAME)) {
      return Optional.empty();
    }

    Map<String, String> result = Maps.newHashMap();

    // Query the database to load DeployedViews
    SelectStatement upgradeAuditSelect = select(field("name"), field("hash")).from(tableRef(DEPLOYED_VIEWS_NAME));
    String sql = dialect.convertStatementToSQL(upgradeAuditSelect);

    if (log.isDebugEnabled()) log.debug("Loading " + DEPLOYED_VIEWS_NAME + " with SQL [" + sql + "]");

    try (Connection connection = dataSource.getConnection();
         java.sql.Statement statement = connection.createStatement();
         ResultSet resultSet = statement.executeQuery(sql)) {
      while (resultSet.next()) {
        // There was previously a bug in Deployment which wrote records to
        // the DeployedViews table without upper-casing them first.  Subsequent
        // Upgrades would write records in upper case but the original records
        // remained and could potentially be picked up here depending on
        // DB ordering.  We make sure we ignore records where there are
        // duplicates and one of them is not uppercased.
        String dbViewName = resultSet.getString(1);
        String viewName = dbViewName.toUpperCase();
        if (!result.containsKey(viewName) || dbViewName.equals(viewName)) {
          result.put(viewName, resultSet.getString(2));
        }
      }
    } catch (SQLException e) {
      throw new RuntimeSqlException("Failed to load deployed views. SQL: [" + sql + "]", e);
    }

    return Optional.of(Collections.unmodifiableMap(result));
  }
}
