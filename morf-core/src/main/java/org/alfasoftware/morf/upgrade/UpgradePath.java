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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.inject.ImplementedBy;
import com.google.inject.Inject;

/**
 * Encapsulates a list of upgrade steps and can return the SQL to enact them.
 * For the purposes of this class, an "upgrade" can include moving from an
 * empty database to a deployed one; in which case {@code steps} will
 * be empty.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class UpgradePath implements SqlStatementWriter {

  private static final Log log = LogFactory.getLog(UpgradePath.class);

  /**
   * The unmodifiable list of upgrade steps.
   */
  private final List<UpgradeStep> steps;

  /**
   * The dialect of the upgrade steps
   */
  private final SqlDialect sqlDialect;

  /**
   * Provides SQL to be run-post upgrade.
   */
  private final Set<UpgradeScriptAddition> upgradeScriptAdditions;

  /**
   * The SQL to execute.
   */
  private final List<String> sql = new LinkedList<>();


  /**
   * Create a new complete deployment.
   * @param upgradeScriptAdditions The SQL to be appended to the upgrade.
   * @param sqlDialect the SQL dialect being used for this upgrade path
   */
  public UpgradePath(Set<UpgradeScriptAddition> upgradeScriptAdditions, SqlDialect sqlDialect) {
    this(upgradeScriptAdditions, new ArrayList<UpgradeStep>(), sqlDialect);
  }


  /**
   * Create a new upgrade for the given list of steps.
   *
   * @param upgradeScriptAdditions The SQL to be appended to the upgrade.
   * @param steps the upgrade steps to run
   * @param sqlDialect the SQL dialect being used for this upgrade path
   */
  public UpgradePath(Set<UpgradeScriptAddition> upgradeScriptAdditions, List<UpgradeStep> steps, SqlDialect sqlDialect) {
    super();
    this.steps = Collections.unmodifiableList(steps);
    this.sqlDialect = sqlDialect;
    this.upgradeScriptAdditions = upgradeScriptAdditions;
  }


  /**
   * Return the list of upgrade steps. If empty, a deployment of the complete
   * database should be assumed.
   *
   * @return the list of upgrade steps; or an empty list if "everything" should be done.
   */
  public List<UpgradeStep> getSteps() {
    return steps;
  }


  /**
   *
   * @see org.alfasoftware.morf.upgrade.SqlStatementWriter#writeSql(Collection)
   */
  @Override
  public void writeSql(Collection<String> statements) {
    this.sql.addAll(statements);
  }


  /**
   * @return the sql
   */
  public List<String> getSql() {
    List<String> results = Lists.newArrayList(sql);

    for (UpgradeScriptAddition addition : upgradeScriptAdditions) {
      Iterables.addAll(results, addition.sql());
    }

    return Collections.unmodifiableList(results);
  }


  /**
   * Returns whether it contains an upgrade path. i.e. if we have either
   * {@link #getSteps()} or {@link #getSql()}.
   *
   * @return true if there is a path.
   */
  public boolean hasStepsToApply() {
    return !getSteps().isEmpty() || !sql.isEmpty();
  }


  /**
   * Log out the SQL that forms this upgrade path.
   */
  public void logUpgradePathSQL() {
    logUpgradePathSQL(log);
  }


  /**
   * Log out the SQL that forms this upgrade path to a logger of your choice.
   *
   * @param logger the logger to use.
   */
  public void logUpgradePathSQL(Log logger) {
    if (sql.isEmpty()) {
      logger.info("No upgrade statements to be applied");
    } else {
      logger.info("Upgrade statements:\n" + getUpgradeSqlScript());
    }
  }


  /**
   * @return A single string of the sql statements appended to each other
   * appropriately
   */
  public String getUpgradeSqlScript() {
    final StringBuilder sqlOutput = new StringBuilder();
    for (final String sqlStatement : getSql()) {
      sqlOutput.append(sqlDialect.formatSqlStatement(sqlStatement));
      sqlOutput.append(System.getProperty("line.separator"));
    }
    return sqlOutput.toString();
  }


  /**
   * Factory interface that can be used to create {@link UpgradePath}s.
   *
   * @author Copyright (c) Alfa Financial Software 2015
   */
  @ImplementedBy(UpgradePathFactoryImpl.class)
  public static interface UpgradePathFactory {

    /**
     * Creates an instance of {@link UpgradePath}.
     *
     * @param sqlDialect The SqlDialect.
     * @return The resulting {@link UpgradePath}.
     */
    public UpgradePath create(SqlDialect sqlDialect);


    /**
     * Creates an instance of {@link UpgradePath}.
     *
     * @param steps The steps represented by the {@link UpgradePath}.
     * @param sqlDialect The SqlDialect.
     * @return The resulting {@link UpgradePath}.
     */
    public UpgradePath create(List<UpgradeStep> steps, SqlDialect sqlDialect);
  }


  /**
   * Implementation of {@link UpgradePathFactory}.
   *
   * @author Copyright (c) Alfa Financial Software 2015
   */
  static final class UpgradePathFactoryImpl implements UpgradePathFactory {

    private final Set<UpgradeScriptAddition> upgradeScriptAdditions;

    @Inject
    UpgradePathFactoryImpl(Set<UpgradeScriptAddition> upgradeScriptAdditions) {
      this.upgradeScriptAdditions = upgradeScriptAdditions;
    }

    @Override
    public UpgradePath create(SqlDialect sqlDialect) {
      return new UpgradePath(upgradeScriptAdditions, sqlDialect);
    }

    @Override
    public UpgradePath create(List<UpgradeStep> steps, SqlDialect sqlDialect) {
      return new UpgradePath(upgradeScriptAdditions, steps, sqlDialect);
    }
  }
}
