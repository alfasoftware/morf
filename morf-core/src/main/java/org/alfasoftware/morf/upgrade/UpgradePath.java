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
import java.util.function.Supplier;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Suppliers;
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
   * Unmodifiable list of SQL statements to execute before
   * any other SQL, if there is other SQL to be executed.
   */
  private final List<String> initialisationSql;

  /**
   * The SQL to execute.
   */
  private final List<String> sql = new LinkedList<>();

  /**
   * Unmodifiable list of SQL statements to execute after
   * all other SQL, if other SQL has been executed.
   */
  private final List<String> finalisationSql;

  /**
   * The status of the upgrade, at the point of creation, if this is a
   * "placeholder" upgrade path.
   */
  private final UpgradeStatus upgradeStatus;

  /**
   * Supplier of {@link GraphBasedUpgrade}. May supply null if
   * {@link GraphBasedUpgrade} instance is not available.
   */
  private final Supplier<GraphBasedUpgrade> graphBasedUpgradeSupplier;


  /**
   * Create a new complete deployment.
   *
   * @param upgradeScriptAdditions The SQL to be appended to the upgrade.
   * @param sqlDialect the SQL dialect being used for this upgrade path
   * @param initialisationSql the SQL to execute before all other, if and only if there is other SQL to execute.
   * @param finalisationSql the SQL to execute after all other, if and only if there is other SQL to execute.
   */
  public UpgradePath(Set<UpgradeScriptAddition> upgradeScriptAdditions, SqlDialect sqlDialect, List<String> initialisationSql, List<String> finalisationSql) {
    this(upgradeScriptAdditions, new ArrayList<UpgradeStep>(), sqlDialect, initialisationSql, finalisationSql, null);
  }


  /**
   * Create a new upgrade for the given list of steps. Graph based upgrade will not be available.
   *
   * @param upgradeScriptAdditions The SQL to be appended to the upgrade.
   * @param steps the upgrade steps to run
   * @param sqlDialect the SQL dialect being used for this upgrade path
   * @param initialisationSql the SQL to execute before all other, if and only if there is other SQL to execute.
   * @param finalisationSql the SQL to execute after all other, if and only if there is other SQL to execute.
   */
  public UpgradePath(Set<UpgradeScriptAddition> upgradeScriptAdditions, List<UpgradeStep> steps, SqlDialect sqlDialect, List<String> initialisationSql, List<String> finalisationSql) {
    this(upgradeScriptAdditions, steps, sqlDialect, initialisationSql, finalisationSql, null);
  }


  /**
   * Create a new upgrade for the given list of steps.
   *
   * @param upgradeScriptAdditions The SQL to be appended to the upgrade.
   * @param steps the upgrade steps to run
   * @param sqlDialect the SQL dialect being used for this upgrade path
   * @param initialisationSql the SQL to execute before all other, if and only if there is other SQL to execute.
   * @param finalisationSql the SQL to execute after all other, if and only if there is other SQL to execute.
   * @param graphBasedUpgradeBuilder prepares {@link GraphBasedUpgrade} instance, may be null if graph based upgrade is not available
   */
  public UpgradePath(Set<UpgradeScriptAddition> upgradeScriptAdditions, List<UpgradeStep> steps, SqlDialect sqlDialect, List<String> initialisationSql, List<String> finalisationSql, GraphBasedUpgradeBuilder graphBasedUpgradeBuilder) {
    super();
    this.steps = Collections.unmodifiableList(steps);
    this.sqlDialect = sqlDialect;
    this.upgradeScriptAdditions = upgradeScriptAdditions;
    this.upgradeStatus = null;
    this.initialisationSql = initialisationSql;
    this.finalisationSql = finalisationSql;
    this.graphBasedUpgradeSupplier = Suppliers.memoize(() -> graphBasedUpgradeBuilder != null ? graphBasedUpgradeBuilder.prepareGraphBasedUpgrade() : null);
  }


  /**
   * Used to create an upgrade path with the given upgrade status.
   *
   * @param upgradeStatus Upgrade status to hold.
   */
  UpgradePath(UpgradeStatus upgradeStatus) {
    super();
    this.steps = Collections.emptyList();
    this.sqlDialect = null;
    this.upgradeScriptAdditions = Collections.emptySet();
    this.upgradeStatus = upgradeStatus;
    this.initialisationSql = null;
    this.finalisationSql = null;
    this.graphBasedUpgradeSupplier = Suppliers.memoize(() -> null);
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
    List<String> results = Lists.newLinkedList();
    if (!sql.isEmpty() || !upgradeScriptAdditions.isEmpty())
      results.addAll(initialisationSql);

    results.addAll(sql);

    for (UpgradeScriptAddition addition : upgradeScriptAdditions) {
      Iterables.addAll(results, addition.sql());
    }

    if (!results.isEmpty())
      results.addAll(finalisationSql);

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
   * Returns whether or not this upgrade knew that an upgrade was in progress
   * at the point it was created.
   *
   * @return true if there was an upgrade in progress.
   */
  public boolean upgradeInProgress() {
    return upgradeStatus != null && upgradeStatus != UpgradeStatus.NONE;
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

    addCommentsToDropUpgradeStatusTable(sqlOutput);

    return sqlOutput.toString();
  }


  /**
   * At the end of the StringBuilder given, add comments to explain how to drop
   * {@value UpgradeStatusTableService#UPGRADE_STATUS} table if the upgrade is
   * done manually.
   */
  private void addCommentsToDropUpgradeStatusTable(final StringBuilder sqlOutput) {
    String separator = System.getProperty("line.separator");
    sqlOutput.append("-- WARNING - This upgrade step creates a temporary table " + UpgradeStatusTableService.UPGRADE_STATUS + "." + separator);
    sqlOutput.append("-- WARNING - If the upgrade is run automatically, the table will be automatically removed at a later point." + separator);
    sqlOutput.append("-- WARNING - If this step is being applied manually, the table must be manually removed - to do so, uncomment the following SQL lines." + separator);
    sqlOutput.append("-- WARNING - Manual removal should not be applied during full deployment of the application to an empty database." + separator);
    for (String statement : sqlDialect.dropStatements(SchemaUtils.table(UpgradeStatusTableService.UPGRADE_STATUS))) {
      sqlOutput.append("-- " + statement + separator);
    }
  }


  /**
   * @return {@link GraphBasedUpgrade} instance if it's available - may return
   *         null
   */
  public GraphBasedUpgrade getGraphBasedUpgradeUpgrade() {
    return graphBasedUpgradeSupplier.get();
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


    /**
     * Creates an instance of {@link UpgradePath}.
     *
     * @param steps The steps represented by the {@link UpgradePath}.
     * @param sqlDialect The SqlDialect.
     * @param graphBasedUpgradeBuilder to be used to create a graph based upgrade if needed
     * @return The resulting {@link UpgradePath}.
     */
    public UpgradePath create(List<UpgradeStep> steps, SqlDialect sqlDialect, GraphBasedUpgradeBuilder graphBasedUpgradeBuilder);
  }


  /**
   * Implementation of {@link UpgradePathFactory}.
   *
   * @author Copyright (c) Alfa Financial Software 2015
   */
  static final class UpgradePathFactoryImpl implements UpgradePathFactory {

    private final Set<UpgradeScriptAddition> upgradeScriptAdditions;
    private final UpgradeStatusTableService upgradeStatusTableService;

    @Inject
    UpgradePathFactoryImpl(Set<UpgradeScriptAddition> upgradeScriptAdditions, UpgradeStatusTableService upgradeStatusTableService) {
      super();
      this.upgradeScriptAdditions = upgradeScriptAdditions;
      this.upgradeStatusTableService = upgradeStatusTableService;
    }


    @Override
    public UpgradePath create(SqlDialect sqlDialect) {
      return new UpgradePath(upgradeScriptAdditions, sqlDialect,
                             upgradeStatusTableService.updateTableScript(UpgradeStatus.NONE, UpgradeStatus.IN_PROGRESS),
                             upgradeStatusTableService.updateTableScript(UpgradeStatus.IN_PROGRESS, UpgradeStatus.DATA_TRANSFER_REQUIRED));
    }


    @Override
    public UpgradePath create(List<UpgradeStep> steps, SqlDialect sqlDialect) {
      return new UpgradePath(upgradeScriptAdditions, steps, sqlDialect,
                             upgradeStatusTableService.updateTableScript(UpgradeStatus.NONE, UpgradeStatus.IN_PROGRESS),
                             upgradeStatusTableService.updateTableScript(UpgradeStatus.IN_PROGRESS, UpgradeStatus.COMPLETED),
                             null);
    }


    @Override
    public UpgradePath create(List<UpgradeStep> steps, SqlDialect sqlDialect, GraphBasedUpgradeBuilder graphBasedUpgradeBuilder) {
      return new UpgradePath(upgradeScriptAdditions, steps, sqlDialect,
                             upgradeStatusTableService.updateTableScript(UpgradeStatus.NONE, UpgradeStatus.IN_PROGRESS),
                             upgradeStatusTableService.updateTableScript(UpgradeStatus.IN_PROGRESS, UpgradeStatus.COMPLETED),
                             graphBasedUpgradeBuilder);
    }
  }
}
