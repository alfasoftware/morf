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

import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.TreeMap;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.Statement;
import org.alfasoftware.morf.sql.element.FieldLiteral;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Supplier;
import com.google.common.collect.ListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;

/**
 * Class which produces human readable statements from a
 * supplied list of upgrade steps.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class HumanReadableStatementProducer {

  /**
   * The graph of upgrade steps.
   */
  private final UpgradeGraph upgradeGraph;


  /**
   * Control flag for whether to report data transformation descriptions from the upgrade steps.
   */
  private final boolean reportDataChanges;


  /**
   * Control flag for the preferred database dialect to use when reporting data transformation
   * descriptions that are written in pure SQL. This is ignored if {@link #reportDataChanges} is
   * false.
   */
  private final String preferredSQLDialect;


  /**
   * Constructs a new {@link HumanReadableStatementProducer}.
   *
   * <p>When constructed in this form the producer will only describe structural changes and not include
   * any data transformations.</p>
   *
   * @param upgradeSteps a list of upgrade steps in any order.
   */
  public HumanReadableStatementProducer(final Collection<Class<? extends UpgradeStep>> upgradeSteps) {
    this(upgradeSteps, false, null);
  }


  /**
   * Constructs a new {@link HumanReadableStatementProducer}.
   *
   * @param upgradeSteps a list of upgrade steps in any order.
   * @param includeDataChanges {@code true} to report on both structural and data changes, {@code false}
   *          to report on only structural changes.
   * @param preferredSQLDialect the SQL dialect to use when reporting on data changes that cannot be
   *          expressed in human readable form.
   */
  public HumanReadableStatementProducer(final Collection<Class<? extends UpgradeStep>> upgradeSteps,
                                        final boolean includeDataChanges,
                                        final String preferredSQLDialect) {
    super();
    this.upgradeGraph = new UpgradeGraph(upgradeSteps);
    this.reportDataChanges = includeDataChanges;
    this.preferredSQLDialect = preferredSQLDialect;
  }


  /**
   * Produces output via the supplied consumer.
   *
   * @param consumer the consumer to consume the events.
   */
  public void produceFor(final HumanReadableStatementConsumer consumer) {

    // Ensure the upgrade steps are in the correct order
    final Collection<Class<? extends UpgradeStep>> upgradeSteps = upgradeGraph.orderedSteps();

    //Create a Multimap which has version ordered keys but insertion ordered values
    ListMultimap<String, UpgradeStep> orderedUpgradeSteps = Multimaps.newListMultimap(
      Maps.<String, Collection<UpgradeStep>>newTreeMap(new TreeMap<String, Collection<UpgradeStep>>(
          new Comparator<String>() {
            @Override
            public int compare(String o1, String o2) {
              return versionCompare(o1, o2);
            }
          })
        ),
      new Supplier<List<UpgradeStep>>() {
        @Override
        public List<UpgradeStep> get() {
          return Lists.newLinkedList();
        }
      });

    // Iterate over the upgrade steps initialising them, and reordering by version and then sequence
    for (Class<? extends UpgradeStep> currentStepClass : upgradeSteps) {
      try {
        // Create an instance of the upgrade step
        Constructor<? extends UpgradeStep> constructor = currentStepClass.getDeclaredConstructor();
        constructor.setAccessible(true);
        UpgradeStep step = constructor.newInstance();
        orderedUpgradeSteps.put(getUpgradeStepVersion(step), step);

      } catch (Exception e) {
        throw new IllegalStateException("Cannot instantiate upgrade step [" + currentStepClass.getName() + "]", e);
      }
    }

    // Create a proxy schema editor to pass through the consumer events
    SchemaEditor schemaEditor = new SchemaEditor() {

      /** @see org.alfasoftware.morf.upgrade.SchemaEditor#addColumn(java.lang.String, org.alfasoftware.morf.metadata.Column) **/
      @Override
      public void addColumn(String tableName, Column definition, FieldLiteral columnDefault) {
        consumer.schemaChange(HumanReadableStatementHelper.generateAddColumnString(tableName, definition, columnDefault));
      }

      /**
       * @see org.alfasoftware.morf.upgrade.SchemaEditor#addColumn(java.lang.String, org.alfasoftware.morf.metadata.Column)
       */
      @Override
      public void addColumn(String tableName, Column definition) {
        consumer.schemaChange(HumanReadableStatementHelper.generateAddColumnString(tableName, definition));
      }

      /** @see org.alfasoftware.morf.upgrade.SchemaEditor#addIndex(java.lang.String, org.alfasoftware.morf.metadata.Index) **/
      @Override
      public void addIndex(String tableName, Index index) {
        consumer.schemaChange(HumanReadableStatementHelper.generateAddIndexString(tableName, index));
      }

      /** @see org.alfasoftware.morf.upgrade.SchemaEditor#addTable(org.alfasoftware.morf.metadata.Table) **/
      @Override
      public void addTable(Table definition) {
        consumer.schemaChange(HumanReadableStatementHelper.generateAddTableString(definition));
      }

      /** @see org.alfasoftware.morf.upgrade.SchemaEditor#changeColumn(java.lang.String, org.alfasoftware.morf.metadata.Column, org.alfasoftware.morf.metadata.Column) **/
      @Override
      public void changeColumn(String tableName, Column fromDefinition, Column toDefinition) {
        consumer.schemaChange(HumanReadableStatementHelper.generateChangeColumnString(tableName, fromDefinition, toDefinition));
      }

      /** @see org.alfasoftware.morf.upgrade.SchemaEditor#changeIndex(java.lang.String, org.alfasoftware.morf.metadata.Index, org.alfasoftware.morf.metadata.Index) **/
      @Override
      public void changeIndex(String tableName, Index fromIndex, Index toIndex) {
        consumer.schemaChange(HumanReadableStatementHelper.generateChangeIndexString(tableName, fromIndex, toIndex));
      }

      /** @see org.alfasoftware.morf.upgrade.SchemaEditor#removeColumn(java.lang.String, org.alfasoftware.morf.metadata.Column) **/
      @Override
      public void removeColumn(String tableName, Column definition) {
        consumer.schemaChange(HumanReadableStatementHelper.generateRemoveColumnString(tableName, definition));
      }

      /**
       * @see org.alfasoftware.morf.upgrade.SchemaEditor#removeColumns(java.lang.String, org.alfasoftware.morf.metadata.Column[])
       */
      @Override
      public void removeColumns(String tableName, Column... definitions) {
        for (Column definition : definitions) {
          removeColumn(tableName, definition);
        }
      }

      /** @see org.alfasoftware.morf.upgrade.SchemaEditor#removeIndex(java.lang.String, org.alfasoftware.morf.metadata.Index) **/
      @Override
      public void removeIndex(String tableName, Index index) {
        consumer.schemaChange(HumanReadableStatementHelper.generateRemoveIndexString(tableName, index));
      }


      /**
       * @see org.alfasoftware.morf.upgrade.SchemaEditor#renameIndex(java.lang.String, java.lang.String, java.lang.String)
       */
      @Override
      public void renameIndex(String tableName, String fromIndexName, String toIndexName) {
        consumer.schemaChange(HumanReadableStatementHelper.generateRenameIndexString(tableName, fromIndexName, toIndexName));

      }

      /** @see org.alfasoftware.morf.upgrade.SchemaEditor#removeTable(org.alfasoftware.morf.metadata.Table) **/
      @Override
      public void removeTable(Table table) {
        consumer.schemaChange(HumanReadableStatementHelper.generateRemoveTableString(table));
      }

      @Override
      public void renameTable(String fromTableName, String toTableName) {
        consumer.schemaChange(HumanReadableStatementHelper.generateRenameTableString(fromTableName, toTableName));
      }

      @Override
      public void changePrimaryKeyColumns(String tableName, List<String> oldPrimaryKeyColumns, List<String> newPrimaryKeyColumns) {
        consumer.schemaChange(HumanReadableStatementHelper.generateChangePrimaryKeyColumnsString(tableName, oldPrimaryKeyColumns, newPrimaryKeyColumns));
      }


      @Override
      public void correctPrimaryKeyColumns(String tableName, List<String> newPrimaryKeyColumns) {
        consumer.schemaChange(HumanReadableStatementHelper.generateChangePrimaryKeyColumnsString(tableName, newPrimaryKeyColumns));
      }

      @Override
      public void addTableFrom(Table table, SelectStatement select) {
        consumer.schemaChange(HumanReadableStatementHelper.generateAddTableFromString(table, select));
      }
    };

    //Similarly, we need a proxy DataEditor
    DataEditor dataEditor = new DataEditor () {
      @Override
      public void executeStatement(Statement statement) {
        if (reportDataChanges) {
          consumer.dataChange(HumanReadableStatementHelper.generateDataUpgradeString(statement, preferredSQLDialect));
        }
      }
    };


    //Iterate over versions, then over the ordered upgrade steps
    for (String version : orderedUpgradeSteps.keySet()) {
      consumer.versionStart("ALFA " + version);
      for (UpgradeStep currentStep : orderedUpgradeSteps.get(version)) {
        // Indicate to the consumer that the upgrade step has started
        consumer.upgradeStepStart(currentStep.getClass().getSimpleName(), currentStep.getDescription(), currentStep.getJiraId());

        // Fire all the actual schema change events
        currentStep.execute(schemaEditor, dataEditor);

        // Indicate to the consumer that the upgrade step has ended
        consumer.upgradeStepEnd(currentStep.getClass().getSimpleName());
      }
      consumer.versionEnd("ALFA " + version);
    }
  }


  /**
   * Gets the version the upgrade {@code step} belongs in.
   * First attempts to pull the version from the {@code @Version}
   * annotation, otherwise from the package name.
   *
   * @param step the upgrade step.
   * @return the version the upgrade step belongs in.
   */
  private String getUpgradeStepVersion(UpgradeStep step) {
    Version versionAnnotation = step.getClass().getAnnotation(Version.class);
    if (versionAnnotation!=null) {
      return "v".concat(versionAnnotation.value());
    }

    String version = step.getClass().getPackage().getName();
    version = version.substring(version.lastIndexOf('.') + 1);
    return version.replace('_', '.');
  }


  /**
   * Compare two version strings. This differs from natural ordering
   * as a version of 5.3.27 is higher than 5.3.3.
   * @param str1 One version string to compare
   * @param str2 The other version string to compare
   * @return a negative integer, zero, or a positive integer as the
   *         first argument is less than, equal to, or greater than the
   *         second.   */
  @VisibleForTesting
  protected static Integer versionCompare(String str1, String str2) {
    String[] vals1 = str1.split("\\.");
    String[] vals2 = str2.split("\\.");

    // set index to first non-equal ordinal or length of shortest version string
    int i = 0;
    while (i < vals1.length && i < vals2.length && vals1[i].equals(vals2[i])) {
      i++;
    }
    // compare first non-equal ordinal number
    if (i < vals1.length && i < vals2.length) {
      try {
        int diff = Integer.valueOf(vals1[i]).compareTo(Integer.valueOf(vals2[i]));
        return Integer.signum(diff);
      } catch (NumberFormatException e) {
        return Integer.signum(vals1[i].compareTo(vals2[i]));
      }
    }
    // the strings are equal or one string is a substring of the other
    // e.g. "1.2.3" = "1.2.3" or "1.2.3" < "1.2.3.4"
    else {
      return Integer.signum(vals1.length - vals2.length);
    }
  }
}
