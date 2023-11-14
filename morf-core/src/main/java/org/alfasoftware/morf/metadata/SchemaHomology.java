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

package org.alfasoftware.morf.metadata;

import static java.util.regex.Pattern.CASE_INSENSITIVE;
import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;
import static org.alfasoftware.morf.metadata.SchemaUtils.toUpperCase;
import static org.alfasoftware.morf.metadata.SchemaUtils.upperCaseNamesOfColumns;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;


/**
 * Measures the extent to which schemas are identical or corresponding in position,
 * value, structure or function.
 *
 * <p>Differences can be logged to a {@link DifferenceWriter}, if one is provided.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class SchemaHomology {

  private static final Log log = LogFactory.getLog(SchemaHomology.class);

  /**
   * Records differences between schema elements.
   */
  private final DifferenceWriter differenceWriter;

  /**
   * The name to use when referring to schema 1 in difference logs.
   */
  private final String schema1Name;

  /**
   * The name to use when referring to schema 2 in difference logs.
   */
  private final String schema2Name;

  /**
   * Records whether any differences have been found.
   */
  private boolean noDifferences;


  /**
   * Default constructor.
   *
   * <p>Details of differences will not be reported other than the overall result.
   * If full difference details are required create using {@link #SchemaHomology(DifferenceWriter)}.</p>
   */
  public SchemaHomology() {
    this(new NullDifferenceWriter());
  }


  /**
   * Use a difference with default schema names.
   *
   * <p>Prefer the constructor that takes schema names, as the logs will be much more readable.</p>
   *
   * @param differenceWriter Records differences between schema elements.
   */
  public SchemaHomology(DifferenceWriter differenceWriter) {
    this(differenceWriter, "schema 1", "schema 2");
  }


  /**
   * @param differenceWriter The write against which to write difference logs.
   * @param schema1Name A name to use for schema 1 in the logs.
   * @param schema2Name A name to use for schema 2 in the logs.
   */
  public SchemaHomology(DifferenceWriter differenceWriter, String schema1Name, String schema2Name) {
    super();
    this.differenceWriter = differenceWriter;
    this.schema1Name = schema1Name;
    this.schema2Name = schema2Name;
  }


  /**
   * @param schema1 First schema to compare.
   * @param schema2 Second schema to compare.
   * @param exclusionRegex A {@link Collection} of regular expressions that corresponds to tables that will be ignored if only found in one schema.
   * @return True if each of the tables in each schema matches a table in the other schema using {@link #tablesMatch(Table, Table)}.
   */
  public boolean schemasMatch(Schema schema1, Schema schema2, Collection<String> exclusionRegex) {
    noDifferences = true;
    HashMap<String, Table> schema2Tables = new HashMap<>();
    for (Table table : schema2.tables()) {
      schema2Tables.put(table.getName().toUpperCase(), table);
    }

    // Collect the names of the tables with differences, so they can be checked against the
    Set<String> additionalTablesInSchema1 = Sets.newHashSet();
    Set<String> additionalTablesInSchema2 = Sets.newHashSet();

    for (Table table : schema1.tables()) {
      Table table2 = schema2Tables.remove(table.getName().toUpperCase());
      if (table2 == null) {
        additionalTablesInSchema1.add(table.getName().toUpperCase());
      } else {
        checkTable(table, table2);
      }
    }

    // Any tables left in schema2 haven't been found in both schemas
    for (Table table : schema2Tables.values()) {
      additionalTablesInSchema2.add(table.getName().toUpperCase());
    }

    // Remove table entries that match any of the exclusion regexes.
    for (String currentRegex : exclusionRegex) {
      Pattern currentPattern;
      try {
        currentPattern = Pattern.compile(currentRegex, CASE_INSENSITIVE);
      } catch (PatternSyntaxException e) {
        throw new RuntimeException(String.format("Could not compile regular expression: [%s]", currentRegex), e);
      }

      for (Iterator<String> iterator = additionalTablesInSchema1.iterator(); iterator.hasNext(); ) {
        String tableName = iterator.next();
        if (currentPattern.matcher(tableName).matches()) {
          log.info(String.format("Table [%s] is not present in %s but was ignored as it matches the exclusion pattern [%s]", tableName, schema2, currentRegex));
          iterator.remove();
        }
      }

      for (Iterator<String> iterator = additionalTablesInSchema2.iterator(); iterator.hasNext(); ) {
        String tableName = iterator.next();
        if (currentPattern.matcher(tableName).matches()) {
          log.info(String.format("Table [%s] is not present in %s but was ignored as it matches the exclusion pattern [%s]", tableName, schema1, currentRegex));
          iterator.remove();
        }
      }
    }

    for (String tableName : additionalTablesInSchema1) {
      difference("Table [" + tableName + "] is present in " + schema1Name + " but was not found in " + schema2Name);
    }

    for (String tableName : additionalTablesInSchema2) {
      difference("Table [" + tableName + "] is present in " + schema2Name + " but was not found in " + schema1Name);
    }

    return noDifferences;
  }


  /**
   * Compare two tables.
   *
   * @param table1 Table 1
   * @param table2 Table 2
   * @return Whether they match.
   */
  public boolean tablesMatch(Table table1, Table table2) {
    noDifferences = true;
    checkTable(table1, table2);
    return noDifferences;
  }


  /**
   * Compare two columns.
   *
   * @param column1 Column 1
   * @param column2 Column 2
   * @return Whether they match.
   */
  public boolean columnsMatch(Column column1, Column column2) {
    noDifferences = true;
    checkColumn(null, column1, column2);
    return noDifferences;
  }


  /**
   * Compare two indexes.
   *
   * @param index1 Index 1
   * @param index2 Index 2
   * @return Whether they match.
   */
  public boolean indexesMatch(Index index1, Index index2) {
    noDifferences = true;
    checkIndex("unspecified", index1, index2);
    return noDifferences;
  }


  /**
   * Write out a difference.
   *
   * @param difference The difference log.
   */
  private void difference(String difference) {
    differenceWriter.difference(difference);
    noDifferences = false;
  }


  /**
   * @param table1 First table to compare.
   * @param table2 Second table to compare.
   */
  private void checkTable(Table table1, Table table2) {
    matches("Table name", table1.getName().toUpperCase(), table2.getName().toUpperCase());

    checkColumns(table1.getName(), table1.columns(), table2.columns());

    checkIndexes(table1.getName(), table1.indexes(), table2.indexes());

    checkPrimaryKeys(table1.getName(), upperCaseNamesOfColumns(primaryKeysForTable(table1)), upperCaseNamesOfColumns(primaryKeysForTable(table2)));
  }


  /**
   * Checks the ordering of the primary keys.
   */
  private void checkPrimaryKeys(String tableName, List<String> table1UpperCaseKeys, List<String> table2UpperCaseKeys) {
    if (table1UpperCaseKeys.size() != table2UpperCaseKeys.size()) {
      difference(String.format("Primary key column count on table [%s] does not match. Column are [%s] and [%s]", tableName, table1UpperCaseKeys, table2UpperCaseKeys));
      return;
    }
    for (int i = 0 ; i < table1UpperCaseKeys.size(); i++) {
      if (!StringUtils.equals(table1UpperCaseKeys.get(i), table2UpperCaseKeys.get(i))) {
        difference(String.format("Primary key at index [%d] on table [%s] does not match. Columns are [%s] and [%s]", i, tableName, table1UpperCaseKeys.get(i), table2UpperCaseKeys.get(i)));
      }
    }
  }


  /**
   * @param tableName The table which contains the columns. Can be null, in which case the errors don't contain the table name.
   * @param column1 First column to compare.
   * @param column2 Second column to compare.
   */
  private void checkColumn(String tableName, Column column1, Column column2) {
    matches("Column name", column1.getUpperCaseName(), column2.getUpperCaseName());
    String prefix = "Column [" + column1.getName() + "] " + (tableName == null ? "" :  "on table [" + tableName + "] ");
    matches(prefix + "data type", column1.getType(), column2.getType());
    matches(prefix + "nullable", column1.isNullable(), column2.isNullable());
    matches(prefix + "primary key", column1.isPrimaryKey(), column2.isPrimaryKey());
    matches(prefix + "default value", column1.getDefaultValue(), column2.getDefaultValue());
    matches(prefix + "autonumber", column1.isAutoNumbered(), column2.isAutoNumbered());
    if (column1.isAutoNumbered()) {
      matches(prefix + "autonumber start", column1.getAutoNumberStart(), column2.getAutoNumberStart());
    }

    if (column1.getType().hasWidth()) {
      matches(prefix + "width", column1.getWidth(), column2.getWidth());
    }

    if (column1.getType().hasScale()) {
      matches(prefix + "scale", column1.getScale(), column2.getScale());
    }
  }


  /**
   * Compare the columns of two tables.

   * @param tableName The table from which the columns came
   * @param columns1 The first set of columns
   * @param columns2 The second set of columns
   */
  private void checkColumns(String tableName, List<Column> columns1, List<Column> columns2) {
    // Note that we compare all columns even once the first mismatch is detected.

    Map<String, Column> columns2Source = new HashMap<>();
    for (Column column : columns2) {
      columns2Source.put(column.getUpperCaseName(), column);
    }

    for (Column column1 : columns1) {
      Column column2 = columns2Source.remove(column1.getUpperCaseName());
      if (column2 == null) {
        difference("Column [" + column1.getName() + "] on table [" + tableName + "] not found in " + schema2Name);
      } else {
        checkColumn(tableName, column1, column2);
      }
    }

    for (Column column : columns2Source.values()) {
      difference("Column [" + column.getName() + "] on table [" + tableName + "] not found in " + schema1Name);
    }
  }


  /**
   * Compare the indexes of two tables.
   *
   * @param tableName Name of the table on which we are comparing indexes.
   * @param indexes1 The first set of indexes
   * @param indexes2 The second set of indexes
   */
  private void checkIndexes(String tableName, List<Index> indexes1, List<Index> indexes2) {
    Map<String, Index> sourceIndexes2 = new HashMap<>();
    for (Index index : indexes2) {
      sourceIndexes2.put(index.getName().toUpperCase(), index);
    }

    // Comparison of indexes is not order dependent
    for (Index index1 : indexes1) {
      // Find the match
      Index index2 = sourceIndexes2.remove(index1.getName().toUpperCase());

      if (index2 == null) {
        difference("Index [" + index1.getName() + "] on table [" + tableName + "] not found in " + schema2Name);
      } else {
        checkIndex(tableName, index1, index2);
      }
    }

    for(Index index : sourceIndexes2.values()) {
      difference("Index [" + index.getName() + "] on table [" + tableName + "] not found in " + schema1Name);
    }
  }


  /**
   * Check two indexes match.
   *
   * @param index1 index to compare
   * @param index2 index to compare
   */
  private void checkIndex(String tableName, Index index1, Index index2) {
    matches("Index name on table [" + tableName + "]", index1.getName().toUpperCase(), index2.getName().toUpperCase());
    matches("Index [" + index1.getName() + "] on table [" + tableName + "] uniqueness", index1.isUnique(), index2.isUnique());
    matches("Index [" + index1.getName() + "] on table [" + tableName + "] columnNames", toUpperCase(index1.columnNames()), toUpperCase(index2.columnNames()));
  }


  /**
   * Check two objects match, writing a difference if they don't.
   *
   * @param description The description to log when the match fails
   * @param value1 The first value
   * @param value2 The second value
   */
  private void matches(String description, Object value1, Object value2) {
    if (ObjectUtils.notEqual(value1, value2)) {
      difference(String.format("%s does not match: [%s] in %s, [%s] in %s", description, value1, schema1Name, value2, schema2Name));
    }
  }


  /**
   * Defines contract for receiving differences between schema elements.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  public static interface DifferenceWriter {

    /**
     * @param message Textual message identifying a single difference.
     */
    public void difference(String message);
  }


  /**
   * No op implementation of {@link DifferenceWriter}.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private static final class NullDifferenceWriter implements DifferenceWriter {

    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.metadata.SchemaHomology.DifferenceWriter#difference(java.lang.String)
     */
    @Override
    public void difference(String message) {
      // Do nothing.
    }
  }


  /**
   * RuntimeException-throwing version of {@link DifferenceWriter}.
   */
  public static final class ThrowingDifferenceWriter implements DifferenceWriter {

    /**
     * @see org.alfasoftware.morf.metadata.SchemaHomology.DifferenceWriter#difference(java.lang.String)
     */
    @Override
    public void difference(String message) {
      throw new RuntimeException("Difference reported: " + message);
    }
  }

  /**
   * RuntimeException-throwing version of {@link DifferenceWriter}.
   */
  public static final class CollectingDifferenceWriter implements DifferenceWriter {

    private final List<String> differences = Lists.newLinkedList();

    /**
     * @see org.alfasoftware.morf.metadata.SchemaHomology.DifferenceWriter#difference(java.lang.String)
     */
    @Override
    public void difference(String message) {
      differences.add(message);
    }

    /**
     * @return the differences
     */
    public List<String> differences() {
      return differences;
    }
  }
}
