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

import static org.alfasoftware.morf.metadata.SchemaUtils.namesOfColumns;
import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;
import static java.util.regex.Pattern.CASE_INSENSITIVE;

import java.util.*;
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

  private enum DifferenceType {
    ABSENT_FROM_SCHEMA_1,
    ABSENT_FROM_SCHEMA_2,
    PRIMARY_KEY_COUNT_MISMATCH,
    PRIMARY_KEY_DIFFERENCE,
    DIFFERENCE,
  }

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
      detailedDifference("Table", tableName, tableName,
               DifferenceType.ABSENT_FROM_SCHEMA_2, schema1Name, schema2Name);
  //    difference("Table [" + tableName + "] is present in " + schema1Name + " but was not found in " + schema2Name);
    }

    for (String tableName : additionalTablesInSchema2) {
      detailedDifference("Table", tableName, tableName,
              DifferenceType.ABSENT_FROM_SCHEMA_1, schema1Name, schema2Name);
 //     difference("Table [" + tableName + "] is present in " + schema2Name + " but was not found in " + schema1Name);
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

//
//  /**
//   * Write out a difference.
//   *
//   * @param difference The difference log.
//   */
//  private void simpleDifference(String difference) {
//    differenceWriter.difference(difference);
//    noDifferences = false;
//  }


  /**
   * Write out a detailed difference, falling back to a simple difference if required
   */
  private void detailedDifference(String objectType, String objectName,  String tableName, DifferenceType differenceType,
                                  String schema1Name, String schema2Name) {
    detailedDifference(objectType, objectName, tableName, "", differenceType, schema1Name, schema2Name, "", "");
  }


  private void detailedDifference(String objectType, String objectName, String tableName, String propertyName,
                                  DifferenceType differenceType, String schema1Name, String schema2Name,
                                  String value1, String value2) {


    String difference;
    String onObjectDescription;
    boolean detailed = differenceWriter instanceof DifferenceDetailWriter;

    if (detailed){
      onObjectDescription = "";
    } else {
      onObjectDescription = objectType + " [" + objectName + "]" + ((!objectType.equals("Table")) ? " on Table [" + tableName + "] " : " ");
    }

    switch (differenceType) {
      case ABSENT_FROM_SCHEMA_1:
        difference = onObjectDescription +
                "is present in " + schema2Name + " but was not found in " + schema1Name;
        break;
      case ABSENT_FROM_SCHEMA_2:
        difference = onObjectDescription +
                "is present in " + schema1Name + " but was not found in " + schema2Name;
        break;
      case PRIMARY_KEY_COUNT_MISMATCH:
        if (detailed) {
          difference = String.format("primary key count differs, count is [%s] in %s and [%s] in %s", value1, schema1Name, value2, schema2Name);
        } else {
          difference = String.format("Primary key column count on %s [%s] does not match. Column are [%s] in %s and [%s] in %s", objectType, objectName, value1, schema1Name, value2, schema2Name);
        }
        break;
      case PRIMARY_KEY_DIFFERENCE:
        if (detailed) {
          difference = String.format("primary key differs, columns are [%s] in %s and [%s] in %s", value1, schema1Name, value2, schema2Name);
        } else {
          difference = String.format("Primary key at [%s]%s does not match. Columns are [%s] and [%s]", objectName, onObjectDescription, value1, value2);
        }
        break;
      case DIFFERENCE:
          if (detailed) {
            difference = String.format("%s does not match: [%s] in %s, [%s] in %s ",
                    propertyName, value1, schema1Name, value2, schema2Name);

          } else {
            difference = String.format("%s does not match on %s %s%s: [%s] in %s, [%s] in %s ",
                propertyName, objectType, objectName, onObjectDescription, value1, schema1Name, value2, schema2Name);
          }
          break;
        default:
          difference = "Unknown difference";
    }

    if ( detailed ) {
      ((DifferenceDetailWriter) differenceWriter).detailedDifference(objectType, objectName, tableName,  propertyName,
              differenceType,  schema1Name, schema2Name, difference);
    } else{
      differenceWriter.difference(difference);
    }

    noDifferences = false;
  }





  /**
   * @param table1 First table to compare.
   * @param table2 Second table to compare.
   */
  private void checkTable(Table table1, Table table2) {
    matches("Table", table1.getName(),table1.getName(),"Table name",
            table1.getName().toUpperCase(), table2.getName().toUpperCase());

    checkColumns(table1.getName(), table1.columns(), table2.columns());

    checkIndexes(table1.getName(), table1.indexes(), table2.indexes());

    checkPrimaryKeys(table1.getName(), namesOfColumns(primaryKeysForTable(table1)), namesOfColumns(primaryKeysForTable(table2)));
  }


  /**
   * Checks the ordering of the primary keys.
   */
  private void checkPrimaryKeys(String tableName, List<String> table1Keys, List<String> table2Keys) {
    if (table1Keys.size() != table2Keys.size()) {
   //   difference(String.format("Primary key column count on table [%s] does not match. Column are [%s] and [%s]", tableName, table1Keys, table2Keys));
      detailedDifference("Table", tableName, tableName, "",
              DifferenceType.PRIMARY_KEY_COUNT_MISMATCH, schema1Name, schema2Name, String.join(",", table1Keys),
              String.join(",", table2Keys));
      return;
    }
    for (int i = 0 ; i < table1Keys.size(); i++) {
      if (!StringUtils.equalsIgnoreCase(table1Keys.get(i), table2Keys.get(i))) {
        detailedDifference("Primary Key", "Index "+i, tableName, "",
                DifferenceType.PRIMARY_KEY_DIFFERENCE, schema1Name, schema2Name, String.join(",", table1Keys),
                String.join(",", table2Keys) );
      }
    }
  }


  /**
   * @param tableName The table which contains the columns. Can be null, in which case the errors don't contain the table name.
   * @param column1 First column to compare.
   * @param column2 Second column to compare.
   */
  private void checkColumn(String tableName, Column column1, Column column2) {
    matches("Column", column1.getName(), tableName, "Column name", column1.getName().toUpperCase(), column2.getName().toUpperCase());
    matches("Column", column1.getName(), tableName, "data type", column1.getType(), column2.getType());
    matches("Column", column1.getName(), tableName, "nullable", column1.isNullable(), column2.isNullable());
    matches("Column", column1.getName(), tableName, "primary key", column1.isPrimaryKey(), column2.isPrimaryKey());
    matches("Column", column1.getName(), tableName, "default value", column1.getDefaultValue(), column2.getDefaultValue());
    matches("Column", column1.getName(), tableName, "autonumber", column1.isAutoNumbered(), column2.isAutoNumbered());
    if (column1.isAutoNumbered()) {
      matches("Column", column1.getName(), tableName, "autonumber start", column1.getAutoNumberStart(), column2.getAutoNumberStart());
    }

    if (column1.getType().hasWidth()) {
      matches("Column", column1.getName(), tableName, "width", column1.getWidth(), column2.getWidth());
    }

    if (column1.getType().hasScale()) {
      matches("Column", column1.getName(), tableName, "scale", column1.getScale(), column2.getScale());
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
      columns2Source.put(column.getName().toUpperCase(), column);
    }

    for (Column column1 : columns1) {
      Column column2 = columns2Source.remove(column1.getName().toUpperCase());
      if (column2 == null) {
   //     difference("Column [" + column1.getName() + "] on table [" + tableName + "] not found in " + schema2Name);
        detailedDifference("Column", column1.getName(), tableName,
                DifferenceType.ABSENT_FROM_SCHEMA_2, schema1Name, schema2Name);
      } else {
        checkColumn(tableName, column1, column2);
      }
    }

    for (Column column : columns2Source.values()) {
 //     difference("Column [" + column.getName() + "] on table [" + tableName + "] not found in " + schema1Name);
      detailedDifference("Column", column.getName(), tableName,
              DifferenceType.ABSENT_FROM_SCHEMA_1, schema1Name, schema2Name);
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
//        difference("Index [" + index1.getName() + "] on table [" + tableName + "] not found in " + schema2Name);
        detailedDifference("Index", index1.getName(),  tableName,
                DifferenceType.ABSENT_FROM_SCHEMA_2, schema1Name, schema2Name);
      } else {
        checkIndex(tableName, index1, index2);
      }
    }

    for(Index index : sourceIndexes2.values()) {
//      difference("Index [" + index.getName() + "] on table [" + tableName + "] not found in " + schema1Name);
      detailedDifference("Index", index.getName(),  tableName,
              DifferenceType.ABSENT_FROM_SCHEMA_1, schema1Name, schema2Name);
    }
  }


  /**
   * Check two indexes match.
   *
   * @param index1 index to compare
   * @param index2 index to compare
   */
  private void checkIndex(String tableName, Index index1, Index index2) {
    matches("Index", index1.getName(), tableName, "name", index1.getName().toUpperCase(), index2.getName().toUpperCase());
    matches("Index", index1.getName(), tableName, "uniqueness", index1.isUnique(), index2.isUnique());
    matches("Index", index1.getName(), tableName, "columnNames", toUpperCase(index1.columnNames()), toUpperCase(index2.columnNames()));
  }


  /**
   * Convert a list of Strings to upper case.
   * @param strings The source
   * @return The strings converted to upper case.
   */
  private List<String> toUpperCase(List<String> strings) {
    List<String> result = new ArrayList<>();
    for(String source : strings) {
      result.add(source.toUpperCase());
    }
    return result;
  }


  /**
   * Check two objects match, writing a difference if they don't.
   */
  private void matches(String objectType, String objectName, String tableName, String propertyName, Object value1, Object value2) {
    if (ObjectUtils.notEqual(value1, value2)) {
      detailedDifference(objectType, objectName, tableName, propertyName, DifferenceType.DIFFERENCE, schema1Name, schema2Name, value1.toString(), value2.toString());
    }
  }


  /**
   * Defines contract for receiving differences between schema elements.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  public interface DifferenceWriter {

    /**
     * @param message Textual message identifying a single difference.
     */
    void difference(String message);
  }

  /**
   * Defines contract for receiving differences between schema elements.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  public interface DifferenceDetailWriter extends DifferenceWriter {
    /**
     *
     * @param objectType is the type of the object with differences (e.g. "Index" or "Column")
     * @param objectName is the name of the object with differences
     * @param tableName is the name of the table with differences
     * @param differenceType is an enum denoting the type of difference
     * @param schema1Name is a description of the first schema being compared
     * @param schema2Name is a description of the second schema being compared
     */
    void detailedDifference(String objectType, String objectName, String tableName, String propertyName,
                                   DifferenceType differenceType, String schema1Name, String schema2Name, String message);
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
   * Collating version of {@link DifferenceWriter}.
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


  public static final class AggregatingDifferenceWriter implements DifferenceDetailWriter {

    class DetailedDifference {
       String objectType;
       String objectName;
       String tableName;
       String propertyName;
       DifferenceType differenceType;
       String schema1Name;
       String schema2Name;
       String message;

      public DetailedDifference(String objectType, String objectName, String tableName, String propertyName,
                                DifferenceType differenceType, String schema1Name, String schema2Name, String message) {
        this.objectType = objectType;
        this.objectName = objectName;
        this.tableName = tableName;
        this.propertyName = propertyName;
        this.differenceType = differenceType;
        this.schema1Name = schema1Name;
        this.schema2Name = schema2Name;
        this.message = message;
      }
    }

    private final List<DetailedDifference> detailedDifferenceList = new ArrayList<>();

    /**
     * This method should not be used on this class
     * Calls should only be made to detailedDifference, so this method throws a runtime exception if called
     *
     * @param message Textual message identifying a single difference.
     */
    @Override
    public void difference(String message) {
      throw new RuntimeException("difference method incorrectly called on DifferenceDetailWriter: " + message);
    }

    @Override
    public void detailedDifference(String objectType, String objectName, String tableName, String propertyName, DifferenceType differenceType, String schema1Name, String schema2Name, String message) {
        detailedDifferenceList.add(new DetailedDifference(objectType, objectName, tableName, propertyName, differenceType, schema1Name, schema2Name, message));
    }

    /**
     * @return the differences
     */
    public List<String> differences() {

      List<String> differences = new ArrayList<>();

      // Order by TableName, Object Type, Object Name, Property Name
      detailedDifferenceList.sort((d1, d2) -> d1.tableName.compareTo(d2.tableName) == 0
              ? d1.objectType.compareTo(d2.objectType) == 0
              ? d1.objectName.compareTo(d2.objectName) == 0
              ? d1.propertyName.compareTo(d2.propertyName)
              : d1.objectName.compareTo(d2.objectName)
              : d1.objectType.compareTo(d2.objectType)
              : d1.tableName.compareTo(d2.tableName));


      String lastTableName  = "123456";  // invalid table name so will not clash
      String lastObjectType = "123456";  // invalid type, so non-clashing
      String lastObjectName = "123456";  // invalid name, so non-clashing

      String tablePrefix      = "  Table ";
      String objectTypePrefix = "       ";
      String objectNamePrefix = "            ";
      String messagePrefix    = "                 ";

      for (DetailedDifference d : detailedDifferenceList) {
          if( ! lastTableName.equals(d.tableName)) {
              differences.add(tablePrefix + d.tableName);
              lastTableName = d.tableName;
          }
          if( ! lastObjectType.equals(d.objectType)) {
              differences.add(objectTypePrefix + d.objectType);
              lastObjectType = d.objectType;
          }
          if( ! lastObjectName.equals(d.objectName)) {
              differences.add(objectNamePrefix + d.objectName);
              lastObjectName = d.objectName;
          }

          differences.add(messagePrefix + d.message);
      }

      return differences;
    }

  }
}
