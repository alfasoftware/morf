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
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.SchemaHomology.CollectingDifferenceWriter;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.upgrade.adapt.AlteredTable;
import org.alfasoftware.morf.upgrade.adapt.TableOverrideSchema;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

/**
 * {@link SchemaChange} which consists of changing an existing column
 * within a schema.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class ChangeColumn implements SchemaChange {

  /** The table to change **/
  private final String tableName;

  /** The start definition for the column **/
  private final Column fromColumn;

  /** The end definition for the column **/
  private final Column toColumn;

  /**
   * A map of the supported data type transitions.
   *
   * <p>Each of the transitions here MUST be supported by a test in TestDatabaseUpgradeIntegration.</p>
   *
   * Transitions do not have to be reversible.
   */
  private final Multimap<DataType,DataType> allowedDataTypeChanges = ImmutableMultimap.<DataType,DataType>builder()
    .put(DataType.DECIMAL, DataType.BIG_INTEGER) // Tested by testChangeColumnDataTypeAndChangeToPrimaryKey
    .build();

  /**
   * @param tableName the name of the table to change
   * @param fromColumn the column definition to change from
   * @param toColumn the column definition to change to
   */
  public ChangeColumn(String tableName, Column fromColumn, Column toColumn) {
    this.tableName = tableName;
    this.fromColumn = fromColumn;
    this.toColumn = toColumn;

    if (fromColumn == null) {
      throw new IllegalStateException(String.format("Cannot change a null column to have a new definition [%s]", toColumn.getName()));
    }

    if (toColumn == null) {
      throw new IllegalStateException(String.format("Cannot change column [%s] to be null", fromColumn.getName()));
    }

    verifyDataTypeChanges();
    verifyWidthAndScaleChanges();
  }


  /**
   * Changes a column definition from the start point to the end point.
   *
   * @param schema {@link Schema} to apply the change against resulting in new metadata.
   * @param columnStartPoint the start definition for the column
   * @param columnEndPoint the end definition for the column
   * @return MetaData with {@link SchemaChange} applied.
   */
  private Schema applyChange(Schema schema, Column columnStartPoint, Column columnEndPoint) {

    // Now setup the new table definition
    Table original = schema.getTable(tableName);

    boolean foundMatch = false;

    // Copy the columns names into a list of strings for column sort order
    List<String> columns = new ArrayList<String>();
    Set<String> processedColumns = new HashSet<String>();
    for (Column column : original.columns()) {
      String currentColumnName = column.getName();

      // If we're looking at the column being changed...
      if (currentColumnName.equalsIgnoreCase(columnStartPoint.getName())) {

        // check the current column matches the specified source column
        CollectingDifferenceWriter differences = new SchemaHomology.CollectingDifferenceWriter();
        if (!new SchemaHomology(differences, "trial schema", "source column").columnsMatch(columnStartPoint, column)) {
          throw new IllegalArgumentException("Cannot change column [" + currentColumnName + "] on table [" + tableName + "], the 'from' column definition does not match: " + differences.differences());
        }

        // Substitute in the new column
        currentColumnName = columnEndPoint.getName();
        foundMatch = true;
      }

      if (!processedColumns.add(currentColumnName.toUpperCase())) {
        throw new IllegalArgumentException(String.format("Cannot change column name from [%s] to [%s] on table [%s] as column with that name already exists", columnStartPoint.getName(), columnEndPoint.getName(), tableName));
      }

      columns.add(currentColumnName);
    }

    if (!foundMatch) {
      throw new IllegalArgumentException(String.format("Cannot change column [%s] as it does not exist on table [%s]", columnStartPoint.getName(), tableName));
    }

    // If the column is being renamed, check it isn't contained in an index
    if (!columnStartPoint.getName().equalsIgnoreCase(columnEndPoint.getName())) {
      for (Index index : original.indexes()) {
        for (String indexedColumnName : index.columnNames()) {
          if (indexedColumnName.equalsIgnoreCase(columnStartPoint.getName())) {
            throw new IllegalArgumentException(
                          String.format("Cannot rename column [%s] as it exists on index [%s] on table [%s]",
                                        columnStartPoint.getName(), index.getName(), tableName));
          }
        }
      }
    }

    return new TableOverrideSchema(schema, new AlteredTable(original, columns, Arrays.asList(new Column[] {columnEndPoint})));
  }


  /**
   * Verify That any data type transistions are allowed.
   */
  private void verifyDataTypeChanges() {
    // if there's no change, there's no problem
    if (Objects.equal(fromColumn.getType(), toColumn.getType())) {
      return;
    }

    // look up what target types we are allowed to change to
    Collection<DataType> allowableTargetTypes = allowedDataTypeChanges.get(fromColumn.getType());

    if (!allowableTargetTypes.contains(toColumn.getType())) {
      throw new IllegalArgumentException(String.format("Attempting to change the data type of [%s]. Changes from %s to %s are not supported.", fromColumn.getName(), fromColumn.getType(), toColumn.getType()));
    }
  }


  /**
   * Verify that any width and scale changes
   */
  private void verifyWidthAndScaleChanges() {

    // Reductions in width of Strings are permitted, although they will only work if the data fits in the narrower column
    if (Objects.equal(toColumn.getType(), fromColumn.getType()) && DataType.STRING.equals(fromColumn.getType())) {
      // don't do a check
      return;
    }

    boolean scaleCheck = toColumn.getType().hasScale() && fromColumn.getType().hasScale();
    boolean widthCheck = toColumn.getType().hasWidth() && fromColumn.getType().hasWidth();

    // Oracle does not permit reductions to precision or scale on numeric columns:
    // ORA-01440: column to be modified must be empty to decrease precision or scale

    if (widthCheck && toColumn.getWidth() < fromColumn.getWidth()) {
      throw new IllegalArgumentException(String.format(
        "Attempting to change the width of [%s] from %d to %d. Reductions in width/precision are not permitted.",
        fromColumn.getName(),
        fromColumn.getWidth(),
        toColumn.getWidth())
      );
    }

    if (scaleCheck && toColumn.getScale() < fromColumn.getScale()) {
      throw new IllegalArgumentException(String.format(
        "Attempting to change the scale of [%s] from %d to %d. Reductions in scale are not permitted.",
        fromColumn.getName(),
        fromColumn.getScale(),
        toColumn.getScale())
      );
    }

    int fromInts = fromColumn.getWidth() - fromColumn.getScale();
    int toInts = toColumn.getWidth() - toColumn.getScale();
    if (scaleCheck && toInts < fromInts) {
      throw new IllegalArgumentException(String.format(
        "Attempting to change precision-width of [%s] from %d to %d. Reductions of this are not permitted.",
        fromColumn.getName(),
        fromInts,
        toInts)
      );
    }
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#accept(org.alfasoftware.morf.upgrade.SchemaChangeVisitor)
   */
  @Override
  public void accept(SchemaChangeVisitor visitor) {
    visitor.visit(this);
  }

  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#apply(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema apply(Schema schema) {
    return applyChange(schema, fromColumn, toColumn);
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#isApplied(Schema, ConnectionResources)
   */
  @Override
  public boolean isApplied(Schema schema, ConnectionResources database) {
    if (!schema.tableExists(tableName)) {
      return false;
    }

    Table table = schema.getTable(tableName);
    SchemaHomology homology = new SchemaHomology();
    for (Column column : table.columns()) {
      if (homology.columnsMatch(column, toColumn)) {
        return true;
      }
    }
    return false;
  }


  /**
   * @see org.alfasoftware.morf.upgrade.SchemaChange#reverse(org.alfasoftware.morf.metadata.Schema)
   */
  @Override
  public Schema reverse(Schema schema) {
    return applyChange(schema, toColumn, fromColumn);
  }


  /**
   * Gets the name of the table to change.
   *
   * @return the name of the table to change
   */
  public String getTableName() {
    return tableName;
  }


  /**
   * Gets the column definition prior to the change
   *
   * @return the column definition prior to the change
   */
  public Column getFromColumn() {
    return fromColumn;
  }


  /**
   * Gets the column definition after the change
   *
   * @return the column definition after the change
   */
  public Column getToColumn() {
    return toColumn;
  }
}
