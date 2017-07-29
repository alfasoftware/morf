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

package org.alfasoftware.morf.dataset;

import static org.alfasoftware.morf.metadata.SchemaUtils.primaryKeysForTable;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.lang.ObjectUtils;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.RecordBean;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.Table;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Ordering;

/**
 * Measures the differences in a single table between {@link DataSetProducer}s.
 *
 * <p>Note that this class assumes the schemas to be identical. This can be checked beforehand using
 * {@link SchemaHomology}.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TableDataHomology {

  // This is HORRID! But we have no way of cleverly filtering them out
  // because we don't have the domain class so don't have any annotations.
  private static final ImmutableSet<String> DEFAULT_COLUMNS_TO_EXCLUDE = ImmutableSet.of("ID", "VERSION");

  private final List<String> differences = new LinkedList<>();
  private final Comparator<Record> orderComparator;
  private final Collection<String> columnsToExclude;


  /**
   * Create with no ordering comparators and assuming that id and version columns will be
   * excluded from the comparison.
   */
  public TableDataHomology() {
    this(Optional.<Comparator<Record>>absent(), Optional.<Collection<String>>absent());
  }


  /**
   * Create with ordering comparators and assuming that id and version columns will be
   * excluded from the comparison.
   *
   * @param orderComparator The comparator to use for ordering the rows, before their are checked for equality.
   */
  public TableDataHomology(Comparator<Record> orderComparator) {
    this(Optional.of(orderComparator), Optional.<Collection<String>>absent());
  }


  /**
   * Full constructor.
   *
   * @param orderComparator The comparator to use for ordering the rows, before their are checked for equality.
   * @param columnsToExclude The column names which will not be subject to comparison.  If absent we will assume that
   *                         we should exclude id and version.
   */
  public TableDataHomology(Optional<Comparator<Record>> orderComparator, Optional<Collection<String>> columnsToExclude) {
    super();
    this.orderComparator = orderComparator.orNull();
    this.columnsToExclude = columnsToExclude.isPresent() ? Collections2.transform(columnsToExclude.get(), new Function<String, String>() {
      @Override
      public String apply(String input) {
        return input.toUpperCase();
      }
    }) : DEFAULT_COLUMNS_TO_EXCLUDE;
  }


  /**
   * @return The list of differences detected by the comparison.
   */
  public List<String> getDifferences() {
    return differences;
  }


  /**
   * Compare all the records for this table.
   *
   * @param table the active {@link Table}
   * @param records1 the first set of records
   * @param records2 the second set of records
   */
  public void compareTable(final Table table, Iterable<Record> records1, Iterable<Record> records2) {

    Iterator<Record> iterator1;
    Iterator<Record> iterator2;

    if (orderComparator == null) {
      // no comparator - just compare the results in the order they arrive.
      iterator1 = records1.iterator();
      iterator2 = records2.iterator();
    } else {
      // There is a comparator. Sort the rows before comparison
      iterator1 = copyAndSort(table, records1, orderComparator).iterator();
      iterator2 = copyAndSort(table, records2, orderComparator).iterator();
    }

    int recordNumber = 0;

    List<Column> primaryKeys = primaryKeysForTable(table);
    List<Column> primaryKeysForComparison = FluentIterable.from(primaryKeys).filter(excludingExcludedColumns()).toList();

    Optional<Record> next1 = optionalNext(iterator1);
    Optional<Record> next2 = optionalNext(iterator2);
    while (moreRecords(table, next1, next2, primaryKeys)) {
      int compareResult = primaryKeysForComparison.isEmpty() ? 0 : compareKeys(next1, next2, primaryKeysForComparison);
      if (compareResult > 0) {
        differences.add(String.format("Table [%s]: Dataset1 is missing %s (Dataset2=%s)", table.getName(), keyColumnsIds(next2.get(), primaryKeysForComparison), RecordHelper.joinRecordValues(table, next2.get())));
        next2 = optionalNext(iterator2);
      } else if (compareResult < 0) {
        differences.add(String.format("Table [%s]: Dataset2 is missing %s (Dataset1=%s)", table.getName(), keyColumnsIds(next1.get(), primaryKeysForComparison), RecordHelper.joinRecordValues(table, next1.get())));
        next1 = optionalNext(iterator1);
      } else {
        compareRecords(table, recordNumber++, next1.get(), next2.get(), primaryKeys);
        next1 = optionalNext(iterator1);
        next2 = optionalNext(iterator2);
      }
    }
  }


  private Predicate<Column> excludingExcludedColumns() {
    return new Predicate<Column>() {
      @Override
      public boolean apply(Column input) {
        return !columnsToExclude.contains(input.getName().toUpperCase());
      }
    };
  }


  private boolean moreRecords(Table table, Optional<Record> next1, Optional<Record> next2, List<Column> primaryKeys) {
    if (primaryKeys.isEmpty()) {
      if (next1.isPresent() && next2.isPresent()) {
        return true;
      } else {
        if (next1.isPresent() != next2.isPresent()) {
          differences.add(String.format("Table [%s]: Dataset1 has more records: %s Dataset2 has more records: %s",
            table.getName(), Boolean.toString(next2.isPresent()), Boolean.toString(next1.isPresent())));
        }
        return false;
      }
    } else {
      return next1.isPresent() || next2.isPresent();
    }
  }


  private Optional<Record> optionalNext(Iterator<Record> iterator) {
    return iterator.hasNext() ? Optional.of(iterator.next()) : Optional.<Record>absent();
  }


  private int compareKeys(Optional<Record> record1, Optional<Record> record2, List<Column> primaryKeys) {
    if (!record1.isPresent() && !record2.isPresent()) {
      throw new IllegalStateException("Cannot compare two nonexistent records.");
    }
    if (!record1.isPresent()) {
      return 1;
    }
    if (!record2.isPresent()) {
      return -1;
    }
    for (Column keyCol : primaryKeys) {
      Comparable<?> value1 = convertToComparableType(keyCol, record1.get().getValue(keyCol.getName()));
      Comparable<?> value2 = convertToComparableType(keyCol, record2.get().getValue(keyCol.getName()));
      int result = ObjectUtils.compare(value1, value2);
      if (result != 0) {
        return result;
      }
    }
    return 0;
  }


  /**
   * @return A sorted copy of the Record list
   */
  private List<Record> copyAndSort(final Table table, Iterable<Record> records, Comparator<Record> comparator) {
    // we need to transform the records to RecordBeans so we don't re-use the Record
    List<Record> unsortedCopy = ImmutableList.copyOf(
      Iterables.transform(records, new com.google.common.base.Function<Record, Record>() {
        @Override
        public Record apply(Record input) {
          return new RecordBean(table, input);
        }
      })
    );

    // now sort the list, and return an iterator over it
    return Ordering.from(comparator).sortedCopy(unsortedCopy);
  }


  /**
   * Compare all the columns for these records.
   *
   * @param table the active {@link Table}
   * @param recordNumber The current record number
   * @param record1 The first record
   * @param record2 The second record
   */
  private void compareRecords(Table table, int recordNumber, Record record1, Record record2, List<Column> primaryKeys) {

    for (Column column : FluentIterable.from(table.columns()).filter(excludingExcludedColumns())) {
      String columnName = column.getName();

      Object value1 = convertToComparableType(column, record1.getValue(columnName));
      Object value2 = convertToComparableType(column, record2.getValue(columnName));

      if (!ObjectUtils.equals(value1, value2)) {
        differences.add(String.format(
          "Table [%s]: Mismatch on key %s column [%s] row [%d]: [%s]<>[%s]",
          table.getName(),
          keyColumnsIds(record1, record2, primaryKeys),
          columnName,
          recordNumber,
          value1,
          value2
        ));
      }
    }
  }


  private String keyColumnsIds(Record record, List<Column> primaryKeys) {
    StringBuilder builder = new StringBuilder();
    for (Column keyCol : primaryKeys) {
      builder.append(keyCol.getName() + "[" + record.getValue(keyCol.getName()) + "]");
    }
    return builder.toString();
  }


  private String keyColumnsIds(Record record1, Record record2, List<Column> primaryKeys) {
    StringBuilder builder = new StringBuilder();
    for (Column keyCol : primaryKeys) {
      builder.append(keyCol.getName() + "=[" + record1.getValue(keyCol.getName()) + "][" + record2.getValue(keyCol.getName()) + "]");
    }
    return builder.toString();
  }


  private Comparable<?> convertToComparableType(Column column, String value) {
    try {
      return RecordHelper.convertToComparableType(column, value);
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Failed converting [" + value + "] to comparable type " + column); // NOPMD Deliberately squashing the exception - we know what it means and we prefer ours.
    }
  }
}
