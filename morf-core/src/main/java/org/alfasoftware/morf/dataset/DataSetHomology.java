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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import com.google.common.base.Optional;

/**
 * Measures the differences between {@link DataSetProducer}s.
 *
 * <p>Note that this class assumes the schemas to be identical. This can be checked beforehand using
 * {@link SchemaHomology}.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class DataSetHomology {

  /**
   * Store up the differences
   */
  private final List<String> differences = new LinkedList<>();

  private final Map<String, Comparator<Record>> orderComparators = new HashMap<>();

  private final Optional<Collection<String>> columnsToExclude;

  /**
   * Create with no ordering comparators and assuming that any columns {@link TableDataHomology} chooses
   * to ignore by default will be ignored.
   */
  public DataSetHomology() {
    this(new HashMap<String, Comparator<Record>>());
  }


  /**
   * Create with ordering comparators and assuming that any columns {@link TableDataHomology} chooses
   * to ignore by default will be ignored.
   *
   * @param orderComparators The comparators to use for ordering the rows, before their are checked for equality.
   */
  public DataSetHomology(Map<String, Comparator<Record>> orderComparators) {
    this(orderComparators, Optional.<Collection<String>>absent());
  }


  /**
   * Full constructor, specifying columns to be excluded.
   *
   * @param orderComparators The comparators to use for ordering the rows, before their are checked for equality.
   * @param columnsToExclude The column names which will not be subject to comparison.
   */
  public DataSetHomology(Map<String, Comparator<Record>> orderComparators, Optional<Collection<String>> columnsToExclude) {
    super();
    // make sure the keys are all in upper case
    for(Map.Entry<String, Comparator<Record>> entry : orderComparators.entrySet()) {
      this.orderComparators.put(entry.getKey().toUpperCase(), entry.getValue());
    }
    this.columnsToExclude = columnsToExclude;
  }



  /**
   * Compare that the data in the two produces matches.
   *
   * The individual differences can be accessed by the {@link #getDifferences()} method.
   *
   * @param producer1 The first producer
   * @param producer2 The second producer
   * @return Whether the data sets are identical
   */
  @SuppressWarnings("unchecked")
  public boolean dataSetProducersMatch(DataSetProducer producer1, DataSetProducer producer2) {

    producer1.open();
    producer2.open();
    Schema schema1 = producer1.getSchema();
    Schema schema2 = producer2.getSchema();

    try {
      Collection<String> tables1 = convertToUppercase(schema1.tableNames());
      Collection<String> tables2 = convertToUppercase(schema2.tableNames());

      Collection<String> commonTables = CollectionUtils.intersection(tables1, tables2);

      // look for extra tables
      Collection<String> extraTablesIn1 = CollectionUtils.subtract(tables1, commonTables);
      Collection<String> extraTablesIn2 = CollectionUtils.subtract(tables2, commonTables);
      for (String table : extraTablesIn1) {
        differences.add(String.format("Extra table in 1: [%s]", table));
      }
      for (String table : extraTablesIn2) {
        differences.add(String.format("Extra table in 2: [%s]", table));
      }

      // only compare the tables that are common
      for(String tableName : commonTables) {
        TableDataHomology tableDataHomology = new TableDataHomology(Optional.fromNullable(orderComparators.get(tableName.toUpperCase())), columnsToExclude);
        tableDataHomology.compareTable(
          schema1.getTable(tableName),
          producer1.records(tableName),
          producer2.records(tableName)
        );
        differences.addAll(tableDataHomology.getDifferences());
      }

    } finally {
      producer1.close();
      producer2.close();
    }


    return differences.isEmpty();
  }


  /**
   * @return The list of differences detected by the comparison.
   */
  public List<String> getDifferences() {
    return differences;
  }


  /**
   * Converts each string from the source collection to uppercase in a new collection.
   *
   * @param source the source collection
   * @return a new collection containing only uppercase strings
   */
  private Collection<String> convertToUppercase(final Collection<String> source) {
    Collection<String> temp = new ArrayList<>();

    for(String table : source) {
      temp.add(table.toUpperCase());
    }
    return temp;
  }
}
