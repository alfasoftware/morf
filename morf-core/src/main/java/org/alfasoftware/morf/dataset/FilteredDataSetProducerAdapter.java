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

import java.util.Collection;
import java.util.Set;

import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.FluentIterable;

/**
 * {@link DataSetProducer} implementation which filters out files other than those specified.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class FilteredDataSetProducerAdapter extends DataSetProducerAdapter {

  private final Predicate<String> includingPredicate;

  /**
   * Create a filtered adapter, passing in a predicate of tables to include.
   * @param producer The wrapped {@link DataSetProducer}.
   * @param includingPredicate The predicate with which to include tables. All values to check will be passed to the predicate in UpperCase.
   * The predicate should return true if the table is to be included.
   */
  public FilteredDataSetProducerAdapter(DataSetProducer producer, Predicate<String> includingPredicate) {
    super(producer);
    this.includingPredicate = includingPredicate;
  }


  /**
   * Create a filtered adapter, passing in the list of tables to include.
   * @param producer The wrapped {@link DataSetProducer}.
   * @param includedTables The tables to include.
   */
  public FilteredDataSetProducerAdapter(DataSetProducer producer, Collection<String> includedTables) {
    super(producer);
    final Set<String> includedSet = FluentIterable.from(includedTables)
                                                  .transform(new Function<String, String>() {
                                                     @Override
                                                     public String apply(String str) {
                                                       return str.toUpperCase();
                                                     }
                                                   })
                                                  .toSet();

    this.includingPredicate = new Predicate<String>() {
      @Override
      public boolean apply(String input) {
        return includedSet.contains(input);
      }
    };
  }


  private boolean includeTable(String table) {
    return includingPredicate.apply(table.toUpperCase());
  }


  /**
   * Produce a {@link Schema} that represents the filtered view.
   * @see org.alfasoftware.morf.dataset.DataSetProducer#getSchema()
   */
  @Override
  public Schema getSchema() {
    return new SchemaAdapter(delegate.getSchema()) {

      @Override
      public Table getTable(String name) {
        if (includeTable(name)) {
          return delegate.getTable(name);
        }
        throw new IllegalStateException("["+name+"] has been excluded or does not exist");
      }


      @Override
      public boolean tableExists(String name) {
        if (includeTable(name)) {
          return delegate.tableExists(name);
        }
        throw new IllegalStateException("["+name+"] has been exlcuded or does not exist");
      }


      /**
       * If multiple calls to this are expected, consider caching the list of table names.
       * @see org.alfasoftware.morf.dataset.SchemaAdapter#tableNames()
       */
      @Override
      public Collection<String> tableNames() {
        return Collections2.filter(delegate.tableNames(), new Predicate<String>() {
          @Override
          public boolean apply(String table) {
            return includeTable(table);
          }
        });
      }


      /**
       * If multiple calls to this are expected, consider caching the list of tables.
       * @see org.alfasoftware.morf.dataset.SchemaAdapter#tableNames()
       */
      @Override
      public Collection<Table> tables() {
        return Collections2.filter(delegate.tables(), new Predicate<Table>() {
          @Override
          public boolean apply(Table table) {
            return includeTable(table.getName());
          }
        });
      }
    };
  }
}
