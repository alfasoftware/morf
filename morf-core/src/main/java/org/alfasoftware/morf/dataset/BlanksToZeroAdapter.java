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

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;

/**
 * Adds handling to numbers such that any blank value in a numeric column will
 * be converted to zero.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class BlanksToZeroAdapter extends DataSetProducerAdapter {

  /**
   * Creates the adapter using the given producer as a data source.
   *
   * @param producer producer to get data from
   */
  public BlanksToZeroAdapter(DataSetProducer producer) {
    super(producer);
  }

  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.dataset.DataSetProducer#records(java.lang.String)
   */
  @Override
  public Iterable<Record> records(String tableName) {
    final Table table = delegate.getSchema().getTable(tableName);
    final List<Record> transformedRecords = new LinkedList<Record>();
    final Map<String, Column> columns = new HashMap<String, Column>();

    for (Column column : table.columns()) {
      columns.put(column.getName(), column);
    }

    for (final Record record : delegate.records(tableName)) {
      /*
       * Transform the record such that whenever a decimal column's value is
       * blank it is turned into a 0.
       */
      Record transformedRecord = new Record() {


        /**
         * {@inheritDoc}
         *
         * @see org.alfasoftware.morf.dataset.Record#getValue(java.lang.String)
         */
        @Override
        public String getValue(String name) {
          String value = record.getValue(name);
          if (value == null || value.length() == 0) {
            if (columns.get(name).getType().equals(DataType.DECIMAL) || columns.get(name).getType().equals(DataType.BIG_INTEGER) || columns.get(name).getType().equals(DataType.INTEGER)) {
              value = "0";
            } else if (columns.get(name).getType().equals(DataType.STRING) && columns.get(name).getWidth() == 1) {
              value = " ";
            }else if (columns.get(name).getType().equals(DataType.STRING)) {
              value = "";
            }
          }
          return value;
        }
      };
      transformedRecords.add(transformedRecord);
    }

    return transformedRecords;
  }
}
