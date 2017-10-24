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
import java.util.List;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Table;

/**
 * Testing implementation of {@link DataSetConsumer}.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class MockDataSetConsumer implements DataSetConsumer {

  /**
   * Stores the meta data of the table we are currently receiving.
   */
  protected Table currentTable;

  /**
   * Store events as they are received.
   */
  protected final List<String> events = new ArrayList<>();

  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#open()
   */
  @Override
  public void open() {
    events.add("open");
  }

  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#close(org.alfasoftware.morf.dataset.DataSetConsumer.CloseState)
   */
  @Override
  public void close(CloseState closeState) {
    events.add("close");
  }

  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#table(org.alfasoftware.morf.metadata.Table, java.lang.Iterable)
   */
  @Override
  public void table(Table table, Iterable<Record> records) {
    currentTable = table;
    List<String> columnNames = new ArrayList<>();
    for (Column column : currentTable.columns()) {
      columnNames.add("column " + column.getName());
    }
    events.add("table " + currentTable.getName() + " " + columnNames.toString());

    for (Record record : records) {
      List<String> recordValues = new ArrayList<>();
      for (Column column : currentTable.columns()) {
        recordValues.add(record.getString(column.getName()));
      }
      events.add(recordValues.toString());
    }

    events.add("end table");
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return events.toString();
  }
}
