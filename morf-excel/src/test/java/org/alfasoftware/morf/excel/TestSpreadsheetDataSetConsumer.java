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

package org.alfasoftware.morf.excel;

import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.OutputStream;
import java.util.Map;
import java.util.Optional;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Table;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

public class TestSpreadsheetDataSetConsumer {

  private static final ImmutableList<Record> NO_RECORDS = ImmutableList.<Record>of();

  @Test
  public void testIgnoreTable() {
    final MockTableOutputter outputter = new MockTableOutputter();
    final SpreadsheetDataSetConsumer consumer = new SpreadsheetDataSetConsumer(
        mock(OutputStream.class),
        Optional.<Map<String, Integer>>of(ImmutableMap.of("COMPANY", 5)),
        outputter);

    consumer.table(table("NotCompany"), NO_RECORDS);
    assertNull("Table not passed through", outputter.tableReceived);
  }

  @Test
  public void testUnsupportedColumns() {
    TableOutputter outputter = mock(TableOutputter.class);
    final SpreadsheetDataSetConsumer consumer = new SpreadsheetDataSetConsumer(
        mock(OutputStream.class),
        Optional.<Map<String, Integer>>empty(),
        outputter);

    Table one = table("one");
    Table two = table("two");
    when(outputter.tableHasUnsupportedColumns(one)).thenReturn(true);
    when(outputter.tableHasUnsupportedColumns(two)).thenReturn(false);

    consumer.table(one, NO_RECORDS);
    consumer.table(two, NO_RECORDS);

    verify(outputter).table(nullable(Integer.class), nullable(Workbook.class), eq(two), eq(NO_RECORDS));
    verify(outputter, times(0)).table(nullable(Integer.class), nullable(Workbook.class), eq(one), eq(NO_RECORDS));
  }

  @Test
  public void testIncludeTableWithSpecificRowCount() {
    final MockTableOutputter outputter = new MockTableOutputter();
    SpreadsheetDataSetConsumer consumer = new SpreadsheetDataSetConsumer(
        mock(OutputStream.class),
        Optional.<Map<String, Integer>>of(ImmutableMap.of("COMPANY", 5)),
        outputter);

    consumer.table(table("Company"), NO_RECORDS);

    assertEquals("Table passed through for output", "Company", outputter.tableReceived);
    assertEquals("Number of rows desired", Integer.valueOf(5), outputter.rowCountReceived);
  }

  private static class MockTableOutputter extends TableOutputter {
    private String tableReceived;
    private Number rowCountReceived;

    MockTableOutputter() {
      super(new DefaultAdditionalSchemaDataImpl());
    }

    @Override
    public void table(int maxRows, Workbook workbook, Table table, Iterable<Record> records) {
      tableReceived = table.getName();
      rowCountReceived = maxRows;
    }
  }
}
