/* Copyright 2017 Alfa Financial Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.nullable;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Optional;

import org.alfasoftware.morf.dataset.DataSetConsumer.CloseState;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Table;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;
import org.apache.poi.ss.util.WorkbookUtil;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;


public class TestSpreadsheetDataSetConsumer {

  private static final ImmutableList<Record> NO_RECORDS = ImmutableList.of();


  @Test
  public void testIgnoreTable() {
    final MockTableOutputter outputter = new MockTableOutputter();
    final SpreadsheetDataSetConsumer consumer = new SpreadsheetDataSetConsumer(
      mock(OutputStream.class),
      Optional.of(ImmutableMap.of("COMPANY", 5)),
      outputter);

    consumer.table(table("NotCompany"), NO_RECORDS);

    assertNull("Table not passed through", outputter.tableReceived);
  }


  @Test
  public void testUnsupportedColumns() {
    TableOutputter outputter = mock(TableOutputter.class);
    final SpreadsheetDataSetConsumer consumer = new SpreadsheetDataSetConsumer(
      mock(OutputStream.class),
      Optional.empty(),
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
      Optional.of(ImmutableMap.of("COMPANY", 5)),
      outputter);

    consumer.table(table("Company"), NO_RECORDS);

    assertEquals("Table passed through for output", "Company", outputter.tableReceived);
    assertEquals("Number of rows desired", 5, outputter.rowCountReceived);
  }


  @Test
  public void testCloseCreatesIndexAndNavigationHyperlinks() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    SpreadsheetDataSetConsumer consumer = new SpreadsheetDataSetConsumer(
      outputStream,
      Optional.empty(),
      new WritingTableOutputter());

    consumer.open();
    consumer.table(table("Company"), NO_RECORDS);
    consumer.close(CloseState.COMPLETE);

    try (Workbook workbook = WorkbookFactory.create(new ByteArrayInputStream(outputStream.toByteArray()))) {
      assertEquals("Index sheet should be first", "Index", workbook.getSheetAt(0).getSheetName());
      assertEquals("Workbook should contain index and table sheet", 2, workbook.getNumberOfSheets());

      Row indexRow = workbook.getSheet("Index").getRow(2);
      assertEquals("Table name should be written to index", "Company", indexRow.getCell(0).getStringCellValue());
      assertNotNull("Index entry should have a hyperlink", indexRow.getCell(0).getHyperlink());
      assertEquals("Index hyperlink should point to the table sheet", "'Company'!A1", indexRow.getCell(0).getHyperlink().getAddress());
      assertEquals("Filename should be copied from the table sheet", "Company file", indexRow.getCell(1).getStringCellValue());

      Row backLinkRow = workbook.getSheet("Company").getRow(1);
      assertEquals("Back-link text should be added to the table sheet", "Back to index", backLinkRow.getCell(0).getStringCellValue());
      assertNotNull("Back-link should have a hyperlink", backLinkRow.getCell(0).getHyperlink());
      assertEquals("Back-link should point back to the index row", "'Index'!A3", backLinkRow.getCell(0).getHyperlink().getAddress());
    }
  }


  @Test
  public void testCloseWrapsIOException() {
    SpreadsheetDataSetConsumer consumer = new SpreadsheetDataSetConsumer(
      new OutputStream() {
        @Override
        public void write(int b) throws IOException {
          throw new IOException("boom");
        }
      },
      Optional.empty(),
      new WritingTableOutputter());

    consumer.open();

    try {
      consumer.close(CloseState.COMPLETE);
    } catch (RuntimeException e) {
      assertEquals("Error closing writable workbook", e.getMessage());
      assertNotNull("Cause should be preserved", e.getCause());
      assertEquals("boom", e.getCause().getMessage());
      return;
    }

    throw new AssertionError("Exception should have been thrown");
  }


  @Test
  public void testWritingTableOutputterUsesSafeSheetNames() throws Exception {
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
    SpreadsheetDataSetConsumer consumer = new SpreadsheetDataSetConsumer(
      outputStream,
      Optional.empty(),
      new WritingTableOutputter());

    consumer.open();
    consumer.table(table("Bad:/Name*[]?"), NO_RECORDS);
    consumer.close(CloseState.COMPLETE);

    try (Workbook workbook = WorkbookFactory.create(new ByteArrayInputStream(outputStream.toByteArray()))) {
      String safeSheetName = WorkbookUtil.createSafeSheetName("Bad:/Name*[]?");
      assertNotNull("Unsafe table name should be converted to a valid sheet name", workbook.getSheet(safeSheetName));
      assertTrue("Original unsafe sheet name should not exist", workbook.getSheet("Bad:/Name*[]?") == null);
    }
  }

  private static class MockTableOutputter extends TableOutputter {
    private String tableReceived;
    private Integer rowCountReceived;

    MockTableOutputter() {
      super(new DefaultAdditionalSchemaDataImpl());
    }

    @Override
    public void table(int maxRows, Workbook workbook, Table table, Iterable<Record> records) {
      tableReceived = table.getName();
      rowCountReceived = maxRows;
    }
  }


  private static final class WritingTableOutputter extends TableOutputter {

    WritingTableOutputter() {
      super(new DefaultAdditionalSchemaDataImpl());
    }

    @Override
    public void table(int maxRows, Workbook workbook, Table table, Iterable records) {
      org.apache.poi.ss.usermodel.Sheet sheet = workbook.createSheet(WorkbookUtil.createSafeSheetName(table.getName()));
      sheet.createRow(0).createCell(0).setCellValue(table.getName());
      sheet.createRow(1).createCell(1).setCellValue(table.getName() + " file");
    }
  }
}
