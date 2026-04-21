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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.List;

import org.alfasoftware.morf.dataset.Record;
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.Test;

import com.google.common.collect.Lists;


/**
 * Ensure that the {@link SpreadsheetDataSetProducer} works correctly.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class TestSpreadsheetDataSetProducer {

  /**
   * Ensure that the schema can be retrieved from a set of excel files
   */
  @Test
  public void testGetSchema() {
    final SpreadsheetDataSetProducer producer = produceTestSpreadsheet();

    // Check that the table names were picked up correctly
    Collection<String> tableNames = producer.getSchema().tableNames();
    assertEquals("Number of tables found [" + tableNames + "]", 12, tableNames.size());
    assertTrue("Tables correctly populated [" + tableNames + "]", tableNames.contains("AssetType"));
    assertTrue("Tables correctly populated [" + tableNames + "]", tableNames.contains("Allowance"));
  }


  /**
   * Ensure that the rows can be retrieved from a set of excel files
   */
  @Test
  public void testGetRecords() {
    final SpreadsheetDataSetProducer producer = produceTestSpreadsheet();

    // Check that the table names were picked up correctly
    List<Record> records = Lists.newArrayList(producer.records("UsageMeterType"));
    assertEquals("Number of rows [" + records + "]", 2, records.size());

    Record record = records.get(0);
    assertEquals("Usage Meter", "KM", record.getString("usageMeter"));
    assertEquals("Description", "Kilometers", record.getString("description"));
    assertEquals("Usage Meter Type", "M", record.getString("usageMeterType"));

    record = records.get(1);
    assertEquals("Usage Meter", "HOUR", record.getString("usageMeter"));
    assertEquals("Description", "Hours", record.getString("description"));
    assertEquals("Usage Meter Type", "", record.getString("usageMeterType"));
  }


  /**
   * Ensure that the generated schema behaves as expected
   */
  @Test
  public void testGetSchemaMethods() {
    final SpreadsheetDataSetProducer producer = produceTestSpreadsheet();

    try {
      // Getting the metadata of a table is an unsupported method of the
      // minimal schema
      producer.getSchema().getTable("Name");
      fail("Exception should have been thrown");
    } catch (UnsupportedOperationException e) {
      // Expected
    }

    try {
      // Getting all tables is an unsupported method of the minimal schema
      producer.getSchema().tables();
      fail("Exception should have been thrown");
    } catch (UnsupportedOperationException e) {
      // Expected
    }

    assertFalse("Schema is not empty", producer.getSchema().isEmptyDatabase());
    assertTrue("UsageMeterType table exists", producer.getSchema().tableExists("UsageMeterType"));
  }


  @Test
  public void testNullInputStreamRejected() {
    try {
      new SpreadsheetDataSetProducer((InputStream) null);
      fail("Exception should have been thrown");
    } catch (IllegalArgumentException e) {
      assertEquals("Spreadsheet input stream was null", e.getMessage());
    }
  }


  @Test
  public void testUnquotedHyperlinkAddressIsParsedAndBlankRowStopsRecords() throws Exception {
    SpreadsheetDataSetProducer producer = new SpreadsheetDataSetProducer(new ByteArrayInputStream(
      workbookBytes(workbookWithSingleTable("UsageMeterType", "UsageMeterType!A1", true, true, true, false, false))));

    List<Record> records = Lists.newArrayList(producer.records("UsageMeterType"));
    assertEquals("One populated data row should be parsed", 1, records.size());
    assertEquals("KM", records.get(0).getString("usageMeter"));
    assertEquals("Kilometers", records.get(0).getString("description"));
    assertEquals("1", records.get(0).getString("id"));
    assertEquals("1", records.get(0).getString("translationId"));
  }


  @Test
  public void testMissingHyperlinkInIndexIsWrapped() throws Exception {
    try {
      new SpreadsheetDataSetProducer(new ByteArrayInputStream(
        workbookBytes(workbookWithSingleTable("UsageMeterType", null, true, false, false, false, false))));
      fail("Exception should have been thrown");
    } catch (RuntimeException e) {
      assertEquals("Failed to parse spreadsheet", e.getMessage());
      assertTrue("Root cause should mention the missing hyperlink",
        findMessage(e, "Failed to find hyperlink in index sheet at [0,2]"));
    }
  }


  @Test
  public void testMissingWorksheetIsWrapped() throws Exception {
    try {
      new SpreadsheetDataSetProducer(new ByteArrayInputStream(
        workbookBytes(workbookWithSingleTable("UsageMeterType", "'MissingSheet'!A1", false, true, false, false, false))));
      fail("Exception should have been thrown");
    } catch (RuntimeException e) {
      assertEquals("Failed to parse spreadsheet", e.getMessage());
      assertTrue("Root cause should mention the missing worksheet",
        findMessage(e, "Failed to find worksheet with name [MissingSheet]"));
    }
  }


  @Test
  public void testMissingHeaderRowIsWrapped() throws Exception {
    try {
      new SpreadsheetDataSetProducer(new ByteArrayInputStream(
        workbookBytes(workbookWithSingleTable("UsageMeterType", "'UsageMeterType'!A1", true, true, false, true, false))));
      fail("Exception should have been thrown");
    } catch (RuntimeException e) {
      assertEquals("Failed to parse spreadsheet", e.getMessage());
      assertTrue("Root cause should mention the missing header row",
        findMessage(e, "Could not find header row in worksheet [UsageMeterType]"));
    }
  }


  @Test
  public void testNoTranslationColumnStillProducesRecords() throws Exception {
    SpreadsheetDataSetProducer producer = new SpreadsheetDataSetProducer(new ByteArrayInputStream(
      workbookBytes(workbookWithSingleTable("UsageMeterType", "'UsageMeterType'!A1", true, true, false, false, true))));

    List<Record> records = Lists.newArrayList(producer.records("UsageMeterType"));
    assertEquals("One populated data row should be parsed", 1, records.size());
    assertEquals("KM", records.get(0).getString("usageMeter"));
    assertEquals("Kilometers", records.get(0).getString("description"));
  }


  private SpreadsheetDataSetProducer produceTestSpreadsheet() {
    InputStream testExcel = getClass().getResourceAsStream("TestSpreadsheetDataSetProducer.xls");
    return new SpreadsheetDataSetProducer(testExcel);
  }


  private Workbook workbookWithSingleTable(String tableName, String hyperlinkAddress, boolean createWorksheet,
      boolean indexHasHyperlink, boolean useTranslationColumn, boolean omitHeaderHyperlink, boolean noTranslationColumn) {
    Workbook workbook = new HSSFWorkbook();

    org.apache.poi.ss.usermodel.Sheet index = workbook.createSheet("Index");
    Row indexRow = index.createRow(2);
    if (indexHasHyperlink) {
      indexRow.createCell(0).setCellValue(tableName);
      indexRow.getCell(0).setHyperlink(workbook.getCreationHelper().createHyperlink(HyperlinkType.DOCUMENT));
      indexRow.getCell(0).getHyperlink().setAddress(hyperlinkAddress);
    }
    indexRow.createCell(1).setCellValue(tableName);

    if (createWorksheet) {
      org.apache.poi.ss.usermodel.Sheet tableSheet = workbook.createSheet(tableName);
      tableSheet.createRow(0).createCell(0).setCellValue(tableName);
      tableSheet.createRow(1).createCell(0).setCellValue("Parameters to Set Up");

      Row headerRow = tableSheet.createRow(2);
      if (!omitHeaderHyperlink) {
        headerRow.createCell(0).setHyperlink(workbook.getCreationHelper().createHyperlink(HyperlinkType.DOCUMENT));
        headerRow.getCell(0).getHyperlink().setAddress("'Index'!A1");
      } else {
        headerRow.createCell(0);
      }
      headerRow.getCell(0).setCellValue("Usage Meter");
      headerRow.createCell(1).setCellValue("Description");
      if (useTranslationColumn && !noTranslationColumn) {
        headerRow.createCell(2).setCellValue("");
        headerRow.createCell(3).setCellValue("Translation");
      }

      Row dataRow = tableSheet.createRow(3);
      dataRow.createCell(0).setCellValue("KM");
      dataRow.createCell(1).setCellValue("Kilometers");
      if (useTranslationColumn && !noTranslationColumn) {
        dataRow.createCell(3).setCellValue("M");
      }

      if (useTranslationColumn && !noTranslationColumn) {
        tableSheet.createRow(5);
      }
    }

    return workbook;
  }


  private byte[] workbookBytes(Workbook workbook) throws IOException {
    try (Workbook closeableWorkbook = workbook;
         ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
      closeableWorkbook.write(outputStream);
      return outputStream.toByteArray();
    }
  }


  private boolean findMessage(Throwable throwable, String expectedMessage) {
    Throwable current = throwable;
    while (current != null) {
      if (expectedMessage.equals(current.getMessage())) {
        return true;
      }
      current = current.getCause();
    }
    return false;
  }
}
