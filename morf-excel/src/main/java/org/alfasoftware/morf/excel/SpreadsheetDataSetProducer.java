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

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.DataSetUtils;
import org.alfasoftware.morf.metadata.DataSetUtils.RecordBuilder;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.apache.commons.lang3.StringUtils;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.usermodel.WorkbookFactory;

/**
 * Converts Excel spreadsheets into a dataset.
 */
public class SpreadsheetDataSetProducer implements DataSetProducer {

  private static final Pattern SHEET_NAME = Pattern.compile("'([^']*)'.*");

  private final Map<String, List<Record>> tables = new HashMap<>();
  private final List<Record> translations = new LinkedList<>();
  private final DataFormatter dataFormatter = new DataFormatter();

  public SpreadsheetDataSetProducer(final InputStream... excelFiles) {
    for (InputStream stream : excelFiles) {
      parseWorkbook(stream);
    }
  }

  private Record createTranslationRecord(final int id, final String translation) {
    final RecordBuilder record = DataSetUtils.record();
    record.setString("translationText", translation);
    final Date now = new Date();
    record.setString("changeDate", new SimpleDateFormat("yyyyMMdd").format(now));
    record.setString("changedTime", new SimpleDateFormat("hhmmss").format(now));
    record.setInteger("localeSequenceNumber", 1);
    record.setInteger("translationSequenceNumber", id);
    record.setInteger("translationId", id);
    record.setInteger("id", id);
    return record;
  }

  private void parseWorkbook(final InputStream inputStream) {
    if (inputStream == null) {
      throw new IllegalArgumentException("Spreadsheet input stream was null");
    }
    try (Workbook workbook = WorkbookFactory.create(inputStream)) {
      final Sheet sheet = workbook.getSheetAt(0);
      final int column = 1;
      for (int row = 2; row <= sheet.getLastRowNum(); row++) {
        String tableName = getCellContents(sheet, column, row);
        if (StringUtils.isEmpty(tableName)) {
          break;
        }

        String worksheetName = getDestinationWorksheet(sheet, column - 1, row);
        Sheet worksheet = workbook.getSheet(worksheetName);
        if (worksheet == null) {
          throw new IllegalStateException("Failed to find worksheet with name [" + worksheetName + "]");
        }
        this.tables.put(tableName, getRecords(worksheet));
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse spreadsheet", e);
    }
  }

  private String getDestinationWorksheet(Sheet sheet, int column, int row) {
    Cell cell = getCell(sheet, column, row);
    if (cell == null || cell.getHyperlink() == null || cell.getHyperlink().getAddress() == null) {
      throw new IllegalStateException("Failed to find hyperlink in index sheet at [" + column + "," + row + "]");
    }

    String address = cell.getHyperlink().getAddress();
    Matcher matcher = SHEET_NAME.matcher(address);
    if (matcher.matches()) {
      return matcher.group(1);
    }
    int separator = address.indexOf('!');
    return separator >= 0 ? address.substring(0, separator) : address;
  }

  private int findHeaderRow(final Sheet sheet) {
    int row = 0;
    for (; row <= sheet.getLastRowNum(); row++) {
      if ("Parameters to Set Up".equalsIgnoreCase(getCellContents(sheet, 0, row))) {
        row++;
        break;
      }
    }

    for (; row <= sheet.getLastRowNum(); row++) {
      Cell cell = getCell(sheet, 0, row);
      if (cell != null && cell.getHyperlink() != null) {
        return row;
      }
    }

    throw new IllegalStateException("Could not find header row in worksheet [" + sheet.getSheetName() + "]");
  }

  private int getTranslationsColumnIndex(Sheet sheet, int headingRow) {
    boolean hasBlank = false;
    int width = getRowWidth(sheet, headingRow);
    int i = 0;
    for (; i < width; i++) {
      if (getCellContents(sheet, i, headingRow).isEmpty()) {
        hasBlank = true;
        break;
      }
    }
    if (!hasBlank) {
      return -1;
    }
    for (; i < width; i++) {
      if (!getCellContents(sheet, i, headingRow).isEmpty()) {
        return i;
      }
    }
    return -1;
  }

  private int countHeadings(final Sheet sheet, final int headingRowIndex) {
    int width = getRowWidth(sheet, headingRowIndex);
    for (int i = 0; i < width; i++) {
      if (getCellContents(sheet, i, headingRowIndex).isEmpty()) {
        return i;
      }
    }
    return width;
  }

  private List<Record> getRecords(Sheet sheet) {
    try {
      long id = 1;
      int row = findHeaderRow(sheet);
      final Map<String, Integer> columnHeadingsMap = new HashMap<>();
      for (int i = 0; i < countHeadings(sheet, row); i++) {
        columnHeadingsMap.put(columnName(getCellContents(sheet, i, row)), i);
      }

      final int translationColumn = getTranslationsColumnIndex(sheet, row);
      row++;
      List<Record> records = new LinkedList<>();
      for (; row <= sheet.getLastRowNum(); row++) {
        if (allBlank(sheet, row)) {
          break;
        }
        records.add(createRecord(id++, columnHeadingsMap, translationColumn, sheet, row));
      }
      return records;
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse worksheet [" + sheet.getSheetName() + "]", e);
    }
  }

  private boolean allBlank(final Sheet sheet, int rowIndex) {
    Row row = sheet.getRow(rowIndex);
    if (row == null) {
      return true;
    }
    for (int i = row.getFirstCellNum() < 0 ? 0 : row.getFirstCellNum(); i < row.getLastCellNum(); i++) {
      if (!getCellContents(sheet, i, rowIndex).isEmpty()) {
        return false;
      }
    }
    return true;
  }

  private Record createRecord(final long id, final Map<String, Integer> columnHeadingsMap, final int translationColumn,
      final Sheet sheet, final int rowIndex) {
    final int translationId;
    String translationValue = translationColumn == -1 ? "" : getCellContents(sheet, translationColumn, rowIndex);
    if (translationColumn != -1 && !translationValue.isEmpty()) {
      translationId = translations.size() + 1;
      translations.add(createTranslationRecord(translationId, translationValue));
    } else {
      translationId = 0;
    }

    final RecordBuilder record = DataSetUtils.record();
    for (Entry<String, Integer> column : columnHeadingsMap.entrySet()) {
      record.setString(column.getKey(), getCellContents(sheet, column.getValue(), rowIndex));
    }
    record.setLong("id", id);
    record.setInteger("translationId", translationId);
    return record;
  }

  private String columnName(final String longName) {
    final String noSpaces = longName.replaceAll(" ", "");
    return noSpaces.substring(0, 1).toLowerCase() + noSpaces.substring(1);
  }

  @Override
  public Schema getSchema() {
    return new Schema() {
      @Override public Table getTable(String name) { throw new UnsupportedOperationException("Cannot get the metadata of a table for a spreadsheet"); }
      @Override public boolean isEmptyDatabase() { return tables.isEmpty(); }
      @Override public boolean tableExists(String name) { return tables.containsKey(name); }
      @Override public Collection<String> tableNames() { return tables.keySet(); }
      @Override public Collection<Table> tables() { throw new UnsupportedOperationException("Cannot get the metadata of a table for a spreadsheet"); }
      @Override public boolean viewExists(String name) { return false; }
      @Override public View getView(String name) { throw new IllegalArgumentException("Invalid view [" + name + "]. Views are not supported in spreadsheets"); }
      @Override public Collection<String> viewNames() { return Collections.emptySet(); }
      @Override public Collection<View> views() { return Collections.emptySet(); }
      @Override public boolean sequenceExists(String name) { return false; }
      @Override public Sequence getSequence(String name) { throw new IllegalArgumentException("Invalid sequence [" + name + "]. Sequences are not supported in spreadsheets"); }
      @Override public Collection<String> sequenceNames() { return Collections.emptySet(); }
      @Override public Collection<Sequence> sequences() { return Collections.emptySet(); }
    };
  }

  /**
   * No-op.
   *
   * <p>This producer is eagerly initialised: the spreadsheet is fully parsed
   * during construction. There are therefore no resources to open.</p>
   */
  @Override public void open() {
    // Intentionally empty
  }


  /**
   * No-op.
   *
   * <p>This producer does not hold any open resources after construction,
   * so there is nothing to close.</p>
   */
  @Override public void close() {
    // Intentionally empty
  }
  @Override public Iterable<Record> records(String tableName) { return tables.get(tableName); }
  @Override public boolean isTableEmpty(String tableName) { return tables.get(tableName).isEmpty(); }

  private int getRowWidth(Sheet sheet, int rowIndex) {
    Row row = sheet.getRow(rowIndex);
    return row == null || row.getLastCellNum() < 0 ? 0 : row.getLastCellNum();
  }

  private Cell getCell(Sheet sheet, int columnIndex, int rowIndex) {
    Row row = sheet.getRow(rowIndex);
    return row == null ? null : row.getCell(columnIndex);
  }

  private String getCellContents(Sheet sheet, int columnIndex, int rowIndex) {
    Cell cell = getCell(sheet, columnIndex, rowIndex);
    return cell == null ? "" : dataFormatter.formatCellValue(cell);
  }
}
