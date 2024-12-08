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

import jxl.Cell;
import jxl.Hyperlink;
import jxl.Sheet;
import jxl.Workbook;
import jxl.WorkbookSettings;
import jxl.read.biff.HyperlinkRecord;

/**
 * Converts Excel spreadsheets into a dataset.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class SpreadsheetDataSetProducer implements DataSetProducer {
  /**
   * Pattern for extracting sheet names from hyperlinks of the form:
   *   'Sheet name'!A1:A1
   */
  private static final Pattern sheetName = Pattern.compile("'([^']*)'.*");

  /**
   * Store of tables extracted from the given excel files and their records.
   */
  private final Map<String, List<Record>> tables = new HashMap<>();


  /**
   * List of translations that have been extracted from the set of Excel files.
   */
  private final List<Record> translations = new LinkedList<>();


  /**
   * Prepares the producer with a set of Excel files to produce data from.
   *
   * @param excelFiles the Excel files to harvest data from
   */
  public SpreadsheetDataSetProducer(final InputStream... excelFiles) {
    // Open each spreadsheet and parse it
    for (InputStream stream : excelFiles) {
      parseWorkbook(stream);
    }
  }


  /**
   * Creates the collection of translation records from a given set of
   * translations.
   *
   * @param id ID of the translation record
   * @param translation Translation string to create
   * @return the record representing the translation
   */
  private Record createTranslationRecord(final int id, final String translation) {
    final RecordBuilder record = DataSetUtils.record();
    record.setString("translationText", translation);
    final Date now = new Date();
    record.setString("changeDate", new SimpleDateFormat("yyyyMMdd").format(now));
    record.setString("changedTime", new SimpleDateFormat("hhmmss").format(now));
    record.setInteger("localeSequenceNumber", 1); // Assume locale 1 for translations on initial upload
    record.setInteger("translationSequenceNumber", id);
    record.setInteger("translationId", id);
    record.setInteger("id", id);
    return record;
  }


  /**
   * Gets the hyperlink that starts at the given column/row in the given sheet.
   *
   * @param sheet sheet to look for a hyperlink in
   * @param column column of the hyperlink
   * @param row row of the hyperlink
   * @return the hyperlink, if found. Otherwise, null
   */
  private HyperlinkRecord getHyperlink(Sheet sheet, int column, int row) {
    for (Hyperlink link : sheet.getHyperlinks()) {
      if (link.getColumn() == column && link.getRow() == row) {
        return (HyperlinkRecord)link;
      }
    }

    return null;
  }


  /**
   * Parse the workbook from an {@link InputStream}.
   *
   * @param inputStream InputStream from the spreadsheet
   */
  private void parseWorkbook(final InputStream inputStream) {
    Workbook workbook = null;
    try {
      final WorkbookSettings settings = new WorkbookSettings();
      settings.setEncoding("CP1252");
      workbook = Workbook.getWorkbook(inputStream, settings);

      /*
       * The first sheet in the workbook is the index sheet. It contains links
       * to sheets containing data as well as the table name to use.
       */
      final Sheet sheet = workbook.getSheet(0);
      final int column = 1;
      for (int row = 2; row < sheet.getRows(); row++) {
        final Cell cell = sheet.getCell(column, row);
        if (StringUtils.isEmpty(cell.getContents())) {
          break;
        }

        final HyperlinkRecord hyperlink = getHyperlink(sheet, cell.getColumn() - 1, cell.getRow());
        final String worksheetName = getDestinationWorksheet(hyperlink);
        if (workbook.getSheet(worksheetName) == null) {
          throw new IllegalStateException("Failed to find worksheet with name [" + worksheetName + "]");
        }
        final List<Record> records = getRecords(workbook.getSheet(worksheetName));
        this.tables.put(cell.getContents(), records);
      }
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse spreadsheet", e);
    } finally {
      if (workbook != null) {
        workbook.close();
      }
    }
  }


  /**
   * Gets the name of the destination worksheet for the given hyperlink.
   *
   * @param hyperlink Hyperlink to determine worksheet name for
   * @return the name of the worksheet that the hyperlink points to
   */
  private String getDestinationWorksheet(HyperlinkRecord hyperlink) {
    /*
     * Hyperlinks will be either to a specific cell or to a worksheet as a
     * whole. If the regular expression for the sheet name part of a link
     * doesn't match then the hyperlink must be to a worksheet as a whole.
     */
    final Matcher matcher = sheetName.matcher(hyperlink.getLocation());
    if (matcher.matches()) {
      return matcher.group(1);
    } else {
      return hyperlink.getLocation();
    }
  }


  /**
   * Finds the heading row in a given worksheet.
   *
   * <p>This works by assuming that the table data starts in the row
   * immediately following a row containing hyperlinks that is after
   * a row that starts with "Parameters to Set Up". E.g.</p>
   *
   * <pre>
   * Parameters to Setup |                  |
   * ----------------------------------------
   * Random comments     | More comments    |
   * ----------------------------------------
   * Column Heading 1    | Column Heading 2 | <-- These are hyperlinks
   * </pre>
   *
   * @param sheet worksheet to search for data
   * @return the index of the starting row for the data
   */
  private int findHeaderRow(final Sheet sheet) {
    int row = 0;

    // Find the start row
    for (; row < sheet.getRows(); row++) {
      if ("Parameters to Set Up".equalsIgnoreCase(sheet.getCell(0, row).getContents())) {
        // Skip this row
        row++;
        break;
      }
    }

    // The heading row contains hyperlinks so continue scanning down until a
    // hyperlink is found
    for (; row < sheet.getRows(); row++) {
      final HyperlinkRecord hyperlink = getHyperlink(sheet, 0, row);
      if (hyperlink != null) {
        return row;
      }
    }

    // Either the parameters to set up row wasn't found or no hyperlinks were found
    throw new IllegalStateException("Could not find header row in worksheet [" + sheet.getName() + "]");
  }


  /**
   * Determines if a worksheet contains something that looks like translations.
   * This is done by looking for a gap in the column headings followed by an
   * actual heading, e.g.
   *
   * <pre>
   * Heading 1 | Heading 2 |         | Translation
   * -------------------------------------------------
   * Value 1   | Value 2   |         | Bonjour
   * </pre>
   *
   * @param sheet sheet to look for translations in
   * @param headingRow the index of the heading row
   * @return the index of the translation column, -1 otherwise
   */
  private int getTranslationsColumnIndex(Sheet sheet, int headingRow) {
    boolean hasBlank = false;
    int i = 0;
    for (; i < sheet.getRow(headingRow).length; i++) {
      if (sheet.getCell(i, headingRow).getContents().length() == 0) {
        hasBlank = true;
        break;
      }
    }
    if (!hasBlank) {
      return -1;
    }
    for (; i < sheet.getRow(headingRow).length; i++) {
      if (sheet.getCell(i, headingRow).getContents().length() > 0) {
        return i;
      }
    }
    return -1;
  }


  /**
   * Counts the number of headings in the given sheet. This excludes any
   * heading related to translations.
   *
   * @param sheet Worksheet to count headings in
   * @param headingRowIndex Index of the heading row
   * @return the number of headings
   */
  private int countHeadings(final Sheet sheet, final int headingRowIndex) {
    for (int i = 0; i < sheet.getRow(headingRowIndex).length; i++) {
      // A blank heading is the start of additional headings such as the
      // translation heading
      if (sheet.getCell(i, headingRowIndex).getContents().length() == 0) {
        return i;
      }
    }
    return sheet.getRow(headingRowIndex).length;
  }


  /**
   * Get all the records from the given Excel sheet.
   *
   * @param sheet worksheet to get records from
   * @return the extracted records
   */
  private List<Record> getRecords(Sheet sheet) {
    try {
      long id = 1;
      int row = findHeaderRow(sheet);

      // Get the column headings
      final Map<String, Integer> columnHeadingsMap = new HashMap<>();
      for (int i = 0; i < countHeadings(sheet, row); i++) {
        columnHeadingsMap.put(columnName(sheet.getCell(i, row).getContents()), i);
      }

      // Does this sheet have translations or not?
      final int translationColumn = getTranslationsColumnIndex(sheet, row);

      // -- Now get the data...
      //
      row++; // The data is always one row below the headings
      List<Record> records = new LinkedList<>();
      for (; row < sheet.getRows(); row++) {
        final Cell[] cells = sheet.getRow(row);

        // If all the cells are blank then this is the end of the table
        if (allBlank(cells)) {
          break;
        }

        records.add(createRecord(id++, columnHeadingsMap, translationColumn, cells));
      }

      return records;
    } catch (Exception e) {
      throw new RuntimeException("Failed to parse worksheet [" + sheet.getName() + "]", e);
    }
  }


  /**
   * Determines if the given cells are all blank or not.
   * @param cells to check if they are blank or not
   * @return true if all the cells are blank, otherwise false.
   */
  private boolean allBlank(final Cell... cells) {
    for (Cell cell : cells) {
      if (cell.getContents().length() != 0) {
        return false;
      }
    }
    return true;
  }


  /**
   * Creates a record from a set of cells from a worksheet.
   *
   * @param id ID of the row
   * @param columnHeadingsMap Map of column headings to their index
   * @param translationColumn Column containing translations
   * @param cells The cells to process
   * @return the created record
   */
  private Record createRecord(final long id, final Map<String, Integer> columnHeadingsMap, final int translationColumn, final Cell... cells) {
    final int translationId;
    if (translationColumn != -1 && cells[translationColumn].getContents().length() > 0) {
      translationId = translations.size() + 1;
      translations.add(createTranslationRecord(translationId, cells[translationColumn].getContents()));
    } else {
      translationId = 0;
    }

    final RecordBuilder record = DataSetUtils.record();
    for (Entry<String, Integer> column : columnHeadingsMap.entrySet()) {
      if (column.getValue() < cells.length) {
        record.setString(column.getKey(), cells[column.getValue()].getContents());
      } else {
        // If the cell is actually specified then assume it is default blank
        record.setString(column.getKey(), "");
      }
    }
    record.setLong("id", id);
    record.setInteger("translationId", translationId);
    return record;
  }


  /**
   * Converts the given long name in to a column name. This is the same as
   * removing all the spaces and making the first character lowercase.
   *
   * @param longName the long name to convert
   * @return the name of the column
   */
  private String columnName(final String longName) {
    final String noSpaces = longName.replaceAll(" ", "");
    return noSpaces.substring(0, 1).toLowerCase() + noSpaces.substring(1);
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.dataset.DataSetProducer#getSchema()
   */
  @Override
  public Schema getSchema() {
    return new Schema() {

      @Override
      public Table getTable(String name) {
        throw new UnsupportedOperationException("Cannot get the metadata of a table for a spreadsheet");
      }

      @Override
      public boolean isEmptyDatabase() {
        return tables.isEmpty();
      }

      @Override
      public boolean tableExists(String name) {
        return tables.containsKey(name);
      }

      @Override
      public Collection<String> tableNames() {
        return tables.keySet();
      }

      @Override
      public Collection<String> partitionedTableNames() {
        return List.of();
      }

      @Override
      public Collection<String> partitionTableNames() {
        return List.of();
      }

      @Override
      public Collection<Table> tables() {
        throw new UnsupportedOperationException("Cannot get the metadata of a table for a spreadsheet");
      }

      @Override
      public boolean viewExists(String name) {
        return false;
      }

      @Override
      public View getView(String name) {
        throw new IllegalArgumentException("Invalid view [" + name + "]. Views are not supported in spreadsheets");
      }

      @Override
      public Collection<String> viewNames() {
        return Collections.emptySet();
      }

      @Override
      public Collection<View> views() {
        return Collections.emptySet();
      }

      @Override
      public boolean sequenceExists(String name) {
        return false;
      }

      @Override
      public Sequence getSequence(String name) {
        throw new IllegalArgumentException("Invalid sequence [" + name + "]. Sequences are not supported in spreadsheets");
      }

      @Override
      public Collection<String> sequenceNames() {
        return Collections.emptySet();
      }

      @Override
      public Collection<Sequence> sequences() {
        return Collections.emptySet();
      }
    };
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.dataset.DataSetProducer#open()
   */
  @Override
  public void open() {
    // Nothing to do
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.dataset.DataSetProducer#close()
   */
  @Override
  public void close() {
    // Nothing to do
  }


  /**
   * {@inheritDoc}
   *
   * @see org.alfasoftware.morf.dataset.DataSetProducer#records(java.lang.String)
   */
  @Override
  public Iterable<Record> records(String tableName) {
    return tables.get(tableName);
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#isTableEmpty(java.lang.String)
   */
  @Override
  public boolean isTableEmpty(String tableName) {
    return tables.get(tableName).isEmpty();
  }
}
