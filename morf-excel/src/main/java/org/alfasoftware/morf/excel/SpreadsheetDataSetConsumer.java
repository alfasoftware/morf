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

import java.io.IOException;
import java.io.OutputStream;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Optional;

import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Table;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import jxl.Workbook;
import jxl.format.Alignment;
import jxl.format.Colour;
import jxl.format.UnderlineStyle;
import jxl.write.Label;
import jxl.write.WritableCellFormat;
import jxl.write.WritableFont;
import jxl.write.WritableHyperlink;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;

/**
 * Consumes data sets and outputs spreadsheets.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class SpreadsheetDataSetConsumer implements DataSetConsumer {

  private static final Log log = LogFactory.getLog(SpreadsheetDataSetConsumer.class);

  /**
   *
   * The number of rows in the title.
   */
  private static final int NUMBER_OF_ROWS_IN_TITLE = 2;

  /**
   * The number of rows to include in the sample section if not specified.
   */
  private static final int DEFAULT_SAMPLE_ROWS = 5;

  /**
   * Stream to which the spreadsheet will be output.
   */
  private final OutputStream documentOutputStream;

  /**
   * The workbook currently under construction.
   */
  private WritableWorkbook workbook;

  /**
   * Number of rows to output for each table.
   */
  private final Optional<Map<String, Integer>> rowsPerTable;


  /**
   * Outputter for putting tables into Excel.
   */
  private final TableOutputter tableOutputter;


  /**
   * @param documentOutputStream Stream to which the spreadsheet file should be written.
   */
  public SpreadsheetDataSetConsumer(OutputStream documentOutputStream) {
    this(documentOutputStream, Optional.<Map<String, Integer>>empty());
  }


  /**
   * @param documentOutputStream Stream to which the spreadsheet file should be written.
   * @param rowsPerTable stream The list of tables to export along with the number of rows per table to export. If not supplied,
   *                            all tables are exported to the limit of rows in Excel.
   */
  public SpreadsheetDataSetConsumer(
      OutputStream documentOutputStream,
      Optional<Map<String, Integer>> rowsPerTable) {
    this(documentOutputStream, rowsPerTable, new DefaultAdditionalSchemaDataImpl());
  }


  /**
   * @param documentOutputStream Stream to which the spreadsheet file should be written.
   * @param rowsPerTable stream The list of tables to export along with the number of rows per table to export. If not supplied,
   *                            all tables are exported to the limit of rows in Excel.
   * @param additionalSchemaData the source of additional metadata not available from the database schema.
   */
  public SpreadsheetDataSetConsumer(
      OutputStream documentOutputStream,
      Optional<Map<String, Integer>> rowsPerTable,
      AdditionalSchemaData additionalSchemaData) {
    this(documentOutputStream, rowsPerTable, new TableOutputter(additionalSchemaData));
  }


  /**
   * Package private constructor, for testing purposes (isolates the {@link TableOutputter}
   * dependency).
   */
  SpreadsheetDataSetConsumer(
      OutputStream documentOutputStream,
      Optional<Map<String, Integer>> rowsPerTable,
      TableOutputter tableOutputter) {
    super();
    this.documentOutputStream = documentOutputStream;
    this.tableOutputter = tableOutputter;
    this.rowsPerTable = rowsPerTable;
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#open()
   */
  @Override
  public void open() {
    try {
      workbook = Workbook.createWorkbook(documentOutputStream);
    } catch (IOException e) {
      throw new RuntimeException("Error creating writable workbook", e);
    }
  }


  /**
   * Create the index worksheet.
   *
   * <p>This also creates links back to the index in each of the worksheets.</p>
   */
  public void createIndex() {
    WritableSheet sheet = workbook.createSheet(spreadsheetifyName("Index"), 0);
    createTitle(sheet, "Index");

    try {
      // Create links for each worksheet, apart from the first sheet which is the
      // index we're currently creating
      final String[] names = workbook.getSheetNames();
      for (int currentSheet = 1; currentSheet < names.length; currentSheet++) {
        // Create the link from the index to the table's worksheet
        WritableHyperlink link = new WritableHyperlink(0, currentSheet - 1 + NUMBER_OF_ROWS_IN_TITLE, names[currentSheet], workbook.getSheet(currentSheet), 0, 0);
        sheet.addHyperlink(link);

        //Add the filename in column B (stored in cell B2 of each sheet)
        String fileName = workbook.getSheet(currentSheet).getCell(1, 1).getContents();
        Label fileNameLabel = new Label(1, currentSheet - 1 + NUMBER_OF_ROWS_IN_TITLE, fileName);
        WritableFont fileNameFont = new WritableFont(WritableFont.ARIAL,10,WritableFont.NO_BOLD,false,UnderlineStyle.NO_UNDERLINE,Colour.BLACK);
        WritableCellFormat fileNameFormat = new WritableCellFormat(fileNameFont);
        fileNameLabel.setCellFormat(fileNameFormat);
        sheet.addCell(fileNameLabel);

        // Create the link back to the index
        link = new WritableHyperlink(0, 1, "Back to index", sheet, 0, currentSheet + NUMBER_OF_ROWS_IN_TITLE - 1);
        workbook.getSheet(currentSheet).addHyperlink(link);
        //Set column A of each sheet to be wide enough to show "Back to index"
        workbook.getSheet(currentSheet).setColumnView(0, 13);
      }

      // Make Column A fairly wide to show tab names and hide column B
      sheet.setColumnView(0, 35);
      sheet.setColumnView(1, 0);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Converts camel capped names to something we can show in a spreadsheet.
   *
   * @param name Name to convert.
   * @return A human readable version of the name wtih camel caps replaced by spaces.
   */
  private String spreadsheetifyName(String name) {
    return StringUtils.capitalize(name).replaceAll("([A-Z])", " $1").trim();
  }


  /**
   * Inserts a row at the top of the sheet with the given title
   * @param sheet add the title to
   * @param title to add
   */
  protected void createTitle(WritableSheet sheet, String title) {
    try {
      Label cell = new Label(0, 0, title);
      WritableFont headingFont = new WritableFont(WritableFont.ARIAL, 16, WritableFont.BOLD);
      WritableCellFormat headingFormat = new WritableCellFormat(headingFont);
      cell.setCellFormat(headingFormat);
      sheet.addCell(cell);

      cell = new Label(12, 0, "Copyright " + new SimpleDateFormat("yyyy").format(new Date()) + " Alfa Financial Software Ltd.");
      WritableCellFormat copyrightFormat = new WritableCellFormat();
      copyrightFormat.setAlignment(Alignment.RIGHT);
      cell.setCellFormat(copyrightFormat);
      sheet.addCell(cell);

    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#close(CloseState)
   */
  @Override
  public void close(CloseState closeState) {
    try {

      // Create the index
      createIndex();

      workbook.write();
      workbook.close();
    } catch (Exception e) {
      throw new RuntimeException("Error closing writable workbook", e);
    }
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#table(org.alfasoftware.morf.metadata.Table, java.lang.Iterable)
   */
  @Override
  public void table(Table table, Iterable<Record> records) {
    Integer maxSampleRows = DEFAULT_SAMPLE_ROWS;
    if (rowsPerTable.isPresent()) {
      maxSampleRows = rowsPerTable.get().get(table.getName().toUpperCase());
    }
    if (maxSampleRows == null) {
      log.info("File [" + table.getName() + "] excluded in configuration." );
    } else if (tableOutputter.tableHasUnsupportedColumns(table)) {
      log.info("File [" + table.getName() + "] skipped - unsupported columns." );
    } else {
      log.info("File [" + table.getName() + "] generating..." );
      tableOutputter.table(maxSampleRows, workbook, table, records);
    }
  }
}
