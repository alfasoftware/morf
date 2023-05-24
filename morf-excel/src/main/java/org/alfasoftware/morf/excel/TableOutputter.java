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

import static org.alfasoftware.morf.metadata.DataType.BIG_INTEGER;
import static org.alfasoftware.morf.metadata.DataType.CLOB;
import static org.alfasoftware.morf.metadata.DataType.DECIMAL;
import static org.alfasoftware.morf.metadata.DataType.INTEGER;
import static org.alfasoftware.morf.metadata.DataType.STRING;

import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

import jxl.Cell;
import jxl.format.Alignment;
import jxl.format.Border;
import jxl.format.BorderLineStyle;
import jxl.format.Colour;
import jxl.format.UnderlineStyle;
import jxl.format.VerticalAlignment;
import jxl.write.Label;
import jxl.write.WritableCell;
import jxl.write.WritableCellFormat;
import jxl.write.WritableFont;
import jxl.write.WritableHyperlink;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;

/**
 * Outputs tables to an excel spreadsheet.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class TableOutputter {

  private static final Log log = LogFactory.getLog(TableOutputter.class);

  /**
   * The number of rows in the title.
   */
  private static final int NUMBER_OF_ROWS_IN_TITLE = 2;

  /**
   * The maximum number of rows supported in an XLS.
   */
  private static final int MAX_EXCEL_ROWS = 65536;

  /**
   * The maximum number of rows supported in an XLS.
   */
  private static final int MAX_EXCEL_COLUMNS = 256;

  /**
   * The maximum number of CLOB characters supported in an XLS cell.
   */
  private static final int MAX_CLOB_CHARACTERS = 1000;

  /**
   * The data types we can output to a spreadsheet.
   */
  private static final Set<DataType> supportedDataTypes = Sets.immutableEnumSet(STRING, DECIMAL, BIG_INTEGER, INTEGER, CLOB);

  /**
   * A source of non-schema related data.
   */
  private final AdditionalSchemaData additionalSchemaData;


  /**
   * Constructor.
   *
   * @param additionalSchemaData A source of non-schema related data.
   */
  public TableOutputter(AdditionalSchemaData additionalSchemaData) {
    this.additionalSchemaData = additionalSchemaData;
  }


  /**
   * Output the given table to the given workbook.
   *
   * @param maxSampleRows the maximum number of rows to export in the "sample data" section
   *                      (all rows are included in the "Parameters to set up" section).
   * @param workbook to add the table to.
   * @param table to add to the workbook.
   * @param records of data to output.
   */
  public void table(int maxSampleRows, final WritableWorkbook workbook, final Table table, final Iterable<Record> records) {
    final WritableSheet workSheet = workbook.createSheet(spreadsheetifyName(table.getName()), workbook.getNumberOfSheets());

    boolean columnsTruncated = table.columns().size() > MAX_EXCEL_COLUMNS;
    if(columnsTruncated) {
      log.warn("Output for table '" + table.getName() + "' exceeds the maximum number of columns (" + MAX_EXCEL_COLUMNS + ") in an Excel worksheet. It will be truncated.");
    }

    boolean rowsTruncated = false;

    try {

      int currentRow = NUMBER_OF_ROWS_IN_TITLE + 1;

      try {

        final Map<String, Integer> helpTextRowNumbers = new HashMap<>();

        //Now output....
        //Help text
        currentRow = outputHelp(workSheet, table, currentRow, helpTextRowNumbers);

        //"Example Data"
        Label exampleLabel = new Label(0, currentRow, "Example Data");
        exampleLabel.setCellFormat(getBoldFormat());
        workSheet.addCell(exampleLabel);
        currentRow++;

        //Headings for example data
        currentRow = outputDataHeadings(workSheet, table, currentRow, helpTextRowNumbers);

        //Actual example data
        currentRow = outputExampleData(maxSampleRows, workSheet, table, currentRow, records);

        //"Parameters to Set Up"
        Label dataLabel = new Label(0, currentRow, "Parameters to Set Up");
        dataLabel.setCellFormat(getBoldFormat());
        workSheet.addCell(dataLabel);
        currentRow++;

        //Headings for parameters to be uploaded
        currentRow = outputDataHeadings(workSheet, table, currentRow, helpTextRowNumbers);
        currentRow = outputExampleData(null, workSheet, table, currentRow, records);
      } catch (RowLimitExceededException e) {
        log.warn(e.getMessage());
        rowsTruncated = true;
      }
    }
    catch (Exception e) {
      throw new RuntimeException("Error outputting table '" + table.getName() + "'", e);
    }

    /*
     * Write the title for the worksheet - adding truncation information if appropriate
     */
    if(columnsTruncated || rowsTruncated) {
      StringBuilder truncatedSuffix = new StringBuilder();
      truncatedSuffix.append(" [");

      if(columnsTruncated) {
        truncatedSuffix.append("COLUMNS");
      }

      if(columnsTruncated && rowsTruncated) {
        truncatedSuffix.append(" & ");
      }

      if(rowsTruncated) {
        truncatedSuffix.append("ROWS");
      }

      truncatedSuffix.append(" TRUNCATED]");

      createTitle(workSheet, workSheet.getName() + truncatedSuffix.toString(), table.getName());
    }
    else {
      createTitle(workSheet, workSheet.getName(), table.getName());
    }
  }


  /**
   * Converts camel capped names to something we can show in a spreadsheet.
   *
   * @param name Name to convert.
   * @return A human readable version of the name wtih camel caps replaced by spaces.
   */
  private String spreadsheetifyName(String name) {
    return StringUtils.capitalize(name).replaceAll("([A-Z][a-z])", " $1").trim();
  }


  /**
   * Inserts a row at the top of the sheet with the given title
   * @param sheet to add the title to
   * @param title to add
   * @param fileName of the ALFA file to which the sheet relates
   */
  private void createTitle(WritableSheet sheet, String title, String fileName) {
    try {
      //Friendly file name in A1
      Label cell = new Label(0, 0, title);
      WritableFont headingFont = new WritableFont(WritableFont.ARIAL, 16, WritableFont.BOLD);
      WritableCellFormat headingFormat = new WritableCellFormat(headingFont);
      cell.setCellFormat(headingFormat);
      sheet.addCell(cell);

      //ALFA file name in B2 (hidden in white)
      cell = new Label(1, 1, fileName);
      WritableFont fileNameFont = new WritableFont(WritableFont.ARIAL,10,WritableFont.NO_BOLD,false,UnderlineStyle.NO_UNDERLINE,Colour.WHITE);
      WritableCellFormat fileNameFormat = new WritableCellFormat(fileNameFont);
      cell.setCellFormat(fileNameFormat);
      sheet.addCell(cell);

      //Copyright notice in M1
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
   * @return the standard font to use
   */
  private WritableFont getStandardFont() {
    return new WritableFont(WritableFont.ARIAL, 8);
  }


  /**
   * @return the format to use for normal cells
   * @throws WriteException if the format could not be created
   */
  private WritableCellFormat getStandardFormat() throws WriteException {
    WritableCellFormat standardFormat = new WritableCellFormat(getStandardFont());
    standardFormat.setVerticalAlignment(VerticalAlignment.TOP);
    return standardFormat;
  }


  /**
   * @return the format to use for bold cells
   * @throws WriteException if the format could not be created
   */
  private WritableCellFormat getBoldFormat() throws WriteException {
    WritableFont boldFont = new WritableFont(WritableFont.ARIAL, 8, WritableFont.BOLD);
    WritableCellFormat boldHeading = new WritableCellFormat(boldFont);
    boldHeading.setBorder(Border.BOTTOM, BorderLineStyle.MEDIUM);
    boldHeading.setVerticalAlignment(VerticalAlignment.CENTRE);
    boldHeading.setBackground(Colour.GRAY_25);


    WritableCellFormat boldFormat = new WritableCellFormat(boldFont);
    boldFormat.setVerticalAlignment(VerticalAlignment.TOP);
    return boldFormat;
  }


  /**
   * Outputs the example data rows.
   *
   * @param numberOfExamples to output
   * @param workSheet to add the data rows to
   * @param table to get metadata from
   * @param startRow to start adding the example rows at
   * @param records to add as examples
   * @return the new row to carry on outputting at
   * @throws WriteException if any of the writes to workSheet fail
   */
  private int outputExampleData(final Integer numberOfExamples, WritableSheet workSheet, Table table, final int startRow, Iterable<Record> records) throws WriteException {
    int currentRow = startRow;

    int rowsOutput = 0;
    for (Record record : records) {

      if (currentRow >= MAX_EXCEL_ROWS) {
        continue;
      }

      if (numberOfExamples != null && rowsOutput >= numberOfExamples) {
        // Need to continue the loop rather than break as we need to close
        // the connection which happens at the end of iteration...
        continue;
      }

      record(currentRow, workSheet, table, record);
      rowsOutput++;
      currentRow++;
    }

    if (currentRow >= MAX_EXCEL_ROWS) {
      // This is a fix for WEB-56074. It will be removed if/when WEB-42351 is developed.
      throw new RowLimitExceededException("Output for table '" + table.getName() + "' exceeds the maximum number of rows (" + MAX_EXCEL_ROWS + ") in an Excel worksheet. It will be truncated.");
    }

    currentRow++;
    return currentRow;
  }


  /**
   * @param workSheet to add the help to
   * @param table to fetch metadata from
   * @param startRow to start adding rows at
   * @param helpTextRowNumbers - map to insert row numbers for each help field into
   * @return the index of the next row to use
   * @throws WriteException if any of the writes to workSheet failed
   */
  private int outputHelp(WritableSheet workSheet, Table table, final int startRow, final Map<String, Integer> helpTextRowNumbers) throws WriteException {
    int currentRow = startRow;

    // Title for the descriptions
    Label dataLabel = new Label(0, currentRow, "Column Descriptions");
    dataLabel.setCellFormat(getBoldFormat());
    workSheet.addCell(dataLabel);
    currentRow++;

    int currentColumn = 0;

    for (Column column : table.columns()) {
      if (!column.getName().equals("id") && !column.getName().equals("version")) {
        // Field name to go with the description
        Label fieldName = new Label(0, currentRow, spreadsheetifyName(column.getName()));
        fieldName.setCellFormat(getBoldFormat());
        workSheet.addCell(fieldName);

        // The type/width
        String typeString = column.getType() + "(" + column.getWidth() + (column.getScale() == 0 ? "" : "," + column.getScale()) + ")";
        Label fieldType = new Label(1, currentRow, typeString);
        fieldType.setCellFormat(getStandardFormat());
        workSheet.addCell(fieldType);

        // The default
        String defaultValue = additionalSchemaData.columnDefaultValue(table, column.getName());
        Label fieldDefault = new Label(2, currentRow, defaultValue);
        fieldDefault.setCellFormat(getStandardFormat());
        workSheet.addCell(fieldDefault);

        // The field documentation
        workSheet.mergeCells(3, currentRow, 12, currentRow);
        String documentation = additionalSchemaData.columnDocumentation(table, column.getName());
        Label documentationLabel = new Label(3, currentRow, documentation);
        WritableCellFormat format = new WritableCellFormat(getStandardFormat());
        format.setWrap(true);
        format.setVerticalAlignment(VerticalAlignment.TOP);
        documentationLabel.setCellFormat(format);
        workSheet.addCell(documentationLabel);

        //If we've exceed the maximum number of columns - then output truncated warnings
        if(currentColumn >= MAX_EXCEL_COLUMNS) {
          Label truncatedWarning = new Label(13, currentRow, "[TRUNCATED]");
          truncatedWarning.setCellFormat(getBoldFormat());
          workSheet.addCell(truncatedWarning);
        }

        // We are aiming for 150px. 1px is 15 Excel "Units"
        workSheet.setRowView(currentRow, 150 * 15);

        // Remember at what row we created the help text for this column
        helpTextRowNumbers.put(column.getName(), currentRow);

        currentRow++;
        currentColumn++;
      }

    }

    // Group all the help rows together
    workSheet.setRowGroup(startRow + 1, currentRow - 1, true);

    // Some extra blank space for neatness
    currentRow++;

    return currentRow;
  }


  /**
   * Outputs the data headings row.
   *
   * @param workSheet to add the row to
   * @param table to fetch metadata from
   * @param startRow to add the headings at
   * @param helpTextRowNumbers - the map of column names to row index for each
   *   bit of help text
   * @throws WriteException if any of the writes to workSheet failed
   * @return the row to carry on inserting at
   */
  private int outputDataHeadings(WritableSheet workSheet, Table table, final int startRow, final Map<String, Integer> helpTextRowNumbers) throws WriteException {
    int currentRow = startRow;

    int columnNumber = 0;
    final WritableCellFormat columnHeadingFormat = getBoldFormat();

    columnHeadingFormat.setBackground(Colour.VERY_LIGHT_YELLOW);
    WritableFont font = new WritableFont(WritableFont.ARIAL, 8, WritableFont.BOLD);
    font.setColour(Colour.BLUE);
    font.setUnderlineStyle(UnderlineStyle.SINGLE);
    columnHeadingFormat.setFont(font);

    for (Column column : table.columns()) {

      if(columnNumber < MAX_EXCEL_COLUMNS && !column.getName().equals("id") && !column.getName().equals("version")) {
        // Data heading is a link back to the help text
        WritableHyperlink linkToHelp = new WritableHyperlink(
          columnNumber, currentRow,
          spreadsheetifyName(column.getName()),
          workSheet, 0, helpTextRowNumbers.get(column.getName()));
        workSheet.addHyperlink(linkToHelp);
        WritableCell label = workSheet.getWritableCell(columnNumber, currentRow);
        label.setCellFormat(columnHeadingFormat);

        // Update the help text such that it is a link to the heading
        Cell helpCell = workSheet.getCell(0, helpTextRowNumbers.get(column.getName()));
        WritableHyperlink linkFromHelp = new WritableHyperlink(
          0, helpTextRowNumbers.get(column.getName()),
          helpCell.getContents(),
          workSheet, columnNumber, currentRow);
        workSheet.addHyperlink(linkFromHelp);

        columnNumber++;
      }
    }

    currentRow++;

    return currentRow;
  }


  /**
   * @param row to add the record at
   * @param worksheet to add the record to
   * @param table that the record comes from
   * @param record Record to serialise. This method is part of the old Cryo API.
   */
  private void record(final int row, final WritableSheet worksheet, final Table table, Record record) {
    int columnNumber = 0;
    WritableFont standardFont = new WritableFont(WritableFont.ARIAL, 8);
    WritableCellFormat standardFormat = new WritableCellFormat(standardFont);

    WritableCellFormat exampleFormat = new WritableCellFormat(standardFont);
    try {
      exampleFormat.setBackground(Colour.ICE_BLUE);
    } catch (WriteException e) {
      throw new RuntimeException("Failed to set example background colour", e);
    }

    for (Column column : table.columns()) {
      if(columnNumber < MAX_EXCEL_COLUMNS && !column.getName().equals("id") && !column.getName().equals("version")) {
        createCell(worksheet, column, columnNumber, row, record, standardFormat);
        columnNumber++;
      }
    }
  }

  /**
   * Creates the cell at the given position.
   *
   * @param currentWorkSheet to add the cell to
   * @param column The meta data for the column in the source table
   * @param columnNumber The column number to insert at (0 based)
   * @param rowIndex The row number to insert at (0 based)
   * @param record The source record
   * @param format The format to apply to the cell
   */
  private void createCell(final WritableSheet currentWorkSheet, Column column, int columnNumber, int rowIndex, Record record, WritableCellFormat format) {
    WritableCell writableCell = null;

    switch (column.getType()) {
      case STRING:
        writableCell = new Label(columnNumber, rowIndex, record.getString(column.getName()));
        break;

      case DECIMAL:
        BigDecimal decimalValue = record.getBigDecimal(column.getName());
        try {
          if (decimalValue == null) {
            writableCell = new jxl.write.Blank(columnNumber, rowIndex);
          } else {
            writableCell = new jxl.write.Number(columnNumber, rowIndex, decimalValue.doubleValue());
          }
        } catch (Exception e) {
          throw new UnsupportedOperationException("Cannot generate Excel cell (parseDouble) for data [" + decimalValue + "] in column [" + column.getName() + "] of table [" + currentWorkSheet.getName() + "]", e);
        }
        break;

      case BIG_INTEGER:
      case INTEGER:
        Long longValue = record.getLong(column.getName());
        try {
          if (longValue == null) {
            writableCell = new jxl.write.Blank(columnNumber, rowIndex);
          } else {
            writableCell = new jxl.write.Number(columnNumber, rowIndex, longValue);
          }
        } catch (Exception e) {
          throw new UnsupportedOperationException("Cannot generate Excel cell (parseInt) for data [" + longValue + "] in column [" + column.getName() + "] of table [" + currentWorkSheet.getName() + "]", e);
        }
        break;

      case CLOB:
        try {
          String stringValue = record.getString(column.getName());
          if (stringValue == null) {
            writableCell = new jxl.write.Blank(columnNumber, rowIndex);
          } else {
            writableCell = new Label(columnNumber, rowIndex, StringUtils.substring(stringValue, 0, MAX_CLOB_CHARACTERS));
          }
        } catch (Exception e) {
          throw new UnsupportedOperationException("Cannot generate Excel cell for CLOB data in column [" + column.getName() + "] of table [" + currentWorkSheet.getName() + "]", e);
        }
        break;

      default:
        throw new UnsupportedOperationException("Cannot output data type [" + column.getType() + "] to a spreadsheet");
    }
    writableCell.setCellFormat(format);

    try {
      currentWorkSheet.addCell(writableCell);
    } catch (Exception e) {
      throw new RuntimeException("Error writing value to spreadsheet", e);
    }
  }


  /**
   * Indicates if the table has a column with a column type which we can't
   * output to a spreadsheet.
   *
   * @param table The table metadata.
   * @return
   */
  boolean tableHasUnsupportedColumns(Table table) {
    return Iterables.any(table.columns(), new Predicate<Column>() {
      @Override
      public boolean apply(Column column) {
        return !supportedDataTypes.contains(column.getType());
      }
    });
  }


  /**
   * Thrown if the excel representation of the table has more than {@link TableOutputter#MAX_EXCEL_ROWS}
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  private class RowLimitExceededException extends RuntimeException {

    public RowLimitExceededException(String message) {
      super(message);
    }
  }
}
