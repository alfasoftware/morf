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
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.hssf.util.HSSFColor.HSSFColorPredefined;
import org.apache.poi.ss.usermodel.BorderStyle;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.Hyperlink;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.VerticalAlignment;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.WorkbookUtil;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

/**
 * Outputs tables to an excel spreadsheet.
 */
class TableOutputter {

  private static final Log log = LogFactory.getLog(TableOutputter.class);
  private static final int NUMBER_OF_ROWS_IN_TITLE = 2;
  private static final int MAX_EXCEL_ROWS = 65536;
  private static final int MAX_EXCEL_COLUMNS = 256;
  private static final int MAX_CELL_CHARACTERS = 32767;
  private static final Set<DataType> SUPPORTED_DATA_TYPES = Sets.immutableEnumSet(STRING, DECIMAL, BIG_INTEGER, INTEGER, CLOB);

  private final AdditionalSchemaData additionalSchemaData;

  public TableOutputter(AdditionalSchemaData additionalSchemaData) {
    this.additionalSchemaData = additionalSchemaData;
  }

  public void table(int maxSampleRows, final Workbook workbook, final Table table, final Iterable<Record> records) {
    final Sheet worksheet = workbook.createSheet(WorkbookUtil.createSafeSheetName(spreadsheetifyName(table.getName())));

    boolean columnsTruncated = table.columns().size() > MAX_EXCEL_COLUMNS;
    if (columnsTruncated) {
      log.warn("Output for table '" + table.getName() + "' exceeds the maximum number of columns (" + MAX_EXCEL_COLUMNS + ") in an Excel worksheet. It will be truncated.");
    }

    boolean rowsTruncated = false;
    try {
      int currentRow = NUMBER_OF_ROWS_IN_TITLE + 1;
      try {
        final Map<String, Integer> helpTextRowNumbers = new HashMap<>();
        currentRow = outputHelp(worksheet, workbook, table, currentRow, helpTextRowNumbers);
        writeValue(worksheet, currentRow++, 0, "Example Data", getBoldFormat(workbook));
        currentRow = outputDataHeadings(worksheet, workbook, table, currentRow, helpTextRowNumbers);
        currentRow = outputExampleData(maxSampleRows, worksheet, workbook, table, currentRow, records);
        writeValue(worksheet, currentRow++, 0, "Parameters to Set Up", getBoldFormat(workbook));
        currentRow = outputDataHeadings(worksheet, workbook, table, currentRow, helpTextRowNumbers);
        outputExampleData(null, worksheet, workbook, table, currentRow, records);
      } catch (RowLimitExceededException e) {
        log.warn(e.getMessage());
        rowsTruncated = true;
      }
    } catch (Exception e) {
      throw new RuntimeException("Error outputting table '" + table.getName() + "'", e);
    }

    if (columnsTruncated || rowsTruncated) {
      StringBuilder suffix = new StringBuilder(" [");
      if (columnsTruncated) {
        suffix.append("COLUMNS");
      }
      if (columnsTruncated && rowsTruncated) {
        suffix.append(" & ");
      }
      if (rowsTruncated) {
        suffix.append("ROWS");
      }
      suffix.append(" TRUNCATED]");
      createTitle(worksheet, workbook, worksheet.getSheetName() + suffix, table.getName());
    } else {
      createTitle(worksheet, workbook, worksheet.getSheetName(), table.getName());
    }
  }

  private int outputHelp(Sheet worksheet, Workbook workbook, Table table, int startRow, Map<String, Integer> helpTextRowNumbers) {
    int currentRow = startRow;
    int columnIndex = 0;
    for (Column column : table.columns()) {
      if (columnIndex >= MAX_EXCEL_COLUMNS) {
        writeValue(worksheet, currentRow, columnIndex, "[TRUNCATED]", getStandardFormat(workbook));
        break;
      }
      helpTextRowNumbers.put(column.getName(), currentRow);
      writeValue(worksheet, currentRow, 0, column.getName(), getBoldFormat(workbook));
      writeValue(worksheet, currentRow, 1, additionalSchemaData.columnDocumentation(table, column.getName()), getStandardFormat(workbook));
      writeValue(worksheet, currentRow, 2, additionalSchemaData.columnDefaultValue(table, column.getName()), getStandardFormat(workbook));
      currentRow++;
      columnIndex++;
    }
    return currentRow + 1;
  }

  private int outputDataHeadings(Sheet worksheet, Workbook workbook, Table table, int rowIndex, Map<String, Integer> helpTextRowNumbers) {
    int columnIndex = 0;
    for (Column column : table.columns()) {
      if (columnIndex >= MAX_EXCEL_COLUMNS) {
        break;
      }
      Cell cell = writeValue(worksheet, rowIndex, columnIndex, spreadsheetifyName(column.getName()), getBoldHeadingFormat(workbook));
      Integer helpRow = helpTextRowNumbers.get(column.getName());
      if (helpRow != null) {
        Hyperlink hyperlink = workbook.getCreationHelper().createHyperlink(HyperlinkType.DOCUMENT);
        hyperlink.setAddress("'" + worksheet.getSheetName() + "'!A" + (helpRow + 1));
        cell.setHyperlink(hyperlink);
      }
      columnIndex++;
    }
    return rowIndex + 1;
  }

  private int outputExampleData(Integer numberOfExamples, Sheet worksheet, Workbook workbook, Table table, int startRow,
      Iterable<Record> records) {
    int currentRow = startRow;
    int written = 0;
    for (Record record : records) {
      if (numberOfExamples != null && written >= numberOfExamples) {
        break;
      }
      if (currentRow >= MAX_EXCEL_ROWS - 1) {
        throw new RowLimitExceededException("Output for table '" + table.getName() + "' exceeds the maximum number of rows (" + MAX_EXCEL_ROWS + ") in an Excel worksheet. It will be truncated.");
      }
      int columnIndex = 0;
      for (Column column : table.columns()) {
        if (columnIndex >= MAX_EXCEL_COLUMNS) {
          writeValue(worksheet, currentRow, columnIndex, "[TRUNCATED]", getStandardFormat(workbook));
          break;
        }
        writeColumnValue(worksheet, workbook, currentRow, columnIndex, column, record);
        columnIndex++;
      }
      currentRow++;
      written++;
    }
    return currentRow + 1;
  }

  private void writeColumnValue(Sheet worksheet, Workbook workbook, int rowIndex, int columnIndex, Column column, Record record) {
    try {
      switch (column.getType()) {
        case STRING:
        case CLOB:
          String text = record.getString(column.getName());
          if (text == null) {
            text = "";
          }
          if (text.length() > MAX_CELL_CHARACTERS) {
            text = text.substring(0, MAX_CELL_CHARACTERS);
          }
          writeValue(worksheet, rowIndex, columnIndex, text, getStandardFormat(workbook));
          break;
        case DECIMAL:
          BigDecimal decimal = record.getBigDecimal(column.getName());
          writeValue(worksheet, rowIndex, columnIndex, decimal == null ? "" : decimal.toString(), getStandardFormat(workbook));
          break;
        case BIG_INTEGER:
        case INTEGER:
          Long longValue = record.getLong(column.getName());
          writeValue(worksheet, rowIndex, columnIndex, longValue == null ? "0" : longValue.toString(), getStandardFormat(workbook));
          break;
        default:
          throw new UnsupportedOperationException("Unsupported data type " + column.getType() + unsupportedOperationExceptionMessageSuffix(column, worksheet));
      }
    } catch (RuntimeException e) {
      String message = column.getType() == CLOB
          ? "Cannot generate Excel cell for CLOB data" + unsupportedOperationExceptionMessageSuffix(column, worksheet)
          : "Cannot generate Excel cell for data" + unsupportedOperationExceptionMessageSuffix(column, worksheet);
      throw new UnsupportedOperationException(message, e);
    }
  }

  private Cell writeValue(Sheet sheet, int rowIndex, int columnIndex, String value, CellStyle style) {
    Row row = getOrCreateRow(sheet, rowIndex);
    Cell cell = row.createCell(columnIndex);
    cell.setCellValue(value);
    cell.setCellStyle(style);
    return cell;
  }

  private void createTitle(Sheet sheet, Workbook workbook, String title, String fileName) {
    Cell titleCell = writeValue(sheet, 0, 0, title, titleStyle(workbook));
    titleCell.setCellStyle(titleStyle(workbook));

    Cell fileNameCell = writeValue(sheet, 1, 1, fileName, hiddenFileNameStyle(workbook));
    fileNameCell.setCellStyle(hiddenFileNameStyle(workbook));

    Cell copyrightCell = writeValue(sheet, 0, 12,
        "Copyright " + new SimpleDateFormat("yyyy").format(new Date()) + " Alfa Financial Software Ltd.",
        copyrightStyle(workbook));
    copyrightCell.setCellStyle(copyrightStyle(workbook));
  }

  private CellStyle titleStyle(Workbook workbook) {
    CellStyle style = workbook.createCellStyle();
    Font font = workbook.createFont();
    font.setBold(true);
    font.setFontHeightInPoints((short) 16);
    style.setFont(font);
    return style;
  }

  private CellStyle hiddenFileNameStyle(Workbook workbook) {
    CellStyle style = workbook.createCellStyle();
    Font font = workbook.createFont();
    font.setColor(HSSFColorPredefined.WHITE.getIndex());
    style.setFont(font);
    return style;
  }

  private CellStyle copyrightStyle(Workbook workbook) {
    CellStyle style = workbook.createCellStyle();
    style.setAlignment(HorizontalAlignment.RIGHT);
    return style;
  }

  private String spreadsheetifyName(String name) {
    return StringUtils.capitalize(name).replaceAll("([A-Z][a-z])", " $1").trim();
  }

  private CellStyle getStandardFormat(Workbook workbook) {
    CellStyle style = workbook.createCellStyle();
    style.setVerticalAlignment(VerticalAlignment.TOP);
    Font font = workbook.createFont();
    font.setFontHeightInPoints((short) 8);
    style.setFont(font);
    return style;
  }

  private CellStyle getBoldFormat(Workbook workbook) {
    CellStyle style = workbook.createCellStyle();
    style.setVerticalAlignment(VerticalAlignment.TOP);
    Font font = workbook.createFont();
    font.setBold(true);
    font.setFontHeightInPoints((short) 8);
    style.setFont(font);
    return style;
  }

  private CellStyle getBoldHeadingFormat(Workbook workbook) {
    CellStyle style = getBoldFormat(workbook);
    style.setBorderBottom(BorderStyle.MEDIUM);
    style.setVerticalAlignment(VerticalAlignment.CENTER);
    style.setFillForegroundColor(HSSFColorPredefined.GREY_25_PERCENT.getIndex());
    style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
    return style;
  }

  boolean tableHasUnsupportedColumns(Table table) {
    return Iterables.any(table.columns(), new Predicate<Column>() {
      @Override
      public boolean apply(Column column) {
        return !SUPPORTED_DATA_TYPES.contains(column.getType());
      }
    });
  }

  private Row getOrCreateRow(Sheet sheet, int rowIndex) {
    Row row = sheet.getRow(rowIndex);
    return row == null ? sheet.createRow(rowIndex) : row;
  }

  private String unsupportedOperationExceptionMessageSuffix(Column column, Sheet worksheet) {
    return " in column [" + column.getName() + "] of table [" + worksheet.getSheetName() + "]";
  }

  private static class RowLimitExceededException extends RuntimeException {
    RowLimitExceededException(String message) {
      super(message);
    }
  }
}
