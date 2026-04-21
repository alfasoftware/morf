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
import java.util.IdentityHashMap;
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
import org.apache.poi.ss.usermodel.CellType;
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
  private final Map<Workbook, CellStyles> styleCache = new IdentityHashMap<>();

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
    writeValue(worksheet, currentRow++, 0, "Column Descriptions", getBoldFormat(workbook));

    int currentColumn = 0;
    for (Column column : table.columns()) {
      if ("id".equals(column.getName()) || "version".equals(column.getName())) {
        continue;
      }

      writeValue(worksheet, currentRow, 0, spreadsheetifyName(column.getName()), getBoldFormat(workbook));
      String typeString = column.getType() + "(" + column.getWidth() + (column.getScale() == 0 ? "" : "," + column.getScale()) + ")";
      writeValue(worksheet, currentRow, 1, typeString, getStandardFormat(workbook));
      writeValue(worksheet, currentRow, 2, additionalSchemaData.columnDefaultValue(table, column.getName()), getStandardFormat(workbook));
      writeValue(worksheet, currentRow, 3, additionalSchemaData.columnDocumentation(table, column.getName()), getWrappedFormat(workbook));
      if (currentColumn >= MAX_EXCEL_COLUMNS) {
        writeValue(worksheet, currentRow, 13, "[TRUNCATED]", getBoldFormat(workbook));
      }
      helpTextRowNumbers.put(column.getName(), currentRow);
      currentRow++;
      currentColumn++;
    }
    return currentRow + 1;
  }

  private int outputDataHeadings(Sheet worksheet, Workbook workbook, Table table, int rowIndex, Map<String, Integer> helpTextRowNumbers) {
    int columnIndex = 0;
    for (Column column : table.columns()) {
      if ("id".equals(column.getName()) || "version".equals(column.getName())) {
        continue;
      }
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
      if (currentRow >= MAX_EXCEL_ROWS) {
        continue;
      }
      if (numberOfExamples != null && written >= numberOfExamples) {
        continue;
      }
      int columnIndex = 0;
      for (Column column : table.columns()) {
        if ("id".equals(column.getName()) || "version".equals(column.getName())) {
          continue;
        }
        if (columnIndex >= MAX_EXCEL_COLUMNS) {
          break;
        }
        writeColumnValue(worksheet, workbook, currentRow, columnIndex, column, record);
        columnIndex++;
      }
      currentRow++;
      written++;
    }
    if (currentRow >= MAX_EXCEL_ROWS) {
      throw new RowLimitExceededException("Output for table '" + table.getName() + "' exceeds the maximum number of rows (" + MAX_EXCEL_ROWS + ") in an Excel worksheet. It will be truncated.");
    }
    return currentRow + 1;
  }

  private void writeColumnValue(Sheet worksheet, Workbook workbook, int rowIndex, int columnIndex, Column column, Record record) {
    CellStyle style = getStandardFormat(workbook);
    switch (column.getType()) {
      case STRING:
        writeStringCell(worksheet, rowIndex, columnIndex, record.getString(column.getName()), style);
        break;
      case DECIMAL:
        BigDecimal decimalValue = record.getBigDecimal(column.getName());
        try {
          if (decimalValue == null) {
            createBlankCell(worksheet, rowIndex, columnIndex, style);
          } else {
            writeNumericCell(worksheet, rowIndex, columnIndex, decimalValue.doubleValue(), style);
          }
        } catch (Exception e) {
          throw new UnsupportedOperationException("Cannot generate Excel cell (parseDouble) for data [" + decimalValue + "]" + unsupportedOperationExceptionMessageSuffix(column, worksheet), e);
        }
        break;
      case BIG_INTEGER:
      case INTEGER:
        Long longValue = record.getLong(column.getName());
        try {
          if (longValue == null) {
            createBlankCell(worksheet, rowIndex, columnIndex, style);
          } else {
            writeNumericCell(worksheet, rowIndex, columnIndex, longValue.doubleValue(), style);
          }
        } catch (Exception e) {
          throw new UnsupportedOperationException("Cannot generate Excel cell (parseInt) for data [" + longValue + "]" + unsupportedOperationExceptionMessageSuffix(column, worksheet), e);
        }
        break;
      case CLOB:
        try {
          String stringValue = record.getString(column.getName());
          if (stringValue == null) {
            createBlankCell(worksheet, rowIndex, columnIndex, style);
          } else {
            writeStringCell(worksheet, rowIndex, columnIndex, StringUtils.substring(stringValue, 0, MAX_CELL_CHARACTERS), style);
          }
        } catch (Exception e) {
          throw new UnsupportedOperationException("Cannot generate Excel cell for CLOB data" + unsupportedOperationExceptionMessageSuffix(column, worksheet), e);
        }
        break;
      default:
        throw new UnsupportedOperationException("Cannot output data type [" + column.getType() + "] to a spreadsheet");
    }
  }

  private Cell writeValue(Sheet sheet, int rowIndex, int columnIndex, String value, CellStyle style) {
    Row row = getOrCreateRow(sheet, rowIndex);
    Cell cell = row.createCell(columnIndex);
    cell.setCellValue(value);
    cell.setCellStyle(style);
    return cell;
  }

  private void writeStringCell(Sheet sheet, int rowIndex, int columnIndex, String value, CellStyle style) {
    if (value == null) {
      createBlankCell(sheet, rowIndex, columnIndex, style);
    } else {
      writeValue(sheet, rowIndex, columnIndex, value, style);
    }
  }

  private void writeNumericCell(Sheet sheet, int rowIndex, int columnIndex, double value, CellStyle style) {
    Row row = getOrCreateRow(sheet, rowIndex);
    Cell cell = row.createCell(columnIndex, CellType.NUMERIC);
    cell.setCellValue(value);
    cell.setCellStyle(style);
  }

  private void createBlankCell(Sheet sheet, int rowIndex, int columnIndex, CellStyle style) {
    Row row = getOrCreateRow(sheet, rowIndex);
    Cell cell = row.createCell(columnIndex, CellType.BLANK);
    cell.setCellStyle(style);
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
    return getStyles(workbook).titleStyle;
  }

  private CellStyle hiddenFileNameStyle(Workbook workbook) {
    return getStyles(workbook).hiddenFileNameStyle;
  }

  private CellStyle copyrightStyle(Workbook workbook) {
    return getStyles(workbook).copyrightStyle;
  }

  private String spreadsheetifyName(String name) {
    return StringUtils.capitalize(name).replaceAll("([A-Z][a-z])", " $1").trim();
  }

  private CellStyle getStandardFormat(Workbook workbook) {
    return getStyles(workbook).standardFormat;
  }

  private CellStyle getWrappedFormat(Workbook workbook) {
    return getStyles(workbook).wrappedFormat;
  }

  private CellStyle getBoldFormat(Workbook workbook) {
    return getStyles(workbook).boldFormat;
  }

  private CellStyle getBoldHeadingFormat(Workbook workbook) {
    return getStyles(workbook).boldHeadingFormat;
  }


  private CellStyles getStyles(Workbook workbook) {
    CellStyles styles = styleCache.get(workbook);
    if (styles == null) {
      styles = new CellStyles(workbook);
      styleCache.put(workbook, styles);
    }
    return styles;
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

  private static final class CellStyles {
    private final CellStyle titleStyle;
    private final CellStyle hiddenFileNameStyle;
    private final CellStyle copyrightStyle;
    private final CellStyle standardFormat;
    private final CellStyle wrappedFormat;
    private final CellStyle boldFormat;
    private final CellStyle boldHeadingFormat;

    private CellStyles(Workbook workbook) {
      Font standardFont = workbook.createFont();
      standardFont.setFontHeightInPoints((short) 8);

      Font boldFont = workbook.createFont();
      boldFont.setBold(true);
      boldFont.setFontHeightInPoints((short) 8);

      Font titleFont = workbook.createFont();
      titleFont.setBold(true);
      titleFont.setFontHeightInPoints((short) 16);

      Font hiddenFileNameFont = workbook.createFont();
      hiddenFileNameFont.setColor(HSSFColorPredefined.WHITE.getIndex());

      titleStyle = workbook.createCellStyle();
      titleStyle.setFont(titleFont);

      hiddenFileNameStyle = workbook.createCellStyle();
      hiddenFileNameStyle.setFont(hiddenFileNameFont);

      copyrightStyle = workbook.createCellStyle();
      copyrightStyle.setAlignment(HorizontalAlignment.RIGHT);

      standardFormat = workbook.createCellStyle();
      standardFormat.setVerticalAlignment(VerticalAlignment.TOP);
      standardFormat.setFont(standardFont);

      wrappedFormat = workbook.createCellStyle();
      wrappedFormat.cloneStyleFrom(standardFormat);
      wrappedFormat.setWrapText(true);

      boldFormat = workbook.createCellStyle();
      boldFormat.setVerticalAlignment(VerticalAlignment.TOP);
      boldFormat.setFont(boldFont);

      boldHeadingFormat = workbook.createCellStyle();
      boldHeadingFormat.cloneStyleFrom(boldFormat);
      boldHeadingFormat.setBorderBottom(BorderStyle.MEDIUM);
      boldHeadingFormat.setVerticalAlignment(VerticalAlignment.CENTER);
      boldHeadingFormat.setFillForegroundColor(HSSFColorPredefined.GREY_25_PERCENT.getIndex());
      boldHeadingFormat.setFillPattern(FillPatternType.SOLID_FOREGROUND);
    }
  }

  private static class RowLimitExceededException extends RuntimeException {
    RowLimitExceededException(String message) {
      super(message);
    }
  }
}
