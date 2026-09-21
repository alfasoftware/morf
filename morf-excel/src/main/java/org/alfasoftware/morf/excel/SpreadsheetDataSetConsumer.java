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
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.HorizontalAlignment;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.ss.util.WorkbookUtil;

/**
 * Consumes data sets and outputs spreadsheets.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class SpreadsheetDataSetConsumer implements DataSetConsumer {

  private static final Log log = LogFactory.getLog(SpreadsheetDataSetConsumer.class);

  private static final int NUMBER_OF_ROWS_IN_TITLE = 2;
  private static final int DEFAULT_SAMPLE_ROWS = 5;

  private final OutputStream documentOutputStream;
  private Workbook workbook;
  private ConsumerStyles consumerStyles;
  private final Optional<Map<String, Integer>> rowsPerTable;
  private final TableOutputter tableOutputter;

  public SpreadsheetDataSetConsumer(OutputStream documentOutputStream) {
    this(documentOutputStream, Optional.<Map<String, Integer>>empty());
  }

  public SpreadsheetDataSetConsumer(OutputStream documentOutputStream, Optional<Map<String, Integer>> rowsPerTable) {
    this(documentOutputStream, rowsPerTable, new DefaultAdditionalSchemaDataImpl());
  }

  public SpreadsheetDataSetConsumer(OutputStream documentOutputStream, Optional<Map<String, Integer>> rowsPerTable,
      AdditionalSchemaData additionalSchemaData) {
    this(documentOutputStream, rowsPerTable, new TableOutputter(additionalSchemaData));
  }

  SpreadsheetDataSetConsumer(OutputStream documentOutputStream, Optional<Map<String, Integer>> rowsPerTable,
      TableOutputter tableOutputter) {
    this.documentOutputStream = documentOutputStream;
    this.tableOutputter = tableOutputter;
    this.rowsPerTable = rowsPerTable;
  }

  @Override
  public void open() {
    workbook = new HSSFWorkbook();
    consumerStyles = new ConsumerStyles(workbook);
  }

  public void createIndex() {
    Sheet sheet = workbook.createSheet(safeSheetName(spreadsheetifyName("Index")));
    workbook.setSheetOrder(sheet.getSheetName(), 0);
    createTitle(sheet, "Index");

    try {
      for (int currentSheet = 1; currentSheet < workbook.getNumberOfSheets(); currentSheet++) {
        Sheet tableSheet = workbook.getSheetAt(currentSheet);
        int rowIndex = currentSheet - 1 + NUMBER_OF_ROWS_IN_TITLE;

        Row row = getOrCreateRow(sheet, rowIndex);
        Cell linkCell = row.createCell(0);
        linkCell.setCellValue(tableSheet.getSheetName());
        linkCell.setHyperlink(workbook.getCreationHelper().createHyperlink(HyperlinkType.DOCUMENT));
        linkCell.getHyperlink().setAddress("'" + tableSheet.getSheetName() + "'!A1");
        linkCell.setCellStyle(consumerStyles.hyperlinkStyle);
        String fileName = getCellString(tableSheet, 1, 1);
        Cell fileNameCell = row.createCell(1);
        fileNameCell.setCellValue(fileName);
        fileNameCell.setCellStyle(indexFileNameStyle());

        Row backLinkRow = getOrCreateRow(tableSheet, 1);
        Cell backLinkCell = backLinkRow.createCell(0);
        backLinkCell.setCellValue("Back to index");
        backLinkCell.setHyperlink(workbook.getCreationHelper().createHyperlink(HyperlinkType.DOCUMENT));
        backLinkCell.getHyperlink().setAddress("'" + sheet.getSheetName() + "'!A" + (rowIndex + 1));
        backLinkCell.setCellStyle(consumerStyles.hyperlinkStyle);
        tableSheet.setColumnWidth(0, 13 * 256);
      }

      sheet.setColumnWidth(0, 35 * 256);
      sheet.setColumnWidth(1, 1);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private CellStyle indexFileNameStyle() {
    return consumerStyles.indexFileNameStyle;
  }

  private String spreadsheetifyName(String name) {
    return StringUtils.capitalize(name).replaceAll("([A-Z])", " $1").trim();
  }

  protected void createTitle(Sheet sheet, String title) {
    try {
      Cell cell = getOrCreateRow(sheet, 0).createCell(0);
      cell.setCellValue(title);
      cell.setCellStyle(consumerStyles.headingStyle);

      Cell copyrightCell = getOrCreateRow(sheet, 0).createCell(12);
      copyrightCell.setCellValue("Copyright " + new SimpleDateFormat("yyyy").format(new Date()) + " Alfa Financial Software Ltd.");
      copyrightCell.setCellStyle(consumerStyles.copyrightStyle);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close(CloseState closeState) {
    try {
      createIndex();
      workbook.write(documentOutputStream);
      workbook.close();
    } catch (IOException e) {
      throw new RuntimeException("Error closing writable workbook", e);
    }
  }

  @Override
  public void table(Table table, Iterable<Record> records) {
    Integer maxSampleRows = DEFAULT_SAMPLE_ROWS;
    if (rowsPerTable.isPresent()) {
      maxSampleRows = rowsPerTable.get().get(table.getName().toUpperCase());
    }

    if (maxSampleRows == null) {
      log.info("File [" + table.getName() + "] excluded in configuration.");
    } else if (tableOutputter.tableHasUnsupportedColumns(table)) {
      log.info("File [" + table.getName() + "] skipped - unsupported columns.");
    } else {
      log.info("File [" + table.getName() + "] generating...");
      tableOutputter.table(maxSampleRows, workbook, table, records);
    }
  }

  private Row getOrCreateRow(Sheet sheet, int rowIndex) {
    Row row = sheet.getRow(rowIndex);
    return row == null ? sheet.createRow(rowIndex) : row;
  }

  private String getCellString(Sheet sheet, int columnIndex, int rowIndex) {
    Row row = sheet.getRow(rowIndex);
    if (row == null) {
      return "";
    }
    Cell cell = row.getCell(columnIndex);
    return cell == null ? "" : cell.toString();
  }

  private String safeSheetName(String name) {
    return WorkbookUtil.createSafeSheetName(name);
  }

  private static final class ConsumerStyles {
    private final CellStyle indexFileNameStyle;
    private final CellStyle headingStyle;
    private final CellStyle copyrightStyle;

    private final CellStyle hyperlinkStyle;

    private ConsumerStyles(Workbook workbook) {
      Font normalFont = workbook.createFont();
      normalFont.setColor(Font.COLOR_NORMAL);

      Font headingFont = workbook.createFont();
      headingFont.setBold(true);
      headingFont.setFontHeightInPoints((short) 16);

      Font hyperlinkFont = workbook.createFont();
      hyperlinkFont.setUnderline(Font.U_SINGLE);
      hyperlinkFont.setColor(IndexedColors.BLUE.getIndex());
      hyperlinkFont.setFontHeightInPoints((short) 8);


      indexFileNameStyle = workbook.createCellStyle();
      indexFileNameStyle.setFont(normalFont);

      headingStyle = workbook.createCellStyle();
      headingStyle.setFont(headingFont);

      copyrightStyle = workbook.createCellStyle();
      copyrightStyle.setAlignment(HorizontalAlignment.RIGHT);

      hyperlinkStyle = workbook.createCellStyle();
      hyperlinkStyle.setFont(hyperlinkFont);
    }
  }
}
