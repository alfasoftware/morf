package org.alfasoftware.morf.excel;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TestTableOutputter {

  private Table table;
  private TableOutputter tableOutputter;
  private Workbook workbook;
  private Sheet sheet;
  private final DataFormatter formatter = new DataFormatter();

  @Before
  public void setUp() {
    table = mock(Table.class);
    when(table.getName()).thenReturn("TestTable");
    when(table.columns()).thenReturn(ImmutableList.of(
        column("Col1", DataType.STRING),
        column("Col2", DataType.DECIMAL),
        column("Col3", DataType.BIG_INTEGER),
        column("Col4", DataType.INTEGER),
        column("Col5", DataType.CLOB)));

    workbook = new HSSFWorkbook();
    AdditionalSchemaData additionalSchemaData = mock(AdditionalSchemaData.class);
    when(additionalSchemaData.columnDocumentation(table, "Col1")).thenReturn("");
    when(additionalSchemaData.columnDocumentation(table, "Col2")).thenReturn("");
    when(additionalSchemaData.columnDocumentation(table, "Col3")).thenReturn("");
    when(additionalSchemaData.columnDocumentation(table, "Col4")).thenReturn("");
    when(additionalSchemaData.columnDocumentation(table, "Col5")).thenReturn("");
    when(additionalSchemaData.columnDefaultValue(table, "Col1")).thenReturn("");
    when(additionalSchemaData.columnDefaultValue(table, "Col2")).thenReturn("");
    when(additionalSchemaData.columnDefaultValue(table, "Col3")).thenReturn("");
    when(additionalSchemaData.columnDefaultValue(table, "Col4")).thenReturn("");
    when(additionalSchemaData.columnDefaultValue(table, "Col5")).thenReturn("");
    tableOutputter = new TableOutputter(additionalSchemaData);
  }

  @Test
  public void testTableOutputWithFieldsPopulated() {
    Record record1 = mock(Record.class);
    when(record1.getString("Col1")).thenReturn("STRING VALUE");
    when(record1.getBigDecimal("Col2")).thenReturn(BigDecimal.valueOf(12.34));
    when(record1.getLong("Col3")).thenReturn(22L);
    when(record1.getLong("Col4")).thenReturn(33L);
    when(record1.getString("Col5")).thenReturn("CLOB VALUE");

    tableOutputter.table(1, workbook, table, ImmutableList.of(record1));
    sheet = workbook.getSheetAt(0);

    assertEquals("STRING VALUE", value(12, 0));
    assertEquals("12.34", value(12, 1));
    assertEquals("22", value(12, 2));
    assertEquals("33", value(12, 3));
    assertEquals("CLOB VALUE", value(12, 4));

    assertEquals("STRING VALUE", value(16, 0));
    assertEquals("12.34", value(16, 1));
    assertEquals("22", value(16, 2));
    assertEquals("33", value(16, 3));
    assertEquals("CLOB VALUE", value(16, 4));
  }

  @Test
  public void testTableOutputWithFieldsUnpopulated() {
    Record record1 = mock(Record.class);

    tableOutputter.table(1, workbook, table, ImmutableList.of(record1));
    sheet = workbook.getSheetAt(0);

    assertEquals("", value(12, 0));
    assertEquals("", value(12, 1));
    assertEquals("0", value(12, 2));
    assertEquals("0", value(12, 3));
    assertEquals("", value(12, 4));

    assertEquals("", value(16, 0));
    assertEquals("", value(16, 1));
    assertEquals("0", value(16, 2));
    assertEquals("0", value(16, 3));
    assertEquals("", value(16, 4));
  }

  @Test
  public void testColumnTruncation() {
    Record record1 = mock(Record.class);
    List<Column> columnList = new ArrayList<>();
    for (int c = 1; c <= 257; c++) {
      columnList.add(column("Col" + c, DataType.STRING));
      when(record1.getString("Col" + c)).thenReturn("Value" + c);
    }
    when(table.columns()).thenReturn(columnList);

    tableOutputter.table(1, workbook, table, ImmutableList.of(record1));
    sheet = workbook.getSheetAt(0);

    assertEquals("[TRUNCATED]", value(12, 256));
  }

  @Test
  public void testRowTruncation() {
    when(table.columns()).thenReturn(ImmutableList.of(column("Col1", DataType.STRING)));
    List<Record> recordList = new ArrayList<>();
    for (int r = 1; r < 65537; r++) {
      recordList.add(mock(Record.class));
    }

    tableOutputter.table(1, workbook, table, recordList);
    sheet = workbook.getSheetAt(0);

    assertTrue(value(0, 0).contains("ROWS TRUNCATED"));
  }

  @Test
  public void testClobTruncation() {
    when(table.columns()).thenReturn(ImmutableList.of(column("Col1", DataType.CLOB)));
    Record record1 = mock(Record.class);
      when(record1.getString("Col1")).thenReturn("X".repeat(35000));

    tableOutputter.table(1, workbook, table, ImmutableList.of(record1));
    sheet = workbook.getSheetAt(0);

    assertEquals(32767, value(12, 0).length());
    assertEquals(32767, value(16, 0).length());
  }

  @Test
  public void testUnsupportedOperationExceptionHandling() {
    Record record1 = mock(Record.class);

    when(table.columns()).thenReturn(ImmutableList.of(column("Col1", DataType.DECIMAL)));
    BigDecimal bigDecimal = mock(BigDecimal.class);
    when(record1.getBigDecimal("Col1")).thenReturn(bigDecimal);
    when(bigDecimal.toString()).thenThrow(new RuntimeException("BAD BIG DECIMAL"));

    try {
      tableOutputter.table(1, workbook, table, ImmutableList.of(record1));
      fail("UnsupportedOperationException should be thrown");
    } catch (Exception e) {
      assertTrue(e.getCause().getMessage().startsWith("Cannot generate Excel cell for data"));
    }

    when(table.columns()).thenReturn(ImmutableList.of(column("Col1", DataType.CLOB)));
    when(record1.getString("Col1")).thenThrow(new RuntimeException("BAD CLOB"));

    try {
      tableOutputter.table(1, workbook, table, ImmutableList.of(record1));
      fail("UnsupportedOperationException should be thrown");
    } catch (Exception e) {
      assertTrue(e.getCause().getMessage().startsWith("Cannot generate Excel cell for CLOB data"));
    }
  }

  private String value(int rowIndex, int colIndex) {
    if (sheet.getRow(rowIndex) == null || sheet.getRow(rowIndex).getCell(colIndex) == null) {
      return "";
    }
    return formatter.formatCellValue(sheet.getRow(rowIndex).getCell(colIndex));
  }
}
