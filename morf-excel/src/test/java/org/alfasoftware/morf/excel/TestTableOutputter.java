package org.alfasoftware.morf.excel;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Table;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import com.google.common.collect.ImmutableList;
import jxl.Cell;
import jxl.write.WritableCell;
import jxl.write.WritableSheet;
import jxl.write.WritableWorkbook;
import jxl.write.WriteException;

/**
 * Ensure that {@link TableOutputter} works correctly.
 *
 * @author Copyright (c) Alfa Financial Software 2023
 */
public class TestTableOutputter {

  private Table table;
  private TableOutputter tableOutputter;
  private WritableSheet writableSheet;
  private WritableWorkbook writableWorkbook;


  /**
   * Set up the test.
   */
  @Before
  public void setUp() {
    table = mock(Table.class);
    when(table.getName()).thenReturn("TestTable");
    when(table.columns()).thenReturn(ImmutableList.of(
        column("Col1", DataType.STRING),
        column("Col2", DataType.DECIMAL),
        column("Col3", DataType.BIG_INTEGER),
        column("Col4", DataType.INTEGER),
        column("Col5", DataType.CLOB)
    ));

    writableWorkbook = mock(WritableWorkbook.class);
    writableSheet = mock(WritableSheet.class);
    when(writableSheet.getName()).thenReturn("Sheet1");
    when(writableSheet.getWritableCell(anyInt(), anyInt())).thenReturn(mock(WritableCell.class));
    when(writableSheet.getCell(anyInt(), anyInt())).thenReturn(mock(Cell.class));
    when(writableWorkbook.createSheet(any(String.class), any(Integer.class))).thenReturn(writableSheet);

    AdditionalSchemaData additionalSchemaData = mock(AdditionalSchemaData.class);
    tableOutputter = new TableOutputter(additionalSchemaData);
  }


  /**
   * Tests that a table with fields containing different data types is written correctly to the worksheet.
   *
   * @throws {@link WriteException} if an error occurs
   */
  @Test
  public void testTableOutput() throws WriteException {
    // Given
    Record record1 = mock(Record.class);
    when(record1.getString("Col1")).thenReturn("STRING VALUE");
    when(record1.getBigDecimal("Col2")).thenReturn(new BigDecimal(12.34));
    when(record1.getLong("Col3")).thenReturn(22L);
    when(record1.getLong("Col4")).thenReturn(33L);
    when(record1.getString("Col5")).thenReturn("CLOB VALUE");
    ImmutableList<Record> recordList = ImmutableList.<Record>of(record1);

    // When
    tableOutputter.table(1, writableWorkbook, table, recordList);

    ArgumentCaptor<WritableCell> writableCellCaptor = ArgumentCaptor.forClass(WritableCell.class);
    verify(writableSheet, times(36)).addCell(writableCellCaptor.capture());
    List<WritableCell> capturedWritableCellList = writableCellCaptor.getAllValues();
    // Example Data
    assertTrue(isCapturedWritableCellListContains(capturedWritableCellList,12, 0, "STRING VALUE"));
    assertTrue(isCapturedWritableCellListContains(capturedWritableCellList,12, 1, BigDecimal.valueOf(12.34).toString()));
    assertTrue(isCapturedWritableCellListContains(capturedWritableCellList,12, 2, Long.valueOf(22).toString()));
    assertTrue(isCapturedWritableCellListContains(capturedWritableCellList,12, 3, Long.valueOf(33).toString()));
    assertTrue(isCapturedWritableCellListContains(capturedWritableCellList,12, 4, "CLOB VALUE"));

    // Parameters to Set Up
    assertTrue(isCapturedWritableCellListContains(capturedWritableCellList,16, 0, "STRING VALUE"));
    assertTrue(isCapturedWritableCellListContains(capturedWritableCellList,16, 1, BigDecimal.valueOf(12.34).toString()));
    assertTrue(isCapturedWritableCellListContains(capturedWritableCellList,16, 2, Long.valueOf(22).toString()));
    assertTrue(isCapturedWritableCellListContains(capturedWritableCellList,16, 3, Long.valueOf(33).toString()));
    assertTrue(isCapturedWritableCellListContains(capturedWritableCellList,16, 4, "CLOB VALUE"));
  }


  /**
   * Tests that columns are truncated correctly.
   *
   * @throws {@link WriteException} if an error occurs
   */
  @Test
  public void testColumnTruncation() throws WriteException {
    // Given
    Record record1 = mock(Record.class);
    List<Column> columnList = new ArrayList<>();

    for (int c = 1; c <= 257; c++) {
      columnList.add(column("Col" + c, DataType.STRING));
      when(record1.getString("Col") + c).thenReturn("Value" + c);
    }

    when(table.columns()).thenReturn(columnList);
    ImmutableList<Record> recordList = ImmutableList.<Record>of(record1);

    // When
    tableOutputter.table(1, writableWorkbook, table, recordList);

    // Then
    ArgumentCaptor<WritableCell> writableCellCaptor = ArgumentCaptor.forClass(WritableCell.class);
    verify(writableSheet, times(1547)).addCell(writableCellCaptor.capture());
    List<WritableCell> capturedWritableCellList = writableCellCaptor.getAllValues();
    assertTrue(isCapturedWritableCellListContains(capturedWritableCellList,260, 13, "[TRUNCATED]"));
  }



  /**
   * Tests that rows are truncated correctly.
   *
   * @throws {@link WriteException} if an error occurs
   */
  @Test
  public void testRowTruncation() throws WriteException {
    // Given
    Record record1 = mock(Record.class);
    when(table.columns()).thenReturn(ImmutableList.of(column("Col1", DataType.STRING)));
    List<Record> recordList = new ArrayList<>();

    for (int r = 1; r < 65537; r++) {
      recordList.add(mock(Record.class));
    }

    // When
    tableOutputter.table(1, writableWorkbook, table, recordList);

    // Then
    ArgumentCaptor<WritableCell> writableCellCaptor = ArgumentCaptor.forClass(WritableCell.class);
    verify(writableSheet, times(65535)).addCell(writableCellCaptor.capture());
    List<WritableCell> capturedWritableCellList = writableCellCaptor.getAllValues();

    for (int i = 0; i < capturedWritableCellList.size(); i++) {
      if (capturedWritableCellList.get(i).getContents().contains("TRUNC")) {
        System.out.println("x");
      }
    }

    assertTrue(isCapturedWritableCellListContains(capturedWritableCellList,0, 0, "Sheet1 [ROWS TRUNCATED]"));
  }


  /**
   * Tests whether a given {@link WritableCell} {@link List} contains an entry with a given row, column and value.
   *
   * @param capturedWritableCellList the {@link WritableCell} {@link List}
   * @param row the row
   * @param col the column
   * @param value the value
   * @return true if capturedWritableCellList contains the entry, otherwise false
   */
  private boolean isCapturedWritableCellListContains(List<WritableCell> capturedWritableCellList, int row, int col, String value) {
    return capturedWritableCellList.stream()
      .filter(writableCell -> writableCell.getRow() == row &&
          writableCell.getColumn() == col &&
          writableCell.getContents().equals(value))
      .count() == 1;
  }
}
