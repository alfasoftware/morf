package org.alfasoftware.morf.upgrade;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.alfasoftware.morf.upgrade.SchemaChangeAdaptor.Combining;
import org.junit.Test;

public class TestSchemaChangeAdaptor {

  @Test
  public void testCombiningAddColumn() {
    AddColumn change1 = mock(AddColumn.class);
    AddColumn change2 = mock(AddColumn.class);
    AddColumn change3 = mock(AddColumn.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningAddTable() {
    AddTable change1 = mock(AddTable.class);
    AddTable change2 = mock(AddTable.class);
    AddTable change3 = mock(AddTable.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningRemoveTable() {
    RemoveTable change1 = mock(RemoveTable.class);
    RemoveTable change2 = mock(RemoveTable.class);
    RemoveTable change3 = mock(RemoveTable.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningAddIndex() {
    AddIndex change1 = mock(AddIndex.class);
    AddIndex change2 = mock(AddIndex.class);
    AddIndex change3 = mock(AddIndex.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningChangeColumn() {
    ChangeColumn change1 = mock(ChangeColumn.class);
    ChangeColumn change2 = mock(ChangeColumn.class);
    ChangeColumn change3 = mock(ChangeColumn.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningRemoveColumn() {
    RemoveColumn change1 = mock(RemoveColumn.class);
    RemoveColumn change2 = mock(RemoveColumn.class);
    RemoveColumn change3 = mock(RemoveColumn.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningRemoveIndex() {
    RemoveIndex change1 = mock(RemoveIndex.class);
    RemoveIndex change2 = mock(RemoveIndex.class);
    RemoveIndex change3 = mock(RemoveIndex.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningChangeIndex() {
    ChangeIndex change1 = mock(ChangeIndex.class);
    ChangeIndex change2 = mock(ChangeIndex.class);
    ChangeIndex change3 = mock(ChangeIndex.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningRenameIndex() {
    RenameIndex change1 = mock(RenameIndex.class);
    RenameIndex change2 = mock(RenameIndex.class);
    RenameIndex change3 = mock(RenameIndex.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningExecuteStatement() {
    ExecuteStatement change1 = mock(ExecuteStatement.class);
    ExecuteStatement change2 = mock(ExecuteStatement.class);
    ExecuteStatement change3 = mock(ExecuteStatement.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningRenameTable() {
    RenameTable change1 = mock(RenameTable.class);
    RenameTable change2 = mock(RenameTable.class);
    RenameTable change3 = mock(RenameTable.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningChangePrimaryKeyColumns() {
    ChangePrimaryKeyColumns change1 = mock(ChangePrimaryKeyColumns.class);
    ChangePrimaryKeyColumns change2 = mock(ChangePrimaryKeyColumns.class);
    ChangePrimaryKeyColumns change3 = mock(ChangePrimaryKeyColumns.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningAddTableFrom() {
    AddTableFrom change1 = mock(AddTableFrom.class);
    AddTableFrom change2 = mock(AddTableFrom.class);
    AddTableFrom change3 = mock(AddTableFrom.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningAnalyseTable() {
    AnalyseTable change1 = mock(AnalyseTable.class);
    AnalyseTable change2 = mock(AnalyseTable.class);
    AnalyseTable change3 = mock(AnalyseTable.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningAddSequence() {
    AddSequence change1 = mock(AddSequence.class);
    AddSequence change2 = mock(AddSequence.class);
    AddSequence change3 = mock(AddSequence.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }


  @Test
  public void testCombiningRemoveSequence() {
    RemoveSequence change1 = mock(RemoveSequence.class);
    RemoveSequence change2 = mock(RemoveSequence.class);
    RemoveSequence change3 = mock(RemoveSequence.class);

    SchemaChangeAdaptor first = mock(SchemaChangeAdaptor.class);
    when(first.adapt(change1)).thenReturn(change2);

    SchemaChangeAdaptor second = mock(SchemaChangeAdaptor.class);
    when(second.adapt(change2)).thenReturn(change3);

    Combining combined = new SchemaChangeAdaptor.Combining(first, second);
    assertEquals(change3, combined.adapt(change1));
  }
}
