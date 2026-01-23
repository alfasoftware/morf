package org.alfasoftware.morf.upgrade;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;

import org.alfasoftware.morf.metadata.Schema;
import org.hamcrest.Matchers;
import org.junit.Test;

public class TestSchemaChangeToSchemaAdaptor {

  @Test
  public void testCombining1() {
    Schema schema = mock(Schema.class);
    AddColumn change1 = mock(AddColumn.class);
    AddColumn change2a = mock(AddColumn.class);
    AddColumn change2b = mock(AddColumn.class);
    AddColumn change3aa = mock(AddColumn.class);
    AddColumn change3ab = mock(AddColumn.class);
    AddColumn change3ba = mock(AddColumn.class);
    AddColumn change3bb = mock(AddColumn.class);

    SchemaChangeToSchemaAdaptor first = mock(SchemaChangeToSchemaAdaptor.class);
    when(first.adapt(change1, schema)).thenReturn(List.of(change2a, change2b));

    SchemaChangeToSchemaAdaptor second = mock(SchemaChangeToSchemaAdaptor.class);
    when(second.adapt(change2a, schema)).thenReturn(List.of(change3aa, change3ab));
    when(second.adapt(change2b, schema)).thenReturn(List.of(change3ba, change3bb));

    SchemaChangeToSchemaAdaptor.Combining combined = new SchemaChangeToSchemaAdaptor.Combining(first, second);
    assertThat(combined.adapt(change1, schema), Matchers.equalTo(
        List.of(change3aa, change3ab, change3ba, change3bb)));
  }

  @Test
  public void testCombining2() {
    Schema schema = mock(Schema.class);
    AddColumn change1 = mock(AddColumn.class);
    AddColumn change2a = mock(AddColumn.class);
    AddColumn change2b = mock(AddColumn.class);
    AddColumn change2c = mock(AddColumn.class);
    AddColumn change3aa = mock(AddColumn.class);
    AddColumn change3ab = mock(AddColumn.class);
    AddColumn change3ba = mock(AddColumn.class);
    AddColumn change3bb = mock(AddColumn.class);
    AddColumn change3bc = mock(AddColumn.class);

    SchemaChangeToSchemaAdaptor first = mock(SchemaChangeToSchemaAdaptor.class);
    when(first.adapt(change1, schema)).thenReturn(List.of(change2a, change2b, change2c));

    SchemaChangeToSchemaAdaptor second = mock(SchemaChangeToSchemaAdaptor.class);
    when(second.adapt(change2a, schema)).thenReturn(List.of(change3aa, change3ab));
    when(second.adapt(change2b, schema)).thenReturn(List.of(change3ba, change3bb, change3bc));
    when(second.adapt(change2c, schema)).thenReturn(List.of());

    SchemaChangeToSchemaAdaptor.Combining combined = new SchemaChangeToSchemaAdaptor.Combining(first, second);
    assertThat(combined.adapt(change1, schema), Matchers.equalTo(
        List.of(change3aa, change3ab, change3ba, change3bb, change3bc)));
  }
}
