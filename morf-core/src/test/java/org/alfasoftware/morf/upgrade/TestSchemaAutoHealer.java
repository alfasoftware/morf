package org.alfasoftware.morf.upgrade;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.upgrade.SchemaAutoHealer.Combining;
import org.alfasoftware.morf.upgrade.SchemaAutoHealer.SchemaHealingResults;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public class TestSchemaAutoHealer {

  @Test
  public void testCombiningSchemaAutoHealer() {

    SqlDialect dialect = mock(SqlDialect.class);

    Schema schema1 = mock(Schema.class);
    Schema schema2 = mock(Schema.class);
    Schema schema3 = mock(Schema.class);

    SchemaAutoHealer first = mock(SchemaAutoHealer.class, Mockito.RETURNS_DEEP_STUBS);
    when(first.analyseSchema(schema1).getHealedSchema()).thenReturn(schema2);
    when(first.analyseSchema(schema1).getHealingStatements(dialect)).thenReturn(ImmutableList.of("SQL1", "SQL2", "SQL3"));

    SchemaAutoHealer second = mock(SchemaAutoHealer.class, Mockito.RETURNS_DEEP_STUBS);
    when(second.analyseSchema(schema2).getHealedSchema()).thenReturn(schema3);
    when(second.analyseSchema(schema2).getHealingStatements(dialect)).thenReturn(ImmutableList.of("SQL4", "SQL5", "SQL5"));

    Combining combined = new SchemaAutoHealer.Combining(first, second);
    SchemaHealingResults results = combined.analyseSchema(schema1);

    assertEquals(schema3, results.getHealedSchema());
    assertEquals(ImmutableList.of("SQL1", "SQL2", "SQL3", "SQL4", "SQL5", "SQL5"), results.getHealingStatements(dialect));
  }
}
