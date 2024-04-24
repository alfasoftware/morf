package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.upgrade.UpgradeTableResolutionVisitor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests of {@link InsertStatement}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestInsertStatement {

  @Mock
  private AliasedField field, field2, field3;

  @Mock
  private SelectStatement fromSelect;

  @Mock
  private UpgradeTableResolutionVisitor res;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    when(field.build()).thenReturn(field);
    when(field2.build()).thenReturn(field2);
    when(field3.build()).thenReturn(field3);
  }


  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    when(field3.getAlias()).thenReturn("whatever");

    InsertStatement ins1 = insert().into(tableRef("table1"))
        .fields(field)
        .from(fromSelect)
        .withDefaults(field3);

    //when
    ins1.accept(res);

    //then
    verify(res).visit(ins1);
    verify(field).accept(res);
    verify(field3).accept(res);
    verify(fromSelect).accept(res);
  }


  @Test
  public void tableResolutionDetectsAllTables2() {
    //given
    InsertStatement ins1 = insert().into(tableRef("table1"))
        .fields(field)
        .values(field2);

    //when
    ins1.accept(res);

    //then
    verify(res).visit(ins1);
    verify(field).accept(res);
    verify(field2).accept(res);
  }
}

