package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlUtils.field;
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
 * Unit tests of {@link MergeStatement}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestMergeStatement {

  @Mock
  private AliasedField field;

  @Mock
  private SelectStatement fromSelect;

  @Mock
  private UpgradeTableResolutionVisitor res;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    when(field.build()).thenReturn(field);
  }


  @Test
  public void tableResolutionDetectsAllTables() {
    //given
    MergeStatement mer1 = MergeStatement.merge()
        .into(tableRef("table1"))
        .tableUniqueKey(field("id"))
        .from(fromSelect)
        .ifUpdating((overrides, values) -> overrides
          .set(field))
        .build();

    //when
    mer1.accept(res);

    //then
    verify(res).visit(mer1);
    verify(field).accept(res);
    verify(fromSelect).accept(res);
  }
}

