package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.mockito.Mockito.verify;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.upgrade.UpgradeTableResolutionVisitor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests of {@link UpdateStatement}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestUpdateStatement {

  @Mock
  private AliasedField field;

  @Mock
  private Criterion crit;

  @Mock
  private UpgradeTableResolutionVisitor res;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    UpdateStatement up1 = update(tableRef("table1"))
        .set(field)
        .where(crit);

    //when
    up1.accept(res);

    //then
    verify(res).visit(up1);
    verify(field).accept(res);
    verify(crit).accept(res);
  }
}

