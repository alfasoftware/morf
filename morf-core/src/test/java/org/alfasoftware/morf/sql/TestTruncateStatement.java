package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.truncate;
import static org.mockito.Mockito.verify;

import org.alfasoftware.morf.upgrade.UpgradeTableResolutionVisitor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests of {@link TruncateStatement}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestTruncateStatement {

  @Mock
  private UpgradeTableResolutionVisitor res;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    TruncateStatement trun = truncate(tableRef("table1"));

    //when
    trun.accept(res);

    //then
    verify(res).visit(trun);
  }
}

