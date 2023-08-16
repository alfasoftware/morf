package org.alfasoftware.morf.sql.element;

import static org.mockito.Mockito.verify;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.upgrade.UpgradeTableResolutionVisitor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * All tests of {@link SqlParameter}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
public class TestSqlParameter {

  @Mock
  private UpgradeTableResolutionVisitor res;

  @Mock
  private Column col;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    SqlParameter sqlParameter = new SqlParameter(col);

    //when
    sqlParameter.accept(res);

    //then
    verify(res).visit(sqlParameter);
  }
}

