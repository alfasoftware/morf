package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlUtils.delete;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;

import org.alfasoftware.morf.sql.element.Criterion;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests of {@link DeleteStatement}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestDeleteStatement {

  @Mock
  private Criterion criterion1;


  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void tableResolutionDetectsAllTables() {
    //given
    DeleteStatement del1 = delete(tableRef("table1")).where(criterion1);
    ResolvedTables res = new ResolvedTables();

    //when
    del1.resolveTables(res);

    //then
    verify(criterion1).resolveTables(res);
    assertThat(res.getModifiedTables(), Matchers.contains("TABLE1"));
  }
}

