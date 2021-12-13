package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.update;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.hamcrest.Matchers;
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
    ResolvedTables res = new ResolvedTables();

    //when
    up1.resolveTables(res);

    //then
    verify(field).resolveTables(res);
    verify(crit).resolveTables(res);
    assertThat(res.getModifiedTables(), Matchers.contains("TABLE1"));
  }
}

