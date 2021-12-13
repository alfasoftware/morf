package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.SqlUtils.truncate;
import static org.hamcrest.MatcherAssert.assertThat;

import org.hamcrest.Matchers;
import org.junit.Test;

/**
 * Unit tests of {@link TruncateStatement}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestTruncateStatement {

  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    TruncateStatement trun = truncate(tableRef("table1"));
    ResolvedTables res = new ResolvedTables();

    //when
    trun.resolveTables(res);

    //then
    assertThat(res.getModifiedTables(), Matchers.contains("TABLE1"));
  }
}

