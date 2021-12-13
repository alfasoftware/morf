package org.alfasoftware.morf.sql;

import static org.junit.Assert.assertTrue;

import org.alfasoftware.morf.upgrade.PortableSqlStatement;
import org.junit.Test;

/**
 * Unit tests of {@link PortableSqlStatement}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestPortableSqlStatement {

  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    PortableSqlStatement statement = new PortableSqlStatement();
    ResolvedTables res = new ResolvedTables();

    //when
    statement.resolveTables(res);

    //then
    assertTrue(res.isPortableSqlStatementUsed());
  }
}

