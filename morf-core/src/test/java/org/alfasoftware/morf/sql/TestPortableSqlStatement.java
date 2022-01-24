package org.alfasoftware.morf.sql;

import static org.mockito.Mockito.verify;

import org.alfasoftware.morf.upgrade.PortableSqlStatement;
import org.alfasoftware.morf.upgrade.UpgradeTableResolutionVisitor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests of {@link PortableSqlStatement}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestPortableSqlStatement {

  @Mock
  private UpgradeTableResolutionVisitor res;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    PortableSqlStatement statement = new PortableSqlStatement();

    //when
    statement.accept(res);

    //then
    verify(res).visit(statement);
  }
}

