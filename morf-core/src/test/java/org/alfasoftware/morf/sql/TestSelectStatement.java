package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlUtils.select;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.upgrade.UpgradeTableResolutionVisitor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests of {@link SelectStatement}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestSelectStatement {

  @Mock
  private AliasedField field, field2;

  @Mock
  private SelectStatement fromSelect, subSelect, unionStatement;

  @Mock
  private Criterion criterion1, onCondition, criterion2;

  @Mock
  private UpgradeTableResolutionVisitor res;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    when(unionStatement.getFields()).thenReturn(Arrays.asList(field2));

    SelectStatement sel1 = select(field)
        .from(fromSelect)
        .innerJoin(subSelect, onCondition)
        .where(criterion2)
        .having(criterion1)
        .union(unionStatement);

    //when
    sel1.accept(res);

    //then
    verify(res).visit(sel1);
    verify(field).accept(res);
    verify(fromSelect).accept(res);
    verify(subSelect).accept(res);
    verify(onCondition).accept(res);
    verify(criterion2).accept(res);
    verify(criterion1).accept(res);
    verify(unionStatement).accept(res);
  }
}

