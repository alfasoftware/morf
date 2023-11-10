package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlUtils.selectFirst;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Criterion;
import org.alfasoftware.morf.upgrade.UpgradeTableResolutionVisitor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests of {@link SelectFirstStatement}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestSelectFirstStatement {

  @Mock
  private AliasedField field;

  @Mock
  private SelectStatement fromSelect, subSelect;

  @Mock
  private Criterion onCondition, criterion2;

  @Mock
  private UpgradeTableResolutionVisitor res;


  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
    when(field.build()).thenReturn(field);
  }


  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    SelectFirstStatement sel1 = selectFirst(field)
        .from(fromSelect)
        .innerJoin(subSelect, onCondition)
        .where(criterion2);

    //when
    sel1.accept(res);

    //then
    verify(res).visit(sel1);
    verify(field).accept(res);
    verify(fromSelect).accept(res);
    verify(subSelect).accept(res);
    verify(onCondition).accept(res);
    verify(criterion2).accept(res);
  }
}

