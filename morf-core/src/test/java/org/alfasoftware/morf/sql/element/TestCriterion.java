package org.alfasoftware.morf.sql.element;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.upgrade.UpgradeTableResolutionVisitor;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests of {@link Criterion}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestCriterion {

  @Mock
  private AliasedField field, field2;

  @Mock
  private SelectStatement select;

  @Mock
  private Criterion criterion1;

  @Mock
  private UpgradeTableResolutionVisitor res;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    when(select.getFields()).thenReturn(Arrays.asList(field2));
    Criterion c = Criterion.and(criterion1, Criterion.in(field, select));

    //when
    c.accept(res);

    //then
    verify(res).visit(c);
    verify(field).accept(res);
    verify(select).accept(res);
    verify(criterion1).accept(res);
  }
}

