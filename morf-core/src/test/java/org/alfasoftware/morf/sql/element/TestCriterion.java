package org.alfasoftware.morf.sql.element;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Arrays;

import org.alfasoftware.morf.sql.ResolvedTables;
import org.alfasoftware.morf.sql.SelectStatement;
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


  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    when(select.getFields()).thenReturn(Arrays.asList(field2));
    Criterion c = Criterion.and(criterion1, Criterion.in(field, select));
    ResolvedTables res = new ResolvedTables();

    //when
    c.resolveTables(res);

    //then
    verify(field).resolveTables(res);
    verify(select).resolveTables(res);
    verify(criterion1).resolveTables(res);
  }
}

