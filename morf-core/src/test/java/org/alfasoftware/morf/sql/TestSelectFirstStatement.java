package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlUtils.selectFirst;
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
 * Unit tests of {@link SelectFirstStatement}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestSelectFirstStatement {

  @Mock
  private AliasedField field, field2;

  @Mock
  private SelectStatement fromSelect, subSelect;

  @Mock
  private Criterion onCondition, criterion2;


  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    SelectFirstStatement sel1 = selectFirst(field)
        .from(fromSelect)
        .innerJoin(subSelect, onCondition)
        .where(criterion2);


    ResolvedTables res = new ResolvedTables();

    //when
    sel1.resolveTables(res);

    //then
    verify(field).resolveTables(res);
    verify(fromSelect).resolveTables(res);
    verify(subSelect).resolveTables(res);
    verify(onCondition).resolveTables(res);
    verify(criterion2).resolveTables(res);
  }


  @Test
  public void tableResolutionDetectsAllTables2() {
    //given
    SelectFirstStatement sel1 = selectFirst(field)
        .from("table1");

    ResolvedTables res = new ResolvedTables();

    //when
    sel1.resolveTables(res);

    //then
    assertThat(res.getReadTables(), Matchers.contains("TABLE1"));
  }
}

