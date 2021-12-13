package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlUtils.insert;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests of {@link InsertStatement}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestInsertStatement {

  @Mock
  private AliasedField field, field2, field3;

  @Mock
  private SelectStatement fromSelect;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    when(field3.getAlias()).thenReturn("whatever");

    InsertStatement ins1 = insert().into(tableRef("table1"))
        .fields(field)
        .from(fromSelect)
        .withDefaults(field3);
    ResolvedTables res = new ResolvedTables();

    //when
    ins1.resolveTables(res);

    //then
    verify(field).resolveTables(res);
    verify(field3).resolveTables(res);
    verify(fromSelect).resolveTables(res);
    assertThat(res.getModifiedTables(), Matchers.contains("TABLE1"));
  }


  @Test
  public void tableResolutionDetectsAllTables2() {
    //given
    InsertStatement ins1 = insert().into(tableRef("table1"))
        .fields(field)
        .values(field2);
    ResolvedTables res = new ResolvedTables();

    //when
    ins1.resolveTables(res);

    //then
    verify(field).resolveTables(res);
    verify(field2).resolveTables(res);
    assertThat(res.getModifiedTables(), Matchers.contains("TABLE1"));
  }


  @Test
  public void tableResolutionDetectsAllTables3() {
    //given
    InsertStatement ins1 = insert().into(tableRef("table1"))
        .from(tableRef("table2"));
    ResolvedTables res = new ResolvedTables();

    //when
    ins1.resolveTables(res);

    //then
    assertThat(res.getModifiedTables(), Matchers.contains("TABLE1"));
    assertThat(res.getReadTables(), Matchers.contains("TABLE2"));
  }
}

