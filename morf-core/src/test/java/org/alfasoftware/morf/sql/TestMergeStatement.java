package org.alfasoftware.morf.sql;

import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.verify;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.hamcrest.Matchers;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

/**
 * Unit tests of {@link MergeStatement}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
public class TestMergeStatement {

  @Mock
  private AliasedField field;

  @Mock
  private SelectStatement fromSelect;

  @Before
  public void setUp() throws Exception {
    MockitoAnnotations.openMocks(this);
  }


  @Test
  public void tableResolutionDetectsAllTables1() {
    //given
    MergeStatement mer1 = MergeStatement.merge()
        .into(tableRef("table1"))
        .tableUniqueKey(field("id"))
        .from(fromSelect)
        .ifUpdating((overrides, values) -> overrides
          .set(field))
        .build();
    ResolvedTables res = new ResolvedTables();

    //when
    mer1.resolveTables(res);

    //then
    verify(field).resolveTables(res);
    verify(fromSelect).resolveTables(res);
    assertThat(res.getModifiedTables(), Matchers.contains("TABLE1"));
  }
}

