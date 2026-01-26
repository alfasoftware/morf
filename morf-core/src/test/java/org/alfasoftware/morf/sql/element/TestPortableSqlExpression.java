package org.alfasoftware.morf.sql.element;

import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.junit.Before;
import org.junit.Test;

import static org.alfasoftware.morf.sql.SqlUtils.CaseStatementBuilder.nativeSql;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Test class for PortableSqlExpression. Most of the functionality is tested in AbstractSqlDialectTest, this class covers the remaining methods.
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
public class TestPortableSqlExpression {

  private static final String EXPRESSION_ALIAS = "exp1";
  private static final String PGSQL = "PGSQL";
  private static final String ORACLE = "ORACLE";

  private static final String NATIVE_EXPRESSION_1 = "something json";
  private static final String NATIVE_EXPRESSION_2 = "something else";
  private static final String TABLE_REF = "table1";
  private static final String COL_REF = "col1";

  private PortableSqlExpression portableSqlExpression;


  @Before
  public void setUp() {
    portableSqlExpression = PortableSqlExpression.builder().withExpressionForDatabaseType(
                    PGSQL,
                    nativeSql(NATIVE_EXPRESSION_1),
                    tableRef(TABLE_REF).field(COL_REF),
                    nativeSql(NATIVE_EXPRESSION_2))
            .withExpressionForDatabaseType(
                    ORACLE,
                    nativeSql(NATIVE_EXPRESSION_1),
                    tableRef(TABLE_REF).field(COL_REF),
                    nativeSql(NATIVE_EXPRESSION_2))
            .as(EXPRESSION_ALIAS)
            .build();
  }


  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupportedDatabase() {
    portableSqlExpression.getExpressionForDatabaseType("H2");
  }


  @Test
  public void testDeepCopy() {
    PortableSqlExpression copy = portableSqlExpression.deepCopyInternal(mock(DeepCopyTransformation.class));
    assertEquals(copy, portableSqlExpression);
  }
}
