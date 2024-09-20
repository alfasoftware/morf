package org.alfasoftware.morf.sql.element;

import org.alfasoftware.morf.util.DeepCopyTransformation;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

/**
 * Test class for PortableFunction. Most of the functionality is tested in AbstractSqlDialectTest, this class covers the remaining methods.
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
public class TestPortableSqlFunction {

  private static final String POSTGRES_FUNCTION = "POSTGRES_FUNCTION";
  private static final String ORACLE_FUNCTION = "ORACLE_FUNCTION";
  private static final String FIELD = "field";
  private static final int INTEGER_VALUE = 123;
  private static final String STRING_VALUE = "C";
  private static final String PGSQL = "PGSQL";
  private static final String ORACLE = "ORACLE";

  private PortableSqlFunction portableFunction;

  @Before
  public void setUp() {
    portableFunction = PortableSqlFunction.builder().withFunctionForDatabaseType(
                    PGSQL,
                    POSTGRES_FUNCTION,
                    new FieldReference(FIELD),
                    new FieldLiteral(INTEGER_VALUE))
            .withFunctionForDatabaseType(
                    ORACLE,
                    ORACLE_FUNCTION,
                    new FieldReference(FIELD),
                    new FieldLiteral(INTEGER_VALUE),
                    new FieldLiteral(STRING_VALUE))
            .as(FIELD)
            .build();
  }


  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupportedDatabase() {
    portableFunction.getFunctionForDatabaseType("H2");
  }


  @Test
  public void testDeepCopy() {
    PortableSqlFunction copy = portableFunction.deepCopyInternal(mock(DeepCopyTransformation.class));
    assertEquals(copy, portableFunction);
  }
}
