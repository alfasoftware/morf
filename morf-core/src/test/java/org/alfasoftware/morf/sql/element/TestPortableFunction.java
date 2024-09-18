package org.alfasoftware.morf.sql.element;

import static org.junit.Assert.assertEquals;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Before;
import org.junit.Test;

/**
 * Test class for PortableFunction.
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
public class TestPortableFunction {

  private static final String POSTGRES_FUNCTION = "POSTGRES_FUNCTION";
  private static final String ORACLE_FUNCTION = "ORACLE_FUNCTION";
  private static final String FIELD = "field";
  private static final int INTEGER_VALUE = 123;
  private static final String STRING_VALUE = "C";
  private static final String PGSQL = "PGSQL";
  private static final String ORACLE = "ORACLE";

  private PortableFunction portableFunction;

  @Before
  public void setUp() {
    portableFunction = PortableFunction.withFunctionForDatabaseType(
            PGSQL,
            POSTGRES_FUNCTION,
            new FieldReference(FIELD),
            new FieldLiteral(INTEGER_VALUE))
        .addFunctionForDatabaseType(
            ORACLE,
            ORACLE_FUNCTION,
            new FieldReference(FIELD),
            new FieldLiteral(INTEGER_VALUE),
            new FieldLiteral(STRING_VALUE));
  }


  @Test
  public void testPostgresFunction() {
    Pair<String, List<AliasedField>> functionWitArgs = portableFunction.getFunctionForDatabaseType(PGSQL);

    assertEquals("Function name should be POSTGRES_FUNCTION", POSTGRES_FUNCTION, functionWitArgs.getLeft());
    assertEquals("Function should have 2 arguments", 2, functionWitArgs.getRight().size());
  }


  @Test
  public void testOracleFunction() {
    Pair<String, List<AliasedField>> functionWithArgs = portableFunction.getFunctionForDatabaseType(ORACLE);

    assertEquals("Function name should be ORACLE_FUNCTION", ORACLE_FUNCTION, functionWithArgs.getLeft());
    assertEquals("Function should have 3 arguments", 3, functionWithArgs.getRight().size());
  }


  @Test(expected = UnsupportedOperationException.class)
  public void testH2Function() {
    portableFunction.getFunctionForDatabaseType("H2");
  }
}
