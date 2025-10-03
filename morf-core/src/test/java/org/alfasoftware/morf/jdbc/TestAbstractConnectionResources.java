package org.alfasoftware.morf.jdbc;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;

import org.hamcrest.Matchers;
import org.junit.Test;
import org.mockito.Mockito;

public class TestAbstractConnectionResources {

  /**
   * Test happy path for non-Postgres connections.
   */
  @Test
  public void testValidatePgConnectionStandardConformingStringsOther() {
    AbstractConnectionResources.validatePgConnectionStandardConformingStrings("OTHER", mock(Connection.class));
  }


  /**
   * Test happy path for Postgres connections, where standard_conforming_strings is on, which is good.
   */
  @Test
  public void testValidatePgConnectionStandardConformingStringsTrue() {
    MockPgConnection mockPgConnection = mock(MockPgConnection.class);
    when(mockPgConnection.getStandardConformingStrings()).thenReturn(true);

    AbstractConnectionResources.validatePgConnectionStandardConformingStrings("PGSQL", mockPgConnection);
  }


  /**
   * Test unhappy path for Postgres connections, where standard_conforming_strings is off, and therefore wrong.
   */
  @Test
  public void testValidatePgConnectionStandardConformingStringsFalse() {
    MockPgConnection mockPgConnection = mock(MockPgConnection.class);
    when(mockPgConnection.getStandardConformingStrings()).thenReturn(false);

    IllegalStateException exception = assertThrows(IllegalStateException.class,
      () -> AbstractConnectionResources.validatePgConnectionStandardConformingStrings("PGSQL", mockPgConnection));

    assertThat(exception.getMessage(), Matchers.containsString("standard_conforming_strings=off"));
  }


  /**
   * Test unhappy path for Postgres connections, where we cannot find the expected method.
   */
  @Test
  public void testValidatePgConnectionStandardConformingStringsPrivateMethod() {
    IllegalStateException exception = assertThrows(IllegalStateException.class,
        () -> AbstractConnectionResources.validatePgConnectionStandardConformingStrings("PGSQL", mock(PrivatePgConnection.class, Mockito.CALLS_REAL_METHODS)));

    assertThat(exception.getMessage(), Matchers.containsString("invoking such method"));
  }


  /**
   * Test unhappy path for Postgres connections, where we cannot invoke the expected method.
   */
  @Test
  public void testValidatePgConnectionStandardConformingStringsMissingMethod() {
    IllegalStateException exception = assertThrows(IllegalStateException.class,
        () -> AbstractConnectionResources.validatePgConnectionStandardConformingStrings("PGSQL", mock(Connection.class)));

    assertThat(exception.getMessage(), Matchers.containsString("no such method"));
  }


  /** Helper interface for testing */
  private interface MockPgConnection extends Connection {
    // see org.postgresql.core.BaseConnection.getStandardConformingStrings()
    boolean getStandardConformingStrings();
  }


  /** Helper class for testing */
  abstract class PrivatePgConnection implements Connection {
    // see org.postgresql.core.BaseConnection.getStandardConformingStrings()
    public boolean getStandardConformingStrings() throws java.lang.reflect.InvocationTargetException {
      throw new java.lang.reflect.InvocationTargetException(null); // emulating reflection issues
    }
  }
}
