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

  @Test
  public void testValidatePgConnectionStandardConformingStringsOther() {
    AbstractConnectionResources.validatePgConnectionStandardConformingStrings("OTHER", mock(Connection.class));
  }


  @Test
  public void testValidatePgConnectionStandardConformingStringsTrue() {
    MockPgConnection mockPgConnection = mock(MockPgConnection.class);
    when(mockPgConnection.getStandardConformingStrings()).thenReturn(true);

    AbstractConnectionResources.validatePgConnectionStandardConformingStrings("PGSQL", mockPgConnection);
  }


  @Test
  public void testValidatePgConnectionStandardConformingStringsFalse() {
    MockPgConnection mockPgConnection = mock(MockPgConnection.class);
    when(mockPgConnection.getStandardConformingStrings()).thenReturn(false);

    IllegalStateException exception = assertThrows(IllegalStateException.class,
      () -> AbstractConnectionResources.validatePgConnectionStandardConformingStrings("PGSQL", mockPgConnection));

    assertThat(exception.getMessage(), Matchers.containsString("standard_conforming_strings=off"));
  }


  @Test
  public void testValidatePgConnectionStandardConformingStringsPrivateMethod() {
    IllegalStateException exception = assertThrows(IllegalStateException.class,
        () -> AbstractConnectionResources.validatePgConnectionStandardConformingStrings("PGSQL", mock(PrivatePgConnection.class, Mockito.CALLS_REAL_METHODS)));

    assertThat(exception.getMessage(), Matchers.containsString("no such method"));
  }


  @Test
  public void testValidatePgConnectionStandardConformingStringsMissingMethod() {
    IllegalStateException exception = assertThrows(IllegalStateException.class,
        () -> AbstractConnectionResources.validatePgConnectionStandardConformingStrings("PGSQL", mock(Connection.class)));

    assertThat(exception.getMessage(), Matchers.containsString("no such method"));
  }


  private interface MockPgConnection extends Connection {
    // see org.postgresql.core.BaseConnection.getStandardConformingStrings()
    boolean getStandardConformingStrings();
  }


  abstract class PrivatePgConnection implements Connection {
    // see org.postgresql.core.BaseConnection.getStandardConformingStrings()
    public boolean getStandardConformingStrings() throws java.lang.reflect.InvocationTargetException {
      throw new java.lang.reflect.InvocationTargetException(null); // emulating reflection issues
    }
  }
}
