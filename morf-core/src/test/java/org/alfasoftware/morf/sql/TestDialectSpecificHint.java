package org.alfasoftware.morf.sql;

import static org.junit.Assert.fail;

import org.junit.Test;

public class TestDialectSpecificHint {

  /**
   * We should not be able to create a DialectSpecificHint object with blank parameters
   */
  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithFirstParameterEmptyThrowsException() {
    new DialectSpecificHint("", "not_empty");
    fail("Did not catch IllegalArgumentException");
  }


  /**
   * We should not be able to create a DialectSpecificHint object with blank parameters
   */
  @Test(expected = IllegalArgumentException.class)
  public void testConstructorWithSecondParameterEmptyThrowsException() {
    new DialectSpecificHint("not_empty", "");
    fail("Did not catch IllegalArgumentException");
  }
}

