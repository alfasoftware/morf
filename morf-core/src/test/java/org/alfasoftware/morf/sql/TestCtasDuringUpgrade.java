package org.alfasoftware.morf.sql;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TestCtasDuringUpgrade {

  @Test
  public void testGeneric() {
    final CtasDuringUpgrade hint = new CtasDuringUpgrade();
    assertTrue(hint.getUseCtasDuringUpgrade());
    assertTrue(hint.getUseCtasDuringUpgrade("?"));
    assertTrue(hint.getUseCtasDuringUpgrade("ANY"));
    assertTrue(hint.getUseCtasDuringUpgrade("H2"));
    assertTrue(hint.getUseCtasDuringUpgrade("DB2"));
  }


  @Test
  public void testGenericTrue() {
    final CtasDuringUpgrade hint = new CtasDuringUpgrade(true);
    assertTrue(hint.getUseCtasDuringUpgrade());
    assertTrue(hint.getUseCtasDuringUpgrade("?"));
    assertTrue(hint.getUseCtasDuringUpgrade("ANY"));
    assertTrue(hint.getUseCtasDuringUpgrade("H2"));
    assertTrue(hint.getUseCtasDuringUpgrade("DB2"));
  }


  @Test
  public void testGenericFalse() {
    final CtasDuringUpgrade hint = new CtasDuringUpgrade(false);
    assertFalse(hint.getUseCtasDuringUpgrade());
    assertFalse(hint.getUseCtasDuringUpgrade("?"));
    assertFalse(hint.getUseCtasDuringUpgrade("ANY"));
    assertFalse(hint.getUseCtasDuringUpgrade("H2"));
    assertFalse(hint.getUseCtasDuringUpgrade("DB2"));
  }


  @Test
  public void testDatabaseType() {
    final CtasDuringUpgrade hint = new CtasDuringUpgrade("DB2");
    assertFalse(hint.getUseCtasDuringUpgrade());
    assertFalse(hint.getUseCtasDuringUpgrade("?"));
    assertFalse(hint.getUseCtasDuringUpgrade("ANY"));
    assertFalse(hint.getUseCtasDuringUpgrade("H2"));
    assertTrue(hint.getUseCtasDuringUpgrade("DB2"));
  }
}
