package org.alfasoftware.morf.metadata;

import static org.alfasoftware.morf.metadata.CaseInsensitiveString.of;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import javax.annotation.concurrent.NotThreadSafe;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests {@link CaseInsensitiveString}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2019
 */
@NotThreadSafe
public class TestCaseInsensitiveString {

  @SuppressWarnings("deprecation")
  @Before
  public void setup() {
    CaseInsensitiveString.unsafeClearCache();
  }

  @Test
  public void testBasicAssumptions() {

    // Truisms
    assertTrue(of("aaaaa").equals(of("aaaaa")));
    assertTrue(of("aaaaa") == of("aaaaa"));
    assertTrue(of("aaAaa").equals(of("aaaaa")));
    assertTrue(of("aaAaa") == of("aaaaa"));
    assertTrue(of("AAAAA").equals(of("aaaaa")));
    assertTrue(of("AAAAA") == of("aaaaa"));


    assertTrue(of("aaaaa").hashCode() == of("aaaaa").hashCode());
    assertTrue(of("aaAaa").hashCode() == of("aaaaa").hashCode());
    assertTrue(of("AAAAA").hashCode() == of("aaaaa").hashCode());

    // Falsehoods
    assertFalse(of("aa aaa").equals(of("aaaaa")));
    assertFalse(of("aaaa").equals(of("aaaaa")));
    assertFalse(of("aaaab"
        + "").equals(of("aaaaa")));
  }

  @Test
  public void testToString1() {
    of("AAAAA");
    assertTrue(of("aaaaA").toString().equals("AAAAA")); // Because this is how we first interned it
  }

  @Test
  public void testToString2() {
    of("aaaAa");
    assertTrue(of("aaaaA").toString().equals("aaaAa")); // Because this is how we first interned it
  }
}