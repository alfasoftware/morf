package org.alfasoftware.morf.xml;

import static org.alfasoftware.morf.xml.Escaping.escapeCharacters;
import static org.alfasoftware.morf.xml.Escaping.unescapeCharacters;
import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

/**
 * Tests for {@link Escaping}
 */
public class TestEscaping {

  // valid chars: https://www.w3.org/TR/xml/#NT-Char

  private static class TestCase {
    final String escaped;
    final String unescaped;

    TestCase(String escaped, String unescaped) {
      super();
      this.escaped = escaped;
      this.unescaped = unescaped;
    }
  }

  private final List<TestCase> testCases = new ArrayList<>();

  private void addTestCase(String escaped, String unescaped) {
    testCases.add(new TestCase(escaped, unescaped));
  }

  {
    /*          XML format             Internal format                     */
    addTestCase("ABCDEF",              "ABCDEF" );
    addTestCase("AB\\\\CD\\\\EF",      "AB\\CD\\EF" ); // backslashes escaped to double-backslash
    addTestCase("ABC\\u0000DEF",       string('A', 'B', 'C', (char) 0, 'D', 'E', 'F'));
    addTestCase("AB\\u0001C",          string('A', 'B', (char)1, 'C') );
    addTestCase("AB\\u0019C",          string('A', 'B', (char) 0x19, 'C') );
    addTestCase("ABCD\\\\u0001EF",     "ABCD\\u0001EF" );
    addTestCase("ABCD\\\\\\\\u00zz",   "ABCD\\\\u00zz" );// ABCD\\\\u00zzEF    ->   ABCD\\u00zz
    addTestCase("ABCD\t\r\nEF",        "ABCD\t\r\nEF" );
    addTestCase("AB\\ud800C",          string('A', 'B', (char) 0xd800, 'C') );

    addTestCase(string('A', 'B', (char) 0xd7ff, 'C'),     string('A', 'B', (char) 0xd7ff, 'C') );
    addTestCase("AB\\udfffC",          string('A', 'B', (char) 0xdfff, 'C') );
    addTestCase(string('A', 'B', (char) 0xe000, 'C'),     string('A', 'B', (char) 0xe000, 'C') );
    addTestCase(string('A', 'B', (char) 0xfffd, 'C'),     string('A', 'B', (char) 0xfffd, 'C') );
    addTestCase("A\\ufffe\\uffffB",    "A\ufffe\uffffB");
  }

  @Test
  public void testEscapeCharacters() {
    for (TestCase testCase : testCases) {
      assertEquals(testCase.escaped, escapeCharacters(testCase.unescaped));
    }
  }

  @Test
  public void testUnescapeCharacters() {
    for (TestCase testCase : testCases) {
      assertEquals(testCase.unescaped, unescapeCharacters(testCase.escaped));
    }

    // special cases for v3
    assertEquals(string('a', 'b', 'c', (char) 0), unescapeCharacters("abc\0"));
    assertEquals(string('a', 'b', 'c', (char) 0), unescapeCharacters("abc\u0000"));
  }


  private static String string(char... chars) {
    return new String(chars);
  }
}

