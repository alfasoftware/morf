package org.alfasoftware.morf.xml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

/**
 * Tests for {@link Version2to4TransformingReader}
 */
public class TestVersion2to4TransformingReader {

  private final long seed = System.currentTimeMillis(); // helps fuzz test
  private final Random random = new Random(seed);

  /**
   * Test helper.
   *
   * Sets up a mock BufferedReader which behaves like one, but where we can control how much it returns when called.
   *
   * @param responses The String elements to make available for reading
   * @return The resulting string
   */
  private String processTest(LinkedList<String> responses, int inputVersion) throws IOException {
    BufferedReader sourceReader = Mockito.mock(BufferedReader.class);

    final java.util.LinkedList<String> markedResponses = new LinkedList<>();

    Mockito.when(sourceReader.read(Mockito.any(char[].class), Mockito.anyInt(), Mockito.anyInt())).thenAnswer((invocation) -> {
      char[] inputChars = (char[]) invocation.getArguments()[0];
      int inputOffset = (int) invocation.getArguments()[1];
      int inputLength = (int) invocation.getArguments()[2];

      if (responses.isEmpty()) {
        return -1;
      }

      String response = responses.remove();
      if (response.length() > inputLength) {
        responses.addFirst(response.substring(inputLength));
      }

      int toCopy = Math.min(response.length(), inputLength);
      System.arraycopy(response.toCharArray(), 0, inputChars, inputOffset, toCopy);
      return toCopy;
    });

    Mockito.doAnswer((invocation) -> {
      assert markedResponses.isEmpty();
      markedResponses.addAll(responses);
      return null;
    }).when(sourceReader).mark(Mockito.anyInt());

    Mockito.doAnswer((invocation) -> {
      responses.clear();
      responses.addAll(markedResponses);
      markedResponses.clear();
      return null;
    }).when(sourceReader).reset();


    Mockito.when(sourceReader.markSupported()).thenReturn(true);

    char[] buff = new char[128];
    StringBuilder result = new StringBuilder();
    try (Reader readerUnderTest = new Version2to4TransformingReader(sourceReader, inputVersion)) {
      while (true) {
        int off = random.nextInt(32);
        int len = random.nextInt(31)+1;

        int charsRead = readerUnderTest.read(buff, off, len);
        if (charsRead < 0) {
          return result.toString();
        }
        result.append(buff, off, charsRead);
      }
    }
  }


  private void runTestNto4(int inputVersion, String expected, List<String> input) throws IOException {
    try {
      Assert.assertEquals(
        expected,
        processTest(new LinkedList<>(input), inputVersion)
      );
    } catch (AssertionError ae) {
      System.err.println("seed="+seed);
      throw ae;
    }
  }


  /**
   * @param expected The expected output
   * @param input The list of fragments to supply from the mock BufferedReader
   */
  private void runTest2to4(String expected, List<String> input) throws IOException {
    runTestNto4(2, expected, input);
  }

  /**
   * @param expected The expected output
   * @param input The list of fragments to supply from the mock BufferedReader
   */
  private void runTest3to4(String expected, List<String> input) throws IOException {
    runTestNto4(3, expected, input);
  }


  /**
   * Test some cases where transforms should not occur.
   */
  @Test
  public void testWithoutTransform() throws IOException {
    runTest2to4("one, two.", ImmutableList.of("one, ", "two."));
    runTest2to4("An &amp; should not be transformed", ImmutableList.of("An &amp; should not be transformed"));
    runTest2to4("Not quite a null &#0", ImmutableList.of("Not quite a null &#0"));
    runTest2to4("Not quite a null &0#; this", ImmutableList.of("Not quite a null &0#; this"));
    runTest2to4("Not quite a null &#0 this", ImmutableList.of("Not quite a null &","#0 this"));
    runTest2to4("Not quite a null &#0 this", ImmutableList.of("Not quite a null &#","0 this"));
    runTest2to4("Not quite a null &#0 this", ImmutableList.of("Not quite a null &#0"," this"));
  }


  /**
   * Test some cases where transforms should occur.
   */
  @Test
  public void testTranform2() throws IOException {
    runTest2to4("1 An \\u0000 should be transformed", ImmutableList.of("1 An &#0; should be transformed"));
    runTest2to4("2 An \\u0000 should be transformed", ImmutableList.of("2 An &#0;", " should be transformed"));
    runTest2to4("3 An \\u0000 should be transformed", ImmutableList.of("3 An ", "&#0; should be transformed"));
    runTest2to4("4 An \\u0000 should be transformed", ImmutableList.of("4 An &", "#0;"," should be transformed"));
    runTest2to4("5 An \\u0000 should be transformed", ImmutableList.of("5 An &", "#", "0", ";", " should be transformed"));
    runTest2to4("6 An \\u0000 should be transformed", ImmutableList.of("6 An &#", "0", ";", " should be transformed"));
    runTest2to4("7 An \\u0000 should be transformed", ImmutableList.of("7 An &#", "0;", " should be transformed"));
    runTest2to4("8 Multiple \\u0000 transforms \\u0000 needed", ImmutableList.of("8 ", "Multiple &#0; transforms &#0; needed"));
    runTest2to4("9 A single \\\\ should be transformed", ImmutableList.of("9 A single \\ should be transformed"));
    runTest2to4("A single \\\\ should be transformed", ImmutableList.of("A single \\", " should be transformed"));
    runTest2to4("A single \\\\ should be transformed", ImmutableList.of("A single ", "\\ should be transformed"));
    runTest2to4("A double \\\\\\\\ should be transformed", ImmutableList.of("A double ", "\\\\ should be transformed"));
    runTest2to4("A double \\\\\\\\ should be transformed", ImmutableList.of("A double \\", "\\ should be transformed"));

    runTest2to4("trans \\u0001 \\u0007 &#9; &#10; &#13; \\u000e \\u001f", ImmutableList.of("trans &#1; &#7; &#9; &#10; &#13; &#14; &#31;"));
    runTest2to4("\\u0007 &#x9; &#xa; &#xd; \\u001f", ImmutableList.of("&#x7; &#x9; &#xa; &#xd; &#x1f;"));
    runTest2to4("trans \\ud800 ", ImmutableList.of("trans &#55296; "));

    runTest2to4("Unusual unicode fffe [\\ufffe] ffff [\\uffff]", ImmutableList.of("Unusual unicode fffe [\ufffe] ffff [\uffff]"));
  }


  @Test
  public void testTranform3() throws IOException {
    //&#0; should never be in v3, but we might as well transform
    runTest3to4("1 An \\u0000 should be transformed", ImmutableList.of("1 An &#0; should be transformed"));
    runTest3to4("2 An \\u0000 should be transformed", ImmutableList.of("2 An &#0;", " should be transformed"));
    runTest3to4("3 An \\u0000 should be transformed", ImmutableList.of("3 An ", "&#0; should be transformed"));
    runTest3to4("4 An \\u0000 should be transformed", ImmutableList.of("4 An &", "#0;"," should be transformed"));
    runTest3to4("5 An \\u0000 should be transformed", ImmutableList.of("5 An &", "#", "0", ";", " should be transformed"));
    runTest3to4("6 An \\u0000 should be transformed", ImmutableList.of("6 An &#", "0", ";", " should be transformed"));
    runTest3to4("7 An \\u0000 should be transformed", ImmutableList.of("7 An &#", "0;", " should be transformed"));
    runTest3to4("8 Multiple \\u0000 transforms \\u0000 needed", ImmutableList.of("8 ", "Multiple &#0; transforms &#0; needed"));
    runTest3to4("A single \\ should not be transformed", ImmutableList.of("A single \\ should not be transformed"));
    runTest3to4("A single \\ should not be transformed", ImmutableList.of("A single \\", " should not be transformed"));
    runTest3to4("A single \\ should not be transformed", ImmutableList.of("A single ", "\\ should not be transformed"));
    runTest3to4("A double \\\\ should not be transformed", ImmutableList.of("A double ", "\\\\ should not be transformed"));

    runTest3to4("trans \\u0001 \\u0007 &#9; &#10; &#13; \\u000e \\u001f", ImmutableList.of("trans &#1; &#7; &#9; &#10; &#13; &#14; &#31;"));
  }


  /**
   * Tests for {@link Version2to4TransformingReader#readVersion(BufferedReader)}
   */
  @Test
  public void testReadVersion() throws IOException {
    try (BufferedReader reader = new BufferedReader(new StringReader(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<table version=\"1\">\n" +
      "  <metadata name=\"Foo\">"
    ))) {
      Assert.assertEquals(1, Version2to4TransformingReader.readVersion(reader));
    }

    try (BufferedReader reader = new BufferedReader(new StringReader(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<table version=\"2\">\n" +
      "  <metadata name=\"Foo\">"
    ))) {
      Assert.assertEquals(2, Version2to4TransformingReader.readVersion(reader));
    }

    try (BufferedReader reader = new BufferedReader(new StringReader(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<table version=\"3\">\n" +
      "  <metadata name=\"Foo\">"
    ))) {
      Assert.assertEquals(3, Version2to4TransformingReader.readVersion(reader));
    }

    try (BufferedReader reader = new BufferedReader(new StringReader(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
      "<table version=\"4\">\n" +
      "  <metadata name=\"Foo\">"
    ))) {
      Assert.assertEquals(4, Version2to4TransformingReader.readVersion(reader));
    }

    try (BufferedReader reader = new BufferedReader(new StringReader(
      "<?xml version=\"1.0\" encoding=\"UTF-8\"?><table version=\"2\">\n" +
      "<metadata name=\"SomeTable\">\n"
    ))) {
      Assert.assertEquals(2, Version2to4TransformingReader.readVersion(reader));
    }

    try (BufferedReader reader = new BufferedReader(new StringReader(
      "foo"
    ))) {
      Assert.assertEquals(-1, Version2to4TransformingReader.readVersion(reader));
    }
  }
}
