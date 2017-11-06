package org.alfasoftware.morf.xml;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.google.common.collect.ImmutableList;

public class TestVersion2to3TranformingReader {

  private BufferedReader outputReader;
  private BufferedReader sourceReader;


  private final java.util.LinkedList<String> responses = new LinkedList<>();
  private final java.util.LinkedList<String> markedResponses = new LinkedList<>();


  @Before
  public void before() throws IOException {
    sourceReader = Mockito.mock(BufferedReader.class);

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
      markedResponses.addAll(responses);
      return null;
    }).when(sourceReader).mark(Mockito.anyInt());

    Mockito.doAnswer((invocation) -> {
      responses.clear();
      responses.addAll(markedResponses);
      return null;
    }).when(sourceReader).reset();


    Mockito.when(sourceReader.markSupported()).thenReturn(true);

    outputReader = new BufferedReader(new Version2to3TranformingReader(sourceReader));
  }

  private void test(String expected, List<String> input) {
    responses.addAll(input);

    Assert.assertEquals(
      expected,
      outputReader.lines().collect(Collectors.joining())
    );
  }

  @Test
  public void testWithoutTransform() throws IOException {
    test("one, two.", ImmutableList.of("one, ", "two."));
    test("An &amp; should not be transformed", ImmutableList.of("An &amp; should not be transformed"));
    test("Not quite a null &#0", ImmutableList.of("Not quite a null &#0"));
    test("Not quite a null &0#; this", ImmutableList.of("Not quite a null &0#; this"));
    test("Not quite a null &#0 this", ImmutableList.of("Not quite a null &","#0 this"));
  }


  @Test
  public void testTranform() throws IOException {
    test("1 An \\0 should be transformed", ImmutableList.of("1 An &#0; should be transformed"));
    test("2 An \\0 should be transformed", ImmutableList.of("2 An &#0;", " should be transformed"));
    test("3 An \\0 should be transformed", ImmutableList.of("3 An ", "&#0; should be transformed"));
    test("4 An \\0 should be transformed", ImmutableList.of("4 An &", "#0;"," should be transformed"));
    test("5 An \\0 should be transformed", ImmutableList.of("5 An &", "#", "0", ";", " should be transformed"));
  }

}
