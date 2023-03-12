package org.alfasoftware.morf.xml;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Transforms version 2 and version 3 XML format into version 4.
 *
 * <p>This essentially involves replacing:
 * <ol>
 * <li>Illegal &amp;#NNNN; character references (like &amp;#0;) with \\uNNNN - the v4 escaped representation of a character that is invalid in xml.</li>
 * <li>Single backslash characters with double backslashes. (only for v2)</li>
 * </ol>
 * </p>
 *
 * <p>Much of the complexity in this class results from it trying to be efficient in the fast case - when there is nothing to transform.
 * In these cases it simply passes the data through and does very fast checks on the characters</p>
 */
class Version2to4TransformingReader extends Reader {

  private final BufferedReader delegateReader;
  private char[] temporary = new char[] {};
  private int skipChars;
  private final int inputVersion;

  /**
   * Construct the transform given a buffered reader.
   */
  Version2to4TransformingReader(BufferedReader bufferedReader, int inputVersion) {
    super();
    this.delegateReader = bufferedReader;
    this.inputVersion = inputVersion;

    if (!delegateReader.markSupported()) {
      throw new UnsupportedOperationException("Mark support is required");
    }
  }


  /**
   * Tests whether a given input stream contains XML format 2, and therefore
   * should have the transform applied.
   * <p>
   * This is designed to match the known output format of
   * {@link XmlDataSetConsumer} which previously produced invalid XML. It is
   * deliberately brittle. There is no need for a more intelligent XML parser
   * here.
   * </p>
   *
   * @param bufferedReader The input stream in a buffered reader
   * @return true if the transform should be applied. (because it's format 2)
   */
  static int readVersion(BufferedReader bufferedReader) {
    try {
      bufferedReader.mark(1024); // arbitrary read-ahead limit - that's enough to get the info we want
      try {
        char[] buffer = new char[1024];

        int read = bufferedReader.read(buffer);
        if (read == -1) {
          return -1;
        }

        String content = new String(buffer, 0, read);

        // Apply the transform if the version number is 2 or 1
        Pattern pattern = Pattern.compile("table\\sversion=\"(\\d+)\""); //
        Matcher matcher = pattern.matcher(content);

        if (!matcher.find()) {
          return -1;
        } else {
          return Integer.parseInt(matcher.group(1));
        }

      } finally {
        bufferedReader.reset();
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * @see java.io.Reader#read(char[], int, int)
   */
  @Override
  public int read(char[] cbuf, int off, int len) throws IOException {
    // We need to transform &#0; into \0...
    int charsRead;

    // if there's no temporary buffer from a previous call, read from the main source
    if (temporary.length == 0) {
      // This is the common path
      charsRead = delegateReader.read(cbuf, off, len);
    } else {
      // there is a temporary buffer from a previous match, use that
      if (temporary.length > len) {
        // The temporary buffer is too big to fit in the buffer that's been supplied. This is an edge case, but we need to deal with it.
        // Copy out what we can, then create another temporary buffer for the remainder
        System.arraycopy(temporary, 0, cbuf, off, len);
        charsRead = len;
        char[] newTemporary = new char[temporary.length-len];
        System.arraycopy(temporary, len, newTemporary, 0, temporary.length-len);
        temporary = newTemporary;
      } else {
        // copy the entire temporary buffer into the output
        System.arraycopy(temporary, 0, cbuf, off, temporary.length);
        charsRead = temporary.length;
        temporary = new char[] {};
      }
    }

    // now search for the string we're replacing
    for (int idx = 0; idx < charsRead; idx++) {
      // we need to skip chars if we've put a backslash in the output
      if (skipChars > 0) {
        skipChars--;
        continue;
      }

      char testChar = cbuf[off + idx];

      if (testChar == '&') { // look for the ampersand
        // The first char matches.
        // Check whether the subsequent chars make up the ref
        ReferenceInfo characterReferenceToTransform = characterReferenceToTransform(cbuf, off + idx, charsRead - idx);
        if (characterReferenceToTransform != null) {
          String escapedString = String.format("\\u%04x", characterReferenceToTransform.referenceValue);
          return processEscape(cbuf, off, charsRead, idx, characterReferenceToTransform.sequenceLength, escapedString);
        }
      }

      if (testChar == '\\' && inputVersion == 2) { // backslash gets escaped to a double-backslash, but not in v3 as this will already have been done.
        return processEscape(cbuf, off, charsRead, idx, 1, "\\\\");
      }

      if (testChar == '\ufffe') { // unusual unicode that's not valid XML
        return processEscape(cbuf, off, charsRead, idx, 1, "\\ufffe");
      }

      if (testChar == '\uffff') { // unusual unicode that's not valid XML
        return processEscape(cbuf, off, charsRead, idx, 1, "\\uffff");
      }
    }

    // If we got here we found no matches to replace, so we can just return the buffer as read.
    // This is the common path
    return charsRead;
  }


  /**
   * @param cbuf The output buffer
   * @param off the current root output
   * @param charsRead the number of chars read from the input buffer
   * @param idx The current scan index in the input buffer
   * @param sequenceLength The length of the sequence we're replacing
   * @param escapedString The output escaped sequence
   * @return The number of
   */
  private int processEscape(char[] cbuf, int off, int charsRead, int idx, int sequenceLength, String escapedString) {
    // can be less than zero if we read past the end of this buffer and into the next
    int charsRemainingInBuffer = Math.max(charsRead - idx - sequenceLength, 0);

    // Create a temporary buffer to hold the remainder of the buffer we haven't yet scanned
    // There might be an existing temporary buffer, in which case keep that too.
    char[] escapedChars = escapedString.toCharArray();
    char[] newTemporary = new char[escapedChars.length + charsRemainingInBuffer + temporary.length];

    // write the escaped string
    System.arraycopy(escapedChars, 0, newTemporary, 0, escapedChars.length);

    // copy in what's left
    System.arraycopy(cbuf, off + idx + sequenceLength, newTemporary, escapedChars.length, charsRemainingInBuffer);

    // keep any existing buffer
    System.arraycopy(temporary, 0, newTemporary, escapedChars.length + charsRemainingInBuffer, temporary.length);

    temporary = newTemporary;
    skipChars = 2;

    // truncate the returned output to where we've got to
    return idx;
  }


  /**
   * Tests whether a given index in the buffer is a full null character reference.
   * Reads forward if required, but resets the position.
   */
  private ReferenceInfo characterReferenceToTransform(char[] cbuf, int ampersandIndex, int remaining) throws IOException {
    char[] bufferToTest;
    int indexToTest;

    // the maximum we could need is enough to read &#65535; or &#xffff; so 8 chars, including the ampersand.
    final int maxRefSize = 8;
    int additionalCharsRequired = maxRefSize-remaining;

    if (additionalCharsRequired > 0) {
      bufferToTest = new char[maxRefSize];
      // we need to read ahead because we don't have enough chars
      // first copy the remaining chars in
      System.arraycopy(cbuf, ampersandIndex, bufferToTest, 0, remaining);

      // look in the temporary buffer first
      int writeIdx = remaining;
      System.arraycopy(temporary, 0, bufferToTest, writeIdx, Math.min(maxRefSize-writeIdx, temporary.length));
      writeIdx += Math.min(maxRefSize-writeIdx, temporary.length);

      // copy in the remainder, resetting the reader after we've read it
      delegateReader.mark(maxRefSize);
      while (writeIdx < maxRefSize) {
        int additionalCharsRead = delegateReader.read(bufferToTest, writeIdx, maxRefSize-writeIdx);
        if (additionalCharsRead < 0) {
          // end of stream
          break;
        }
        writeIdx += additionalCharsRead;
      }

      indexToTest = 0;
      // always reset. We'll gobble the extra chars below if we need to
      delegateReader.reset();
    } else {
      // The common path - we have enough buffer to work with
      bufferToTest = cbuf;
      indexToTest = ampersandIndex;
    }

    // we know the first char is &
    // shortcut out if the second char is not #
    if (bufferToTest[indexToTest+1] != '#') {
      return null;
    }

    // put the rest in a string
    int semiColonPos = -1;
    for (int i = 0; i < bufferToTest.length-indexToTest; i++) {
      if (bufferToTest[indexToTest+i] == ';') {
        semiColonPos = i;
        break;
      }
    }

    // no semicolon - exit
    if (semiColonPos == -1) {
      return null;
    }

    int reference;
    // Hex references look like:  &#x1a3;  decimal ones look like &#456;
    if (bufferToTest[indexToTest+2] == 'x') {
      // it's a hex reference
      reference = parseReference(bufferToTest, indexToTest+3, semiColonPos-3, 16);
    } else {
      // it's a decimal reference
      reference = parseReference(bufferToTest, indexToTest+2, semiColonPos-2, 10);
    }

    if (reference == -1) {
      return null;
    }

    // we don't need to transform valid references
    if (Escaping.isCharValidForXml(reference)) {
      return null;
    }

    int referenceLength = semiColonPos+1;

    // If we get here, it matches and we have a reference to escape.
    // We now need to ensure the main parser doesn't read the rest of the character reference.
    // Gobble any extra chars we needed...
    int toGobble = referenceLength-remaining;

    // ...first from any temporary buffer
    int temporaryLength = temporary.length;
    if (temporaryLength > 0 && toGobble > 0) {
      if (toGobble >= temporaryLength) {
        // gobble all of it
        temporary = new char[0];
      } else {
        // gobble some of it
        char[] newTemporary = new char[temporaryLength - toGobble];
        System.arraycopy(temporary, toGobble, newTemporary, 0, temporaryLength - toGobble);
        temporary = newTemporary;

      }
      toGobble -= temporaryLength;
    }

    // ...then whatever's left from the main stream
    while (toGobble > 0) {
      int charsRead = delegateReader.read(new char[toGobble], 0, toGobble);
      if (charsRead == -1) throw new IllegalStateException("Unexpected EOF");
      toGobble -= charsRead;
    }

    return new ReferenceInfo(reference, referenceLength);
  }


  /**
   * Parse a section of the buffer into a character reference value.
   */
  private int parseReference(char[] buffer, int off, int len, int radix) {
    String testString = new String(buffer, off, len);
    try {
      return Integer.parseInt(testString, radix);
    } catch (NumberFormatException nfe) {
      // no worries - just don't transform
      return -1;
    }
  }


  private static final class ReferenceInfo {
    private final int referenceValue;
    private final int sequenceLength;

    ReferenceInfo(int referenceValue, int sequenceLength) {
      super();
      this.referenceValue = referenceValue;
      this.sequenceLength = sequenceLength;
    }
  }


  @Override
  public void close() throws IOException {
    delegateReader.close();
  }
}