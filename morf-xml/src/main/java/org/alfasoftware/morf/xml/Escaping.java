package org.alfasoftware.morf.xml;

class Escaping {

  /**
   * Un-  escape strings from XML format back to internal format:
   * \0 -> 0
   * \\ -> \
   */
  static String unescapeCharacters(String attributeValue) {
    if (attributeValue == null) return null;

    char[] attributeChars = attributeValue.toCharArray();
    StringBuilder result = new StringBuilder(attributeChars.length);
    int from = 0;
    int len = 0;
    int i = 0;

    while (i < attributeChars.length) {

      // look for the escape character
      if (attributeChars[i] == '\\') {
        result.append(attributeChars, from, len);
        len = 0;

        switch(attributeChars[i+1]) {
          case '\\' :
            result.append('\\');
            i++;
            break;
          case '0' :
            result.append((char) 0);
            i++;
            break;
          case 'u' :
            result.append(decodeEscapedCharacter(attributeChars, i));
            i+=5;
            break;
          default: throw new IllegalStateException("Illegal escape sequence at char [" + i + "] in [" + attributeValue + "]");
        }
        from = i+1;

      } else {
        len++;
      }
      i++;
    }

    result.append(attributeChars, from, len);
    return result.toString();
  }


  private static char decodeEscapedCharacter(char[] attributeChars, int i) {
    // read the 4 digits into a string
    if (attributeChars.length < i+4) {
      throw new IllegalStateException("Illegal escape sequence at char [" + i + "] in [" + new String(attributeChars) + "]");
    }
    int charCode;
    try {
      charCode = Integer.parseInt(new String(attributeChars, i+2, 4), 16);
    } catch (NumberFormatException nfe) {
      throw new IllegalStateException("Illegal escape sequence at char [" + i + "] in [" + new String(attributeChars) + "]", nfe);
    }

    return (char) charCode;
  }


  /**
   * Escape for writing to XML, from the internal format.
   */
  static String escapeCharacters(String recordValue) {
    // Escape any null characters - these aren't valid XML so in format version 3 we escape these as \0
    // We also, consequently, have to escape backslashes as \\
    // can't use String::replace as it doesn't like null characters
    int last = 0;
    StringBuilder escapedValue = new StringBuilder(recordValue.length());
    for (int i=0; i<recordValue.length(); i++) {
      if (!isCharValidForXml(recordValue.charAt(i))) {
        escapedValue.append(recordValue.substring(last, i));
        escapedValue.append(String.format("\\u%04x", (int)recordValue.charAt(i)));
        last=i+1;
      }
      if (recordValue.charAt(i) == '\\') {
        escapedValue.append(recordValue.substring(last, i));
        escapedValue.append("\\\\");
        last=i+1;
      }
    }
    escapedValue.append(recordValue.substring(last, recordValue.length()));

    return escapedValue.toString();
  }


  /**
   * https://www.w3.org/TR/xml/#NT-Char
   */
  static boolean isCharValidForXml(int c) {
    return
        c >= 0x20 && c <= 0xd7ff
        ||
        c >= 0xe000 && c <= 0xfffd
        ||
        c == 0x9 || c == 0xd || c == 0xa;
  }
}

