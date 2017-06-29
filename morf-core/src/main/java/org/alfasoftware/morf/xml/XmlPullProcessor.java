/* Copyright 2017 Alfa Financial Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.alfasoftware.morf.xml;

import org.xmlpull.v1.XmlPullParser;


/**
 * Parent class for XML data readers that are based on pull processing.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
class XmlPullProcessor {

  /**
   * Pull parser allows us to pull data from a single pass over the XML reader.
   */
  protected final XmlPullParser xmlPullParser;


  /**
   * Create a new pull processor using the provided pull parser.
   *
   * @param xmlPullParser The pull parser to use
   */
  public XmlPullProcessor(XmlPullParser xmlPullParser) {
    super();
    this.xmlPullParser = xmlPullParser;
  }


  /**
   * Reads the next tag from the pull parser and throws an exception if its name does not
   * match <var>expectedTagName</var>.
   *
   * @param expectedTagName The tag name expected
   */
  protected void readTag(String expectedTagName) {
    XmlPullProcessor.readTag(xmlPullParser, expectedTagName);
  }


  /**
   * Reads the next tag from the pull parser and throws an exception if its name does not
   * match <var>expectedTagName</var>.
   *
   * @param xmlPullParser The pull parser to read from
   * @param expectedTagName The tag name expected
   */
  public static void readTag(XmlPullParser xmlPullParser, String expectedTagName) {
    // Look for any start tag event
    int event;
    try {
      do {
        event = xmlPullParser.next();
      } while (event == XmlPullParser.TEXT || event == XmlPullParser.END_TAG);
    } catch (Exception e) {
      throw new RuntimeException("Error reading data from the XML pull parser", e);
    }

    if (event == XmlPullParser.START_TAG) {
      if (!expectedTagName.equals(xmlPullParser.getName())) {
        throw new IllegalArgumentException("Expected tag [" + expectedTagName + "] but got [" + xmlPullParser.getName() + "]");
      }

    } else if (event == XmlPullParser.END_DOCUMENT) {
      throw new IllegalStateException("Unexpected end of document while looking for tag [" + expectedTagName + "]");

    } else {
      throw new IllegalStateException("Expecting a tag but found [" + xmlPullParser.getText() + "]");
    }
  }


  /**
   * Reads the next tag name from the XML parser so long as it lies within the parent tag name.
   * If the close tag event for the parent is read this method will return null. Otherwise it
   * returns the name of the tag read.
   *
   * @param parentTagName The enclosing tag that forms the limit for the read operation.
   * @return The next tag name or null if there are no more tags to read inside the specified parent.
   */
  protected String readNextTagInsideParent(String parentTagName) {
    int event;
    try {
      do {
        event = xmlPullParser.next();
      } while (event == XmlPullParser.TEXT || event == XmlPullParser.END_TAG && !xmlPullParser.getName().equals(parentTagName));
    } catch (Exception e) {
      throw new RuntimeException("Error reading data from the XML pull parser", e);
    }

    if (event == XmlPullParser.START_TAG) {
      return xmlPullParser.getName();

    } else if (event == XmlPullParser.END_TAG) {
      return null;

    } else if (event == XmlPullParser.END_DOCUMENT) {
      throw new IllegalStateException("Unexpected end of document while looking for a tag inside [" + parentTagName + "]");

    } else {
      throw new IllegalStateException("Expecting a tag inside [" + parentTagName + "] but got [" + xmlPullParser.getText() + "]");
    }
  }

}
