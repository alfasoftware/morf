package org.alfasoftware.morf.xml;


import java.io.File;
import java.net.URL;

import org.hamcrest.Matcher;

/**
 * Hamcrest matchers to perform assertions on morf XML datasets.
 */
public class MorfXmlDatasetMatchers {

  /**
   * Matches two cryo/morf extract if they have the same xml files and each of them have the same row count.
   *
   * <p>Given the current structure of morf extracts it ignores folders.</p>
   *
   * <p><b>Limits</b>: currently the comparison demands the same order for records in a table between the model and the extract to check.
   * Also, attribute names are case sensitive.</p>
   *
   * @param modelURL URL of the Folder containing the control morf extract
   * @return a Matcher which will check the provided extract is xml content equal to the model
   */
  public static Matcher<File> sameXmlFileAndLengths(final URL modelURL) {
    return new SameXmlFileAndLengths(modelURL);
  }
}
