package org.alfasoftware.morf.xml;

import static java.util.Arrays.stream;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.BiConsumer;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.diff.Comparison;
import org.xmlunit.diff.ComparisonResult;
import org.xmlunit.diff.Diff;
import org.xmlunit.diff.DifferenceEvaluator;

class SameXmlFileAndLengths extends TypeSafeMatcher<File> {

  private final URL modelURL;

  SameXmlFileAndLengths(final URL modelURL) {
    this.modelURL = modelURL;
  }


  /**
   * Keeps track of the size of each file of the model extract.
   */
  private final Map<String, Long> fileToSize = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

  private final List<String> mismatches = new ArrayList<>();

  /**
   * @param file a morf extract in the from of a folder or a file
   * @return true if the file matches by XML content the control file
   */
  @Override
  protected boolean matchesSafely(File file) {
    try (ViewURLAsFile urlHandler = new ViewURLAsFile()) {
      File model = urlHandler.getFile(modelURL, null, null);

      fileToSize.clear();
      mismatches.clear();

      stream(model.listFiles()).filter(f -> isFileToBeConsidered(f.getName())).forEach(f -> fileToSize.put(f.getName(), f.length()));

      if (file.isDirectory()) {
        stream(model.listFiles()).filter(f -> isFileToBeConsidered(f.getName())).forEach(f -> checkForMismatches(model, f.getName(), toInputStream(f)));
      } else {
        forEachXmlEntry(file, (z, e) -> checkForMismatches(model, e.getName(), toInputStream(z, e)));
      }
    }

    for (Map.Entry<String, Long> unmatchedControlFiles : fileToSize.entrySet()) {
      mismatches.add(unmatchedControlFiles.getKey() + " file from control extract has not been matched by the extract under test");
    }

    return mismatches.isEmpty();
  }

  /**
   * Checks whether the table provided as a file exists in the control model and has equivalent XML content.
   */
  private void checkForMismatches(File model, String tablename, InputStream contentInputStream) {
    Long modelFileSize = fileToSize.remove(tablename);
    if (modelFileSize == null) {
      mismatches.add(tablename + " is not present in the control extract");
    } else {
      File modelTable = stream(model.listFiles()).filter(modelFile -> modelFile.getName().equalsIgnoreCase(tablename)).findAny().get();
      Diff diff = DiffBuilder.compare(contentInputStream).withTest(modelTable)
          .ignoreWhitespace()
          .withDifferenceEvaluator(new IgnoreMetadataAttributesCaseOnNameAndColumnsDifferenceEvaluator())
          .checkForSimilar()
          .ignoreComments()
          .build();
      diff.getDifferences().forEach(d -> mismatches.add(tablename + ": " + d.toString()));
    }
  }

  private InputStream toInputStream(File file) {
    try {
      return new FileInputStream(file);
    } catch (FileNotFoundException e) {
      throw new IllegalStateException(e);
    }
  }

  private InputStream toInputStream(ZipFile zipFile, ZipEntry zipEntry) {
    try {
      return zipFile.getInputStream(zipEntry);
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * Applies a Consumer to each xml file contained in the root folder of the provided zip.
   *
   * @param extract the zip file containing the cryo/morf extract
   * @param action  the action to perform on each xml file in the root folder of the extract
   */
  private void forEachXmlEntry(File extract, BiConsumer<ZipFile, ZipEntry> action) {

    try (ZipFile zipFile = new ZipFile(extract)) {
      Enumeration<? extends ZipEntry> zipEntries = zipFile.entries();
      while (zipEntries.hasMoreElements()) {
        ZipEntry zipEntry = zipEntries.nextElement();
        if (!zipEntry.isDirectory() && isFileToBeConsidered(zipEntry.getName())) {
          action.accept(zipFile, zipEntry);
        }
      }
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private boolean isFileToBeConsidered(String filename) {
    return filename.substring(filename.length() - 4).equalsIgnoreCase(".xml");
  }

  @Override
  public void describeTo(Description description) {
    description.appendText("cryo/morf extract with same-sized xml files outputted");
    mismatches.forEach(m -> description.appendText("\n" + m));
  }


  static class IgnoreMetadataAttributesCaseOnNameAndColumnsDifferenceEvaluator implements DifferenceEvaluator {

    @Override
    public ComparisonResult evaluate(Comparison comparison, ComparisonResult outcome) {
      if (outcome == ComparisonResult.EQUAL) return outcome; // only evaluate differences.

      if (comparison.getControlDetails().getXPath().startsWith("/table[1]/metadata")
          && (comparison.getControlDetails().getXPath().endsWith("@name") || comparison.getControlDetails().getXPath().endsWith("@columns"))) {
        if (comparison.getControlDetails().getValue().toString().equalsIgnoreCase(comparison.getTestDetails().getValue().toString())) {
          return ComparisonResult.SIMILAR;
        }
      }
      return outcome;
    }
  }
}
