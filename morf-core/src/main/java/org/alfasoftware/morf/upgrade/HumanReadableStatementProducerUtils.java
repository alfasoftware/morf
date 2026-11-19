package org.alfasoftware.morf.upgrade;

import com.google.common.annotations.VisibleForTesting;

public class HumanReadableStatementProducerUtils {

  public String sanitise(String version) {
    return version.replaceAll("^v", "").replaceAll("\\.r$", "");
  }

  /**
   * Gets the version the upgrade {@code step} belongs in.
   * First attempts to pull the version from the {@code @Version}
   * annotation, otherwise from the package name.
   *
   * @param step the upgrade step.
   * @return the version the upgrade step belongs in.
   */
  public String getUpgradeStepVersion(UpgradeStep step) {
    Version versionAnnotation = step.getClass().getAnnotation(Version.class);
    if (versionAnnotation!=null) {
      return "v".concat(versionAnnotation.value());
    }

    String version = step.getClass().getPackage().getName();
    version = version.substring(version.lastIndexOf('.') + 1);
    return version.replace('_', '.');
  }


  /**
   * Compare two version strings. This differs from natural ordering
   * as a version of 5.3.27 is higher than 5.3.3.
   * @param str1 One version string to compare
   * @param str2 The other version string to compare
   * @return a negative integer, zero, or a positive integer as the
   *         first argument is less than, equal to, or greater than the
   *         second.   */
  @VisibleForTesting
  public Integer versionCompare(String str1, String str2) {
    String[] vals1 = str1.split("\\.");
    String[] vals2 = str2.split("\\.");

    // set index to first non-equal ordinal or length of shortest version string
    int i = 0;
    while (i < vals1.length && i < vals2.length && vals1[i].equals(vals2[i])) {
      i++;
    }
    // compare first non-equal ordinal number
    if (i < vals1.length && i < vals2.length) {
      try {
        int diff = Integer.valueOf(vals1[i]).compareTo(Integer.valueOf(vals2[i]));
        return Integer.signum(diff);
      } catch (NumberFormatException e) {
        return Integer.signum(vals1[i].compareTo(vals2[i]));
      }
    }
    // the strings are equal or one string is a substring of the other
    // e.g. "1.2.3" = "1.2.3" or "1.2.3" < "1.2.3.4"
    else {
      return Integer.signum(vals1.length - vals2.length);
    }
  }
}
