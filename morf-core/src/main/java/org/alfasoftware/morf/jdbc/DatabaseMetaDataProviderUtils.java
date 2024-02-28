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

package org.alfasoftware.morf.jdbc;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

/**
 * Common methods for use by database metadata providers.
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class DatabaseMetaDataProviderUtils {

  /**
   * Regex for extracting the autonum start from column comments
   */
  private static final Pattern AUTONUM_START_REGEX = Pattern.compile("AUTONUMSTART:\\[(\\d+)\\]");
  /**
   * Regex for extracting the data type from column comments
   */
  private static final Pattern DATA_TYPE_REGEX = Pattern.compile("TYPE:\\[(\\w+)\\]");
  /**
   * Regex for matching ignored indexes
   */
  private static final Pattern IGNORE_INDEX_REGEX = Pattern.compile(".*_PRF\\d+$");

  /**
   * Get the auto increment start value (if available) from the column comments
   *
   * @param columnComment The column comment
   * @return the auto increment start value
   */
  public static int getAutoIncrementStartValue(String columnComment) {
    if (StringUtils.isNotEmpty(columnComment)) {
      Matcher matcher = AUTONUM_START_REGEX.matcher(columnComment);
      if (matcher.find()) {
        return Integer.parseInt(matcher.group(1));
      }
    }
    return -1;
  }


  public static Optional<String> getDataTypeFromColumnComment(String columnComment) {
    if(StringUtils.isNotEmpty(columnComment)) {
      Matcher matcher = DATA_TYPE_REGEX.matcher(columnComment);
      if(matcher.find()) {
        return Optional.of(matcher.group(1));
      }
    }
    return Optional.empty();
  }


  /**
   * Indexes which contain the suffix _PRF and a digit are to be ignored:
   * this allows performance testing of new index to verify their effect,
   * without breaking the schema checking.
   *
   * eg. Schedule_PRF1
   *
   * Also any indexes with a $ character will be ignored:
   * this allows technical indexes to be added to the schema.
   *
   * @param indexName The name of an index
   * @return Whether it should be ignored
   */
  public static boolean shouldIgnoreIndex(String indexName) {
    return IGNORE_INDEX_REGEX.matcher(indexName.toUpperCase()).matches()
        || indexName.indexOf('$') > -1;
  }
}
