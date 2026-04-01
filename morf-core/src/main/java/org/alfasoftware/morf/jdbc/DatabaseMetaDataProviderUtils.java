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

import static org.alfasoftware.morf.metadata.SchemaUtils.index;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder;
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
   * Label used in table comments to declare deferred indexes.
   */
  public static final String DEFERRED_COMMENT_LABEL = "DEFERRED";

  /**
   * Regex for extracting DEFERRED segments from table comments.
   * Matches: /DEFERRED:[indexName|col1,col2|unique] or /DEFERRED:[indexName|col1,col2]
   */
  private static final Pattern DEFERRED_INDEX_REGEX = Pattern.compile("DEFERRED:\\[([^\\]]+)\\]");


  /**
   * Parses deferred index declarations from a table comment string.
   * Each DEFERRED segment has the format: {@code DEFERRED:[indexName|col1,col2|unique]}
   * where the {@code |unique} suffix is optional.
   *
   * @param tableComment the full table comment string, may be null or empty.
   * @return a list of deferred indexes parsed from the comment, each with {@code isDeferred()=true}.
   */
  public static List<Index> parseDeferredIndexesFromComment(String tableComment) {
    if (StringUtils.isEmpty(tableComment)) {
      return Collections.emptyList();
    }

    List<Index> result = new ArrayList<>();
    Matcher matcher = DEFERRED_INDEX_REGEX.matcher(tableComment);
    while (matcher.find()) {
      String content = matcher.group(1);
      String[] parts = content.split("\\|");
      if (parts.length < 2) {
        continue;
      }

      String indexName = parts[0];
      List<String> columns = Arrays.asList(parts[1].split(","));
      boolean unique = parts.length >= 3 && "unique".equalsIgnoreCase(parts[2]);

      IndexBuilder builder = index(indexName).columns(columns).deferred();
      if (unique) {
        builder = builder.unique();
      }
      result.add(builder);
    }
    return result;
  }


  /**
   * Builds the DEFERRED comment segments for the given deferred indexes.
   * Each index produces a segment like {@code /DEFERRED:[indexName|col1,col2|unique]}.
   *
   * @param deferredIndexes the deferred indexes to serialize.
   * @return the concatenated DEFERRED segments, or an empty string if none.
   */
  public static String buildDeferredIndexCommentSegments(List<Index> deferredIndexes) {
    if (deferredIndexes == null || deferredIndexes.isEmpty()) {
      return "";
    }

    StringBuilder sb = new StringBuilder();
    for (Index idx : deferredIndexes) {
      sb.append("/").append(DEFERRED_COMMENT_LABEL).append(":[");
      sb.append(idx.getName());
      sb.append("|");
      sb.append(idx.columnNames().stream().collect(Collectors.joining(",")));
      if (idx.isUnique()) {
        sb.append("|unique");
      }
      sb.append("]");
    }
    return sb.toString();
  }


  /**
   * Indexes which contain the suffix _PRF and a digit are to be ignored:
   * this allows performance testing of new index to verify their effect,
   * without breaking the schema checking.
   *
   * eg. Schedule_PRF1
   *
   * @param indexName The name of an index
   * @return Whether it should be ignored
   */
  public static boolean shouldIgnoreIndex(String indexName) {
    return indexName.toUpperCase().matches(".*_PRF\\d+$");
  }
}
