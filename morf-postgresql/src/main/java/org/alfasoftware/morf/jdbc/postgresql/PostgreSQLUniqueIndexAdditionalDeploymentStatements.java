package org.alfasoftware.morf.jdbc.postgresql;

import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toSet;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Joiner;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Range;

/**
 * Helper class for producing additional supporting constraints
 * for unique indexes containing nullable columns.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
class PostgreSQLUniqueIndexAdditionalDeploymentStatements {

    private static final int NULLABLE_COLUMNS_CUTOFF = 2;

    public static final String INDEX_COMMENT_LABEL = "NULLABLE";

    private static final String INDEX_NAME_SUFFIX = "$null";
    private static final char MAGIC_GLUE = '\u00A7';   //This is the section symbol §

    private static final Pattern ADDITIONAL_INDEX_NAME_MATCHER = Pattern.compile("(.*)\\Q" + INDEX_NAME_SUFFIX + "\\E(\\d*)");

    private static final Log log = LogFactory.getLog(PostgreSQLUniqueIndexAdditionalDeploymentStatements.class);

    private final Table table;
    private final Index index;

    private final List<String> nullableIndexColumns;

    private final Strategy strategy;

    public PostgreSQLUniqueIndexAdditionalDeploymentStatements(Table table, Index index) {
      this.table = table;
      this.index = index;

      this.nullableIndexColumns = extractNullableIndexColumns(table, index);

      // having too many nullable columns in a unique index would be expensive!
      this.strategy = nullableIndexColumns.size() <= NULLABLE_COLUMNS_CUTOFF
          ? new UsingConstellations()
          : new UsingConcatenation();
    }


    /**
     * Different type of indexes use different implementations of {@link Strategy} to solve the problem.
     */
    private interface Strategy {

      public Iterable<String> createIndexStatements(String prefixedTableName);

      public Iterable<String> renameIndexStatements(String prefixedTableName, String fromIndexName, String toIndexName);

      public Iterable<String> dropIndexStatements(String prefixedTableName);

      public boolean healIndexStatementsNeeded(Collection<AdditionalIndexInfo> additionalConstraintIndexInfos);

      public Iterable<String> healIndexStatements(Collection<AdditionalIndexInfo> additionalConstraintIndexInfos, String prefixedTableName);
    }


    /**
     * Strategy, which creates an additional unique index for each different constellation
     * of the nullable columns from the unique index. This is the cleanest solution, which
     * also allows for some interesting database optimisations, and may potentially avoid
     * some pitfalls.
     *
     * Sample result (with no non-nullable columns available):
     * <pre>
     * CREATE UNIQUE INDEX Test_NK ON schema.Test (stringField);
     * CREATE UNIQUE INDEX Test_NK$null0 ON schema.Test ((0)) WHERE stringField IS NULL;
     * </pre>
     *
     * Sample result (floatField being the only non-nullable column):
     * <pre>
     * CREATE UNIQUE INDEX Test_1 ON schema.Test (intField, floatField);
     * CREATE UNIQUE INDEX Test_1$null0 ON schema.Test (floatField) WHERE intField IS NULL;
     * </pre>
     *
     * Sample result (floatField being the only non-nullable column):
     * <pre>
     * CREATE UNIQUE INDEX indexName ON testschema.Test (stringField, intField, floatField, dateField);
     * CREATE UNIQUE INDEX indexName$null0 ON testschema.Test (intField, floatField, dateField) WHERE stringField IS NULL;
     * CREATE UNIQUE INDEX indexName$null01 ON testschema.Test (floatField, dateField) WHERE stringField IS NULL AND intField IS NULL;
     * CREATE UNIQUE INDEX indexName$null1 ON testschema.Test (stringField, floatField, dateField) WHERE intField IS NULL;
     * </pre>
     *
     * Note: This strategy can also easily handle trivial cases: non-unique indexes, non-nullable unique indexes, etc.
     */
    private class UsingConstellations implements Strategy {

      private final List<Iterable<Integer>> constellations;


      public UsingConstellations() {
        this.constellations = ImmutableList.copyOf(enumerateNullableIndexColumns(FluentIterable.of(), Range.closed(0, nullableIndexColumns.size())));
      }


      @Override
      public Iterable<String> createIndexStatements(String prefixedTableName) {
        return FluentIterable.from(constellations)
            .transformAndConcat(nullColumns -> createIndexStatement(prefixedTableName, nullColumns));
      }


      private Iterable<String> createIndexStatement(String prefixedTableName, Iterable<Integer> nullColumns) {
        final Iterator<Integer> nullColumnsIterator = nullColumns.iterator();
        String nextNullColumn = nullableIndexColumns.get(nullColumnsIterator.next());

        final List<String> indexColumns = new ArrayList<>();
        final List<String> whereColumns = new ArrayList<>();
        for (String column : index.columnNames()) {
          if (column.equals(nextNullColumn)) {
            // add to the where clause
            whereColumns.add(column);

            // move to the next null column
            nextNullColumn = nullColumnsIterator.hasNext()
                ? nullableIndexColumns.get(nullColumnsIterator.next())
                : null;
          }
          else {
            // add to the indexed columns
            indexColumns.add(column);
          }
        }

        // no columns left to be indexed?
        if (indexColumns.isEmpty()) {
          // index needs a value to index, use a constant value
          indexColumns.add("(0)");
        }

        String fullIndexName = makeIndexName(nullColumns);
        String indexHash = makeIndexHash(nullColumns);

        String createStatement =
            "CREATE UNIQUE INDEX " + fullIndexName + " ON " + prefixedTableName
            + " (" + Joiner.on(", ").join(indexColumns) + ")"
            + " WHERE " + Joiner.on(" IS NULL AND ").join(whereColumns) + " IS NULL";

        String commentStatement =
            "COMMENT ON INDEX " + fullIndexName + " IS '" + SqlDialect.REAL_NAME_COMMENT_LABEL + ":[" + indexHash + "]'";

        return ImmutableList.of(createStatement, commentStatement);
      }


      @Override
      public Iterable<String> renameIndexStatements(String prefixedTableName, String fromIndexName, String toIndexName) {
        return FluentIterable.from(constellations)
            .transformAndConcat(nullColumns -> renameIndexStatements(prefixedTableName, fromIndexName, toIndexName, nullColumns));
      }


      private Iterable<String> renameIndexStatements(String prefixedTableName, String fromIndexName, String toIndexName, Iterable<Integer> nullColumns) {
        String indexNameSuffix = makeIndexSuffix(nullColumns);
        String alterStatement = "ALTER INDEX IF EXISTS " + prefixedTableName + fromIndexName + indexNameSuffix + " RENAME TO " + toIndexName + indexNameSuffix;
        return ImmutableList.of(alterStatement);
      }


      @Override
      public Iterable<String> dropIndexStatements(String prefixedTableName) {
        return FluentIterable.from(constellations)
            .transformAndConcat(nullColumns -> dropIndexStatements(prefixedTableName, nullColumns));
      }


      private Iterable<String> dropIndexStatements(@SuppressWarnings("unused") String prefixedTableName, Iterable<Integer> nullColumns) {
        String dropStatement = "DROP INDEX IF EXISTS " + makeIndexName(nullColumns);
        return ImmutableList.of(dropStatement);
      }


      @Override
      public boolean healIndexStatementsNeeded(Collection<AdditionalIndexInfo> additionalConstraintIndexInfos) {
        Set<ImmutablePair<String, String>> existingIndexes = FluentIterable.from(additionalConstraintIndexInfos)
          .transform(info -> new ImmutablePair<>(info.getIndexName(), info.getIndexHash()))
          .toSet(); // removes duplicates

        Set<ImmutablePair<String, String>> expectedIndexes = FluentIterable.from(constellations)
          .transform(nullColumns -> new ImmutablePair<>(makeIndexName(nullColumns).toLowerCase(), makeIndexHash(nullColumns)))
          .toSet();

        return !expectedIndexes.equals(existingIndexes);
      }


      @Override
      public Iterable<String> healIndexStatements(Collection<AdditionalIndexInfo> additionalConstraintIndexInfos, String prefixedTableName) {
        // For simplicity, let's just drop all the existing indexes and create new ones, which will always work.
        // It could be that some of the indexes could perhaps be kept, but currently the chances of that are expected to be tiny.
        // To keep some of the existing indexes, we would need to work out which ones to drop, which ones to keep, and which ones to create.

        Set<String> dropOldIndexes = FluentIterable.from(additionalConstraintIndexInfos)
          .transform(info -> "DROP INDEX IF EXISTS " + info.getIndexName())
          .toSet(); // removes duplicates

        return FluentIterable.from(dropOldIndexes)
            .append(createIndexStatements(prefixedTableName));
      }


      private String makeIndexSuffix(Iterable<Integer> nullColumns) {
        return INDEX_NAME_SUFFIX + Joiner.on("").join(nullColumns);
      }


      private String makeIndexName(Iterable<Integer> nullColumns) {
        return index.getName() + makeIndexSuffix(nullColumns);
      }


      private String makeIndexHash(Iterable<Integer> nullColumns) {
        return calculateIndexHashVariation(index.columnNames(), nullableIndexColumns, nullColumns);
      }
    }


    /**
     * Strategy, suitable for many-nullable-columns unique indexes, which uses
     * a single concatenated value of all columns to achieve uniqueness. This
     * has potential downsides, for example, it requires {@link MAGIC_GLUE}, a
     * magic value joiner character.
     *
     * Sample result (floatField being the only non-nullable column):
     * <pre>
     * CREATE UNIQUE INDEX indexName ON testschema.Test (stringField, intField, floatField, dateField);
     * CREATE UNIQUE INDEX indexName$null ON testschema.Test ((stringField ||'§'|| intField ||'§'|| floatField ||'§'|| dateField)) WHERE stringField IS NULL OR intField IS NULL OR dateField IS NULL;
     * </pre>
     *
     * Important: This strategy does not try to handle trivial cases: non-unique indexes, non-nullable unique indexes, etc.
     */
    private class UsingConcatenation implements Strategy {

      public UsingConcatenation() {
        if (nullableIndexColumns.isEmpty()) {
          throw new UnsupportedOperationException("This strategy does not handle trivial cases; use a different one");
        }
      }

      @Override
      public Iterable<String> createIndexStatements(String prefixedTableName) {
        String fullIndexName = index.getName() + INDEX_NAME_SUFFIX;
        String indexHash = calculateIndexHash(index.columnNames(), nullableIndexColumns);

        String createStatement =
            "CREATE UNIQUE INDEX " + fullIndexName + " ON " + prefixedTableName
            + " ((" + Joiner.on(" ||'" + MAGIC_GLUE + "'|| ").join(index.columnNames()) + "))"
            + " WHERE " + Joiner.on(" IS NULL OR ").join(nullableIndexColumns) + " IS NULL";

        String commentStatement =
            "COMMENT ON INDEX " + fullIndexName + " IS '" + SqlDialect.REAL_NAME_COMMENT_LABEL + ":[" + indexHash + "]'";

        return ImmutableList.of(createStatement, commentStatement);
      }


      @Override
      public Iterable<String> renameIndexStatements(String prefixedTableName, String fromIndexName, String toIndexName) {
        String alterStatement = "ALTER INDEX IF EXISTS " + prefixedTableName + fromIndexName + INDEX_NAME_SUFFIX + " RENAME TO " + toIndexName + INDEX_NAME_SUFFIX;
        return ImmutableList.of(alterStatement);
      }


      @Override
      public Iterable<String> dropIndexStatements(String prefixedTableName) {
        String dropStatement = "DROP INDEX IF EXISTS " + index.getName() + INDEX_NAME_SUFFIX;
        return ImmutableList.of(dropStatement);
      }


      @Override
      public boolean healIndexStatementsNeeded(Collection<AdditionalIndexInfo> additionalConstraintIndexInfos) {
        String fullIndexName = (index.getName() + INDEX_NAME_SUFFIX).toLowerCase();
        String indexHash = calculateIndexHash(index.columnNames(), nullableIndexColumns);

        if (additionalConstraintIndexInfos.size() != 1) {
          return true;
        }

        return !additionalConstraintIndexInfos.stream()
          .allMatch(info -> info.getIndexName().equals(fullIndexName) && info.getIndexHash().equals(indexHash));
      }


      @Override
      public Iterable<String> healIndexStatements(Collection<AdditionalIndexInfo> additionalConstraintIndexInfos, String prefixedTableName) {
        Set<String> dropOldIndexes = FluentIterable.from(additionalConstraintIndexInfos)
          .transform(info -> "DROP INDEX IF EXISTS " + info.getIndexName())
          .toSet(); // removes duplicates

        return FluentIterable.from(dropOldIndexes)
            .append(createIndexStatements(prefixedTableName));
      }
    }


    public Iterable<String> createIndexStatements(String prefixedTableName) {
      return strategy.createIndexStatements(prefixedTableName);
    }


    public Iterable<String> renameIndexStatements(String prefixedTableName, String fromIndexName, String toIndexName) {
      return strategy.renameIndexStatements(prefixedTableName, fromIndexName, toIndexName);
    }


    public Iterable<String> dropIndexStatements(String prefixedTableName) {
      return strategy.dropIndexStatements(prefixedTableName);
    }


    public Iterable<String> healIndexStatements(Collection<AdditionalIndexInfo> additionalConstraintIndexInfos, String prefixedTableName) {
      return healIndexStatementsNeeded(additionalConstraintIndexInfos)
          ? strategy.healIndexStatements(additionalConstraintIndexInfos, prefixedTableName)
          : ImmutableList.of();
    }


    private boolean healIndexStatementsNeeded(Collection<AdditionalIndexInfo> additionalConstraintIndexInfos) {
      boolean healIndexStatementsNeeded = strategy.healIndexStatementsNeeded(additionalConstraintIndexInfos);
      if (log.isDebugEnabled()) log.debug(strategy.getClass().getSimpleName() + ".healIndexStatementsNeeded: " + healIndexStatementsNeeded + ", " + additionalConstraintIndexInfos);
      return healIndexStatementsNeeded;
    }


    /**
     * For a unique index with at least one nullable column,
     * lists all the nullable column names of the index.
     */
    private static List<String> extractNullableIndexColumns(Table table, Index index) {
      // this only concerns unique indexes
      if (!index.isUnique()) {
        return ImmutableList.of();
      }

      // get the list of nullable columns
      final Set<String> nullableColumnNames = table.columns().stream()
        .filter(Column::isNullable)
        .map(Column::getName)
        .collect(toSet());

      final List<String> nullableIndexColumns = index.columnNames().stream()
        .filter(nullableColumnNames::contains)
        .collect(toList());

      // this only concerns nullable columns in those unique indexes
      if (nullableIndexColumns.isEmpty()) {
        return ImmutableList.of();
      }

      // allow logging for full analysis
      if (log.isDebugEnabled()) log.debug("Unique index [" + index.getName() + "] contains " + nullableIndexColumns.size() + " nullable columns: " + nullableIndexColumns);

      return nullableIndexColumns;
    }


    /**
     * Produces a "power set" from the given range of integers. The empty subset is excluded.
     *
     * For range 0-0 produces empty iterable.
     * For range 0-1 produces iterable with (1).
     * For range 0-2 produces iterable with (1), (2), (1, 2).
     * For range 0-3 produces iterable with (1), (2), (3), (1,2), (1,3), (2,3), (1,2,3).
     * Etc.
     */
    private static Iterable<Iterable<Integer>> enumerateNullableIndexColumns(FluentIterable<Integer> nullColumns, Range<Integer> nullableColumns) {
      FluentIterable<Iterable<Integer>> constellations = FluentIterable.of();

      for (int i = nullableColumns.lowerEndpoint(); i < nullableColumns.upperEndpoint(); i++) {
        // add the next null column to the other null columns
        FluentIterable<Integer> newNullColumns = nullColumns.append(ImmutableList.of(i));

        // add this constellation to the results
        constellations = constellations.append(ImmutableList.of(newNullColumns));

        // recurse from this constellation
        constellations = constellations.append(enumerateNullableIndexColumns(newNullColumns, Range.closed(i + 1, nullableColumns.upperEndpoint())));
      }

      return constellations;
    }


    private static String calculateIndexHash(Iterable<String> allColumnNames, Iterable<String> allNullableColumnNames) {
      // List all the index column names, in their appearance order.
      // This ensures the hash changes whenever any columns change in the index.
      // Note: This should be the same for all index constellations.
      final String allColumns = Joiner.on('|').join(allColumnNames);

      // List all the nullable index column names, in their appearance order.
      // This ensures the hash changes whenever any column nullability changes.
      // Note: This should be the same for all index constellations.
      final String allNullableColumns = Joiner.on('|').join(allNullableColumnNames);

      // Put it all together
      return DigestUtils.md5Hex(allColumns + '/' + allNullableColumns);
    }


    private static String calculateIndexHashVariation(Iterable<String> allColumnNames, Iterable<String> allNullableColumnNames, Iterable<?> nulledColumnNames) {
      // List the columns picked to be nulled within this particular constellation.
      // Note: This should tell various constellations/variations apart.
      final String nulledColumns = Joiner.on('|').join(nulledColumnNames);

      // Put it all together
      return calculateIndexHash(allColumnNames, allNullableColumnNames) + '/' + DigestUtils.md5Hex(nulledColumns);
    }


    /**
     * Recognises a name of additional supporting constraint index.
     *
     * @param indexName Index name as read from the database.
     * @param indexHash Index hash as read from the index comment.
     * @param indexResultSet Further information available on the index.
     * @return Optional.empty() if the index name is not an additional supporting constraint.
     * Optional.isPresent() if the index name is an additional supporting constraint.
     * This return should then be used for @TODO to validate and heal the supporting indexes.
     */
    public static Optional<AdditionalIndexInfo> matchAdditionalIndex(String indexName, String indexHash) throws SQLException {
      Matcher matcher = ADDITIONAL_INDEX_NAME_MATCHER.matcher(indexName);
      if (matcher.matches()) {
        String baseName = matcher.group(1);
        String suffixName = matcher.group(2);
        return Optional.of(new AdditionalIndexInfo(indexName, baseName, suffixName, indexHash));
      }
      return Optional.empty();
    }


    /**
     * Simple POJO for holding additional info about indexes and their columns,
     * as provided by the JDBC.
     */
    public static class AdditionalIndexInfo {

      private final String indexName;
      private final String baseName;
      private final String suffixName;
      private final String indexHash;

      public AdditionalIndexInfo(String indexName, String baseName, String suffixName, String indexHash) throws SQLException {
        this.indexName = indexName.toLowerCase();
        this.baseName = baseName.toLowerCase();
        this.suffixName = suffixName;
        this.indexHash = indexHash;
      }

      public String getIndexName() {
        return indexName;
      }

      public String getBaseName() {
        return baseName;
      }

      public String getSuffixName() {
        return suffixName;
      }

      public String getIndexHash() {
        return indexHash;
      }

      @Override
      public String toString() {
        return "{" + indexName + ", " + baseName + "/" + INDEX_NAME_SUFFIX + "/" + suffixName + ", " + indexHash + "}";
      }
    }
  }
