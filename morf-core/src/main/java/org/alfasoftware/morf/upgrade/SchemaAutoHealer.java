package org.alfasoftware.morf.upgrade;

import java.util.List;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaResource;

import com.google.common.collect.ImmutableList;

/**
 * This healer is intended for automated database modifications visible via the {@link Schema}.
 *
 * For example the names of indexes are available via the {@link Schema}, therefore changing them
 * requires both the DDL statements and also a modified {@link Schema}. This interface could be
 * used for such auto-healing of names of indexes.
 *
 * On the other hand, the names of primary key indexes are not generally available via the
 * {@link Schema}, and therefore cannot be healed via this interface, because this interface does
 * not have access to such invisible characteristics of the database.
 *
 * See {@link org.alfasoftware.morf.jdbc.SqlDialect#getSchemaConsistencyStatements(SchemaResource)},
 * another type of a database auto-healer, which is intended to heal characteristics of the database
 * not visible in the {@link Schema}.
 *
 * Also note that this type of healer is dialect-agnostic, all dialects will get equally affected.
 */
public interface SchemaAutoHealer {

  /**
   * Analyses given database schema,
   * and produces a set of statements to be run on the database
   * to arrive at a healed version of the that schema.
   *
   * @param sourceSchema Schema to be healed
   * @return Healing statements to be run, and the resulting healed schema.
   */
  public SchemaHealingResults analyseSchema(Schema sourceSchema);


  /**
   * Results of {@link SchemaAutoHealer#analyseSchema(Schema)}.
   */
  public interface SchemaHealingResults {

    /**
     * Statements required to be run on the database to arrive at {@link #getHealedSchema()}.
     */
    public List<String> getHealingStatements(SqlDialect dialect);

    /**
     * The auto-healed schema of the database, as it will be once the statements
     * from {@link #getHealingStatements(SqlDialect)} are run on that database.
     */
    public Schema getHealedSchema();
  }


  /**
   * Simply uses a no-op healer.
   * By no-op, we mean non-healing: the input is passed through as output.
   */
  public static final class NoOp implements SchemaAutoHealer {

    @Override
    public SchemaHealingResults analyseSchema(Schema sourceSchema) {
      return new SchemaHealingResults() {

        @Override
        public List<String> getHealingStatements(SqlDialect dialect) {
          return ImmutableList.of();
        }

        @Override
        public Schema getHealedSchema() {
          return sourceSchema;
        }
      };
    }
  }


  /**
   * Combines two {@link SchemaAutoHealer}s to run the second one on the results of the first one.
   */
  public static final class Combining implements SchemaAutoHealer {

    private final SchemaAutoHealer first;
    private final SchemaAutoHealer second;

    public Combining(SchemaAutoHealer first, SchemaAutoHealer second) {
      this.first = first;
      this.second = second;
    }


    @Override
    public SchemaHealingResults analyseSchema(Schema sourceSchema) {
      final SchemaHealingResults firstResults = first.analyseSchema(sourceSchema);
      final SchemaHealingResults secondResults = second.analyseSchema(firstResults.getHealedSchema());

      return new SchemaHealingResults() {

        @Override
        public List<String> getHealingStatements(SqlDialect dialect) {
          return ImmutableList.<String>builder()
              .addAll(firstResults.getHealingStatements(dialect))
              .addAll(secondResults.getHealingStatements(dialect))
              .build();
        }

        @Override
        public Schema getHealedSchema() {
          return secondResults.getHealedSchema();
        }
      };
    }

  }
}
