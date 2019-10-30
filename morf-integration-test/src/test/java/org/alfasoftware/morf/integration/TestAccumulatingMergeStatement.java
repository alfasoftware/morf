package org.alfasoftware.morf.integration;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.InsertStatement.insert;
import static org.alfasoftware.morf.sql.MergeStatement.merge;
import static org.alfasoftware.morf.sql.SelectStatement.select;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.element.Function.coalesce;
import static org.alfasoftware.morf.sql.element.Function.sum;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Executors;

import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.SqlScriptExecutor;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.sql.MergeStatement;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.TableReference;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.DatabaseSchemaManager.TruncationBehavior;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.alfasoftware.morf.upgrade.LoggingSqlScriptVisitor;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.google.inject.Inject;
import com.google.inject.Provider;

/**
 * Tests accumulating MergeStatement behaviour using multi-threaded access.
 *
 * <p>This tests makes actual database connections and schema changes.<p>
 *
 * @author Copyright (c) Alfa Financial Software 2019
 */
public class TestAccumulatingMergeStatement {

  @Rule public InjectMembersRule injectMembersRule = new InjectMembersRule(new TestingDataSourceModule());

  private static final int THREADS = 10;
  private static final int LOOPS = 10;

  @Inject private Provider<DatabaseSchemaManager> schemaManager;
  @Inject private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Inject private ConnectionResources connectionResources;

  private final TableReference destinationTable = tableRef("Destination");

  private final List<Worker> workers = new ArrayList<>();

  @Before
  public void before() {
    schemaManager.get().mutateToSupportSchema(
      schema(
        table("destination")
          .columns(
            column("keyColumn", DataType.STRING, 3).primaryKey(),
            column("lastValue", DataType.DECIMAL,12, 2),
            column("totalValue", DataType.DECIMAL,12, 2))
      ),
      TruncationBehavior.ALWAYS);

    /*
     * We have to provide the initial record on Oracle, otherwise we get
     *   ORA-00001: unique constraint (DESTINATION_PK) violated
     */
    if (connectionResources.sqlDialect().getDatabaseType().identifier().equals("ORACLE")) {
      InsertStatement insertStatement = insert()
        .into(destinationTable)
        .values(literal("A").as("keyColumn"))
        .values(literal(0).as("lastValue"))
        .values(literal(0).as("totalValue"))
        .build();

      sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor())
        .execute(connectionResources.sqlDialect().convertStatementToSQL(insertStatement));
    }
  }


  @After
  public void cleanup() {
    for (Worker worker : workers) {
      worker.release();
    }
  }


  @Test(timeout = 120_000)
  public void testAccumulatingMerge() throws Exception {
    final ExecutorCompletionService<Boolean> executor = new ExecutorCompletionService<>(Executors.newFixedThreadPool(THREADS));

    // create all workers
    for (int i = 0; i < THREADS; i++) {
      workers.add(new Worker());
    }

    // start all workers
    for (Worker worker : workers) {
      executor.submit(worker);
    }

    // wait for each worker
    for (int i = 0; i < workers.size(); i++) {
      assertTrue(executor.take().get());
    }

    // check the results
    SelectStatement selectStatement =
        select()
          .fields(field("totalValue"))
          .from(destinationTable)
          .where(field("keyColumn").eq("A"))
          .build();

    Long result = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor())
      .executeQuery(selectStatement)
      .processWith(resultSet -> resultSet.next() ? resultSet.getLong(1) : null);

    assertEquals("Aggregated value", Long.valueOf((LOOPS-1) * LOOPS * THREADS / 2), result);
  }

  private final class Worker implements Callable<Boolean> {

    private final SqlScriptExecutor sqlExecutor = sqlScriptExecutorProvider.get(new LoggingSqlScriptVisitor());

    private Thread currentThread;

    private void release() {
      if (currentThread != null) {
        currentThread.interrupt();
      }
    }

    @Override
    public Boolean call() throws Exception {
      currentThread = Thread.currentThread();

      for(int i = 0; i < LOOPS; i++) {
        double random = Math.random();
        if (random > 0.5)
          newLambdaSolution(i);
        else
          oldLockingSolution(i);

        if (Thread.interrupted()) {
          return false;
        }
      }
      return true;
    }


    private void newLambdaSolution(int addValue) {
      MergeStatement mergeStatement =
          merge()
           .into(destinationTable)
           .tableUniqueKey(field("keyColumn"))
           .from(select()
                   .fields(literal("A").as("keyColumn"))
                   .fields(literal(addValue).as("lastValue"))
                   .fields(literal(addValue).as("totalValue"))
                   .build())
           .ifUpdating((overrides, values) -> overrides
             .set(values.input("lastValue").as("lastValue"))
             .set(values.input("totalValue").plus(values.existing("totalValue")).as("totalValue")))
           .build();

      sqlExecutor.execute(connectionResources.sqlDialect().convertStatementToSQL(mergeStatement));
    }


    private void oldLockingSolution(int addValue) {
      MergeStatement mergeStatement =
          merge()
           .into(destinationTable)
           .tableUniqueKey(field("keyColumn"))
           .from(oldSolutionSelectStatement(addValue))
           .build();

      sqlExecutor.execute(connectionResources.sqlDialect().convertStatementToSQL(mergeStatement));
    }


    /**
     * We need slightly different approach for different platforms
     * - Oracle automatically avoids collisions, but does not support FOR UPDATE
     * - MySQL needs explicit FOR UPDATE, and it can be on the summing main select
     * - H2 and PgSQL need explicit FOR UPDATE, but on a non-aggregating sub-select
     */
    private SelectStatement oldSolutionSelectStatement(int addValue) {
      switch(connectionResources.sqlDialect().getDatabaseType().identifier()) {
        case "H2":
        case "MY_SQL":
        case "PGSQL":
          AliasedField originalValue =
              select(field("totalValue"))
                .from(destinationTable)
                .where(field("keyColumn").eq("A"))
                .forUpdate() // row locking to prevent race conditions
                .build().asField();

          AliasedField accumulatedScalarValue =
              coalesce(sum(originalValue), literal(0))
                .plus(literal(addValue));

          return select()
                  .fields(literal("A").as("keyColumn"))
                  .fields(literal(addValue).as("lastValue"))
                  .fields(accumulatedScalarValue.as("totalValue"))
                  .build();

        default:
          AliasedField accumulatedFieldValue =
              coalesce(sum(destinationTable.field("totalValue")), literal(0))
                .plus(literal(addValue));

          return select()
                  .fields(literal("A").as("keyColumn"))
                  .fields(literal(addValue).as("lastValue"))
                  .fields(accumulatedFieldValue.as("totalValue"))
                  .from(destinationTable)
                  .where(field("keyColumn").eq("A"))
                  .build();
      }
    }
  }
}
