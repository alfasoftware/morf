package org.alfasoftware.morf.upgrade;

import static org.alfasoftware.morf.metadata.DataSetUtils.dataSetProducer;
import static org.alfasoftware.morf.metadata.DataSetUtils.record;
import static org.alfasoftware.morf.sql.InsertStatement.insert;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.upgradeAuditTable;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Set;

import javax.inject.Inject;

import org.alfasoftware.morf.dataset.DataSetConnector;
import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.DatabaseDataSetConsumer;
import org.alfasoftware.morf.jdbc.RuntimeSqlException;
import org.alfasoftware.morf.jdbc.SqlScriptExecutorProvider;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.sql.InsertStatement;
import org.alfasoftware.morf.testing.DatabaseSchemaManager;
import org.alfasoftware.morf.testing.TestingDataSourceModule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.MethodRule;

/**
 * Test for {@link DatabaseUpgradePathValidationService}
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2023
 */
public class TestDatabaseUpgradePathValidationService {

  @Rule
  public MethodRule injectorRule = new InjectMembersRule(new TestingDataSourceModule());

  @Inject
  private SqlScriptExecutorProvider sqlScriptExecutorProvider;
  @Inject
  private DatabaseSchemaManager schemaManager;
  @Inject
  private DatabaseDataSetConsumer databaseDataSetConsumer;
  @Inject
  private ConnectionResources connectionResources;

  @Inject
  private DatabaseUpgradePathValidationService databaseUpgradePathValidationService;

  private final Schema schema = SchemaUtils.schema(upgradeAuditTable());

  private final DataSetProducer dataSet = dataSetProducer(schema)
      .table(upgradeAuditTable().getName(),
          record()
              .setString("upgradeUUID", "003a64f7-3f9c-4624-8162-31606b9dad8b")
              .setString("description", "Dummy upgrade 1")
              .setLong("appliedTime", 20231113125538L),
          record()
              .setString("upgradeUUID", "ead354fd-a987-48dd-bd42-e1cebf9a3905")
              .setString("description", "Dummy upgrade 2")
              .setLong("appliedTime", 20231113125724L)
      );


  @Before
  public void setup() {
    schemaManager.dropAllTables();
    schemaManager.mutateToSupportSchema(schema, DatabaseSchemaManager.TruncationBehavior.ALWAYS);
    new DataSetConnector(dataSet, databaseDataSetConsumer).connect();
  }


  @After
  public void tearDown() {
    schemaManager.invalidateCache();
  }


  /**
   * Test that the initialisation SQL produced provides an optimistic locking mechanism which prevents duplicate
   * executions of the same script.
   */
  @Test
  public void testInitialisationSql() {
    // Given
    List<String> initialisationSql = databaseUpgradePathValidationService.getPathValidationSql(2); // 2 records added to UpgradeAudit in test setup

    // When
    // Execute initialisation SQL for the first time (perform an upgrade)
    sqlScriptExecutorProvider.get().execute(initialisationSql);

    // Add a record to UpgradeAudit and drop the zzzUpgradeStatus table after first run as this is what would happen in an actual upgrade
    addUpgradeAuditRecord();
    dropUpgradeStatusTable();

    // Then
    RuntimeSqlException exception = assertThrows(RuntimeSqlException.class, () -> sqlScriptExecutorProvider.get().execute(initialisationSql));
    // SQL State 23505 indicates a unique constraint violation on H2/PGSQL and 23000 on Oracle, so match based on either
    String upgradeStatusUniqueConstraintViolationRegex = "Error executing SQL \\[INSERT INTO [A-Za-z0-9]*[_.]?zzzUpgradeStatus .*? SQL state \\[(23505|23000)]";
    assertTrue("Should have been a unique constraint violation exception thrown", exception.getMessage().matches(upgradeStatusUniqueConstraintViolationRegex));
  }


  private void dropUpgradeStatusTable() {
    schemaManager.invalidateCache();
    schemaManager.dropTablesIfPresent(Set.of(UpgradeStatusTableService.UPGRADE_STATUS));
  }


  private void addUpgradeAuditRecord() {
    InsertStatement insertIntoUpgradeAudit = insert().into(tableRef(upgradeAuditTable().getName()))
        .values(
            literal("3e3f4bfd-2ab6-45f1-9deb-06a33de150a2").as("upgradeUUID"),
            literal("Dummy upgrade 3").as("description"),
            literal(20231113130954L).as("appliedTime")
        ).build();
    sqlScriptExecutorProvider.get().execute(connectionResources.sqlDialect().convertStatementToSQL(insertIntoUpgradeAudit));
  }
}
