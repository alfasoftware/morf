package org.alfasoftware.morf.upgrade;

import static java.util.stream.Collectors.toList;
import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.idColumn;
import static org.alfasoftware.morf.metadata.SchemaUtils.index;
import static org.alfasoftware.morf.metadata.SchemaUtils.schema;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.metadata.SchemaUtils.versionColumn;
import static org.alfasoftware.morf.sql.SqlUtils.field;
import static org.alfasoftware.morf.sql.SqlUtils.literal;
import static org.alfasoftware.morf.sql.SqlUtils.tableRef;
import static org.alfasoftware.morf.sql.UpdateStatement.update;
import static org.alfasoftware.morf.sql.element.Function.leftPad;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.Random;

import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaHomology;
import org.alfasoftware.morf.metadata.SchemaUtils.IndexBuilder;
import org.alfasoftware.morf.metadata.SchemaUtils.TableBuilder;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.UpdateStatement;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class TestUpdateToCtasAdaptor {

  private static final Log log = LogFactory.getLog(TestUpdateToCtasAdaptor.class);

  private final UpgradeConfigAndContext upgradeConfigAndContext = new UpgradeConfigAndContext();


  @Test
  public void testSimpleCtas() {
    UpdateToCtasAdaptor adaptor = new UpdateToCtasAdaptor();
    upgradeConfigAndContext.setSchemaChangeToSchemaAdaptor(adaptor);

    Table someTable = table("SomeTable")
      .columns(
          idColumn(),
          column("someIntValue", DataType.DECIMAL, 13),
          column("someStrValue", DataType.STRING, 10),
          column("someDateValue", DataType.DATE),
          versionColumn())
      .indexes(
          index("SomeTable_1").columns("someIntValue", "someStrValue"),
          index("someStrValue_idx").columns("someStrValue"));

    Table randomTable1 = randomTable("R1");
    Table randomTable2 = randomTable("R2");
    Table randomTable3 = randomTable("R3");

    Schema initialSchema = schema(randomTable1, randomTable2, someTable, randomTable3);
    Schema targetSchema = initialSchema;
    List<UpgradeStep> steps = List.of(new UpdateSomeTable());

    Pair<List<SchemaChange>, List<SchemaChange>> result =
        findAndAdaptUpgradePath(initialSchema, targetSchema, steps);

    List<SchemaChange> originalChanges = result.getLeft();
    assertThat(originalChanges.size(), equalTo(1));

    assertThat(originalChanges.get(0), instanceOf(ExecuteStatement.class));
    ExecuteStatement executeStatement = (ExecuteStatement) originalChanges.get(0);
    assertThat(executeStatement.getStatement(), instanceOf(UpdateStatement.class));
    assertThat(executeStatement.getStatement().toString(), equalTo(
        "SQL UPDATE [[CtasDuringUpgrade{useForDatabaseType=<ANY>}]SomeTable]"
        + " SET [someIntValue + 1 AS someIntValue, LEFT_PAD(someStrValue, 5, \"0\") AS someStrValue]"));

    List<SchemaChange> adaptedChanges = result.getRight();
    assertThat(adaptedChanges.size(), equalTo(5));

    assertThat(adaptedChanges.get(0), instanceOf(AddTableFrom.class));
    AddTableFrom addTableFrom = (AddTableFrom) adaptedChanges.get(0);
    assertThat(addTableFrom.getTable().getName(), equalTo("CTAS_SomeTable"));
    assertThat(addTableFrom.getTable().primaryKey().toString(), equalTo("[Column-id-BIG_INTEGER---notNull-pk--0-]"));
    assertThat(addTableFrom.getTable().columns().toString(), equalTo(
        "[Column-id-BIG_INTEGER---notNull-pk--0-, "
        + "Column-someIntValue-DECIMAL-13-0-notNull---0-, "
        + "Column-someStrValue-STRING-10--notNull---0-, "
        + "Column-someDateValue-DATE---notNull---0-, "
        + "Column-version-INTEGER---null---0-0]"));
    assertThat(addTableFrom.getTable().indexes().toString(), equalTo("[]"));
    assertThat(addTableFrom.getTable().isTemporary(), equalTo(false));
    assertThat(addTableFrom.getSelectStatement().toString(), equalTo(
        "SQL SELECT [id, "
        + "CAST(someIntValue + 1 AS someIntValue AS DECIMAL(13, 0)),"
        + " CAST(LEFT_PAD(someStrValue, 5, \"0\") AS someStrValue AS STRING(10, 0)),"
        + " someDateValue, version] FROM [SomeTable]"));

    assertThat(adaptedChanges.get(1), instanceOf(RemoveTable.class));
    RemoveTable removeTable = (RemoveTable) adaptedChanges.get(1);
    assertThat(removeTable.getTable(), equalTo(someTable));

    assertThat(adaptedChanges.get(2), instanceOf(RenameTable.class));
    RenameTable renameTable = (RenameTable) adaptedChanges.get(2);
    assertThat(renameTable.getOldTableName(), equalTo("CTAS_"+someTable.getName()));
    assertThat(renameTable.getNewTableName(), equalTo(someTable.getName()));

    assertThat(adaptedChanges.get(3), instanceOf(AddIndex.class));
    AddIndex addIndex1 = (AddIndex) adaptedChanges.get(3);
    assertThat(addIndex1.getTableName(), equalTo(someTable.getName()));
    assertThat(addIndex1.getNewIndex().toString(), equalTo("Index-SomeTable_1--someIntValue,someStrValue"));

    assertThat(adaptedChanges.get(4), instanceOf(AddIndex.class));
    AddIndex addIndex2 = (AddIndex) adaptedChanges.get(4);
    assertThat(addIndex2.getTableName(), equalTo(someTable.getName()));
    assertThat(addIndex2.getNewIndex().toString(), equalTo("Index-someStrValue_idx--someStrValue"));
  }


  @Test
  public void testSimpleCtasWithOtherUpgrade() {
    UpdateToCtasAdaptor adaptor = new UpdateToCtasAdaptor();
    upgradeConfigAndContext.setSchemaChangeToSchemaAdaptor(adaptor);

    Table someTable = table("SomeTable")
      .columns(
          idColumn(),
          column("someIntValue", DataType.DECIMAL, 13),
          column("someStrValue", DataType.STRING, 10),
          column("someDateValue", DataType.DATE),
          versionColumn())
      .indexes(
          index("SomeTable_1").columns("someIntValue", "someStrValue"),
          index("someStrValue_idx").columns("someStrValue"));

    Table someTableUpgraded = table("SomeTable")
        .columns(
            idColumn(),
            column("someIntValue", DataType.DECIMAL, 13),
            column("someStrValue", DataType.STRING, 10),
            column("someBoolValue", DataType.BOOLEAN).nullable(),
            versionColumn())
        .indexes(
            index("SomeTable_1").columns("someIntValue", "someStrValue"),
            index("someBoolValue_idx").columns("someBoolValue").unique());

    Table randomTable1 = randomTable("R1");
    Table randomTable2 = randomTable("R2");
    Table randomTable3 = randomTable("R3");

    Schema initialSchema = schema(randomTable1, randomTable2, someTable, randomTable3);
    Schema targetSchema = schema(randomTable1, randomTable2, randomTable3, someTableUpgraded);
    List<UpgradeStep> steps = List.of(new UpgradeSomeTable(), new UpdateSomeTable());

    Pair<List<SchemaChange>, List<SchemaChange>> result =
        findAndAdaptUpgradePath(initialSchema, targetSchema, steps);

    List<SchemaChange> originalChanges = result.getLeft();
    assertThat(originalChanges.size(), equalTo(4+1));

    assertThat(originalChanges.get(0), instanceOf(RemoveColumn.class));
    assertThat(originalChanges.get(1), instanceOf(AddColumn.class));
    assertThat(originalChanges.get(2), instanceOf(RemoveIndex.class));
    assertThat(originalChanges.get(3), instanceOf(AddIndex.class));

    assertThat(originalChanges.get(4+0), instanceOf(ExecuteStatement.class));
    ExecuteStatement executeStatement = (ExecuteStatement) originalChanges.get(4+0);
    assertThat(executeStatement.getStatement(), instanceOf(UpdateStatement.class));
    assertThat(executeStatement.getStatement().toString(), equalTo(
        "SQL UPDATE [[CtasDuringUpgrade{useForDatabaseType=<ANY>}]SomeTable]"
        + " SET [someIntValue + 1 AS someIntValue, LEFT_PAD(someStrValue, 5, \"0\") AS someStrValue]"));

    List<SchemaChange> adaptedChanges = result.getRight();
    assertThat(adaptedChanges.size(), equalTo(4+5));

    assertThat(originalChanges.get(0), instanceOf(RemoveColumn.class));
    assertThat(originalChanges.get(1), instanceOf(AddColumn.class));
    assertThat(originalChanges.get(2), instanceOf(RemoveIndex.class));
    assertThat(originalChanges.get(3), instanceOf(AddIndex.class));

    assertThat(adaptedChanges.get(4+0), instanceOf(AddTableFrom.class));
    AddTableFrom addTableFrom = (AddTableFrom) adaptedChanges.get(4+0);
    assertThat(addTableFrom.getTable().getName(), equalTo("CTAS_SomeTable"));
    assertThat(addTableFrom.getTable().primaryKey().toString(), equalTo("[Column-id-BIG_INTEGER---notNull-pk--0-]"));
    assertThat(addTableFrom.getTable().columns().toString(), equalTo(
        "[Column-id-BIG_INTEGER---notNull-pk--0-, "
        + "Column-someIntValue-DECIMAL-13-0-notNull---0-, "
        + "Column-someStrValue-STRING-10--notNull---0-, "
        + "Column-version-INTEGER---null---0-0, "
        + "Column-someBoolValue-BOOLEAN---null---0-]"));
    assertThat(addTableFrom.getTable().indexes().toString(), equalTo("[]"));
    assertThat(addTableFrom.getTable().isTemporary(), equalTo(false));
    assertThat(addTableFrom.getSelectStatement().toString(), equalTo(
        "SQL SELECT [id, "
        + "CAST(someIntValue + 1 AS someIntValue AS DECIMAL(13, 0)),"
        + " CAST(LEFT_PAD(someStrValue, 5, \"0\") AS someStrValue AS STRING(10, 0)),"
        + " version, someBoolValue] FROM [SomeTable]"));

    assertThat(adaptedChanges.get(4+1), instanceOf(RemoveTable.class));
    RemoveTable removeTable = (RemoveTable) adaptedChanges.get(4+1);
    assertThat(removeTable.getTable().getName(), equalTo(someTableUpgraded.getName()));
    assertThat(removeTable.getTable().columns().toString(), equalTo(
        "[Column-id-BIG_INTEGER---notNull-pk--0-, "
        + "Column-someIntValue-DECIMAL-13-0-notNull---0-, "
        + "Column-someStrValue-STRING-10--notNull---0-, "
        + "Column-version-INTEGER---null---0-0, "
        + "Column-someBoolValue-BOOLEAN---null---0-]"));
    assertThat(removeTable.getTable().indexes().toString(), equalTo(
        "[Index-SomeTable_1--someIntValue,someStrValue, "
        + "Index-someBoolValue_idx-unique-someBoolValue]"));
    assertThat(removeTable.getTable().primaryKey().toString(), equalTo("[Column-id-BIG_INTEGER---notNull-pk--0-]"));
    assertThat(removeTable.getTable().isTemporary(), equalTo(someTableUpgraded.isTemporary()));

    assertThat(adaptedChanges.get(4+2), instanceOf(RenameTable.class));
    RenameTable renameTable = (RenameTable) adaptedChanges.get(4+2);
    assertThat(renameTable.getOldTableName(), equalTo("CTAS_"+someTable.getName()));
    assertThat(renameTable.getNewTableName(), equalTo(someTable.getName()));

    assertThat(adaptedChanges.get(4+3), instanceOf(AddIndex.class));
    AddIndex addIndex1 = (AddIndex) adaptedChanges.get(4+3);
    assertThat(addIndex1.getTableName(), equalTo(someTable.getName()));
    assertThat(addIndex1.getNewIndex().toString(), equalTo("Index-SomeTable_1--someIntValue,someStrValue"));

    assertThat(adaptedChanges.get(4+4), instanceOf(AddIndex.class));
    AddIndex addIndex2 = (AddIndex) adaptedChanges.get(4+4);
    assertThat(addIndex2.getTableName(), equalTo(someTable.getName()));
    assertThat(addIndex2.getNewIndex().toString(), equalTo("Index-someBoolValue_idx-unique-someBoolValue"));
  }


  @Test
  public void testNoIndexesOnTable() {
    UpdateToCtasAdaptor adaptor = new UpdateToCtasAdaptor();
    upgradeConfigAndContext.setSchemaChangeToSchemaAdaptor(adaptor);

    Table someTable = table("SomeTable")
      .columns(
          idColumn(),
          column("someIntValue", DataType.DECIMAL, 13),
          column("someStrValue", DataType.STRING, 10),
          column("someDateValue", DataType.DATE),
          versionColumn());

    Table randomTable1 = randomTable("R1");
    Table randomTable2 = randomTable("R2");
    Table randomTable3 = randomTable("R3");

    Schema initialSchema = schema(randomTable1, randomTable2, someTable, randomTable3);
    Schema targetSchema = initialSchema;
    List<UpgradeStep> steps = List.of(new UpdateSomeTable());

    Pair<List<SchemaChange>, List<SchemaChange>> result =
        findAndAdaptUpgradePath(initialSchema, targetSchema, steps);

    List<SchemaChange> originalChanges = result.getLeft();
    assertThat(originalChanges.size(), equalTo(1));

    assertThat(originalChanges.get(0), instanceOf(ExecuteStatement.class));
    ExecuteStatement executeStatement = (ExecuteStatement) originalChanges.get(0);
    assertThat(executeStatement.getStatement(), instanceOf(UpdateStatement.class));
    assertThat(executeStatement.getStatement().toString(), equalTo(
        "SQL UPDATE [[CtasDuringUpgrade{useForDatabaseType=<ANY>}]SomeTable]"
        + " SET [someIntValue + 1 AS someIntValue, LEFT_PAD(someStrValue, 5, \"0\") AS someStrValue]"));

    List<SchemaChange> adaptedChanges = result.getRight();
    assertThat(adaptedChanges.size(), equalTo(3));

    assertThat(adaptedChanges.get(0), instanceOf(AddTableFrom.class));
    AddTableFrom addTableFrom = (AddTableFrom) adaptedChanges.get(0);
    assertThat(addTableFrom.getTable().getName(), equalTo("CTAS_SomeTable"));
    assertThat(addTableFrom.getTable().primaryKey().toString(), equalTo("[Column-id-BIG_INTEGER---notNull-pk--0-]"));
    assertThat(addTableFrom.getTable().columns().toString(), equalTo(
        "[Column-id-BIG_INTEGER---notNull-pk--0-, "
        + "Column-someIntValue-DECIMAL-13-0-notNull---0-, "
        + "Column-someStrValue-STRING-10--notNull---0-, "
        + "Column-someDateValue-DATE---notNull---0-, "
        + "Column-version-INTEGER---null---0-0]"));
    assertThat(addTableFrom.getTable().indexes().toString(), equalTo("[]"));
    assertThat(addTableFrom.getTable().isTemporary(), equalTo(false));
    assertThat(addTableFrom.getSelectStatement().toString(), equalTo(
        "SQL SELECT [id, "
        + "CAST(someIntValue + 1 AS someIntValue AS DECIMAL(13, 0)),"
        + " CAST(LEFT_PAD(someStrValue, 5, \"0\") AS someStrValue AS STRING(10, 0)),"
        + " someDateValue, version] FROM [SomeTable]"));

    assertThat(adaptedChanges.get(1), instanceOf(RemoveTable.class));
    RemoveTable removeTable = (RemoveTable) adaptedChanges.get(1);
    assertThat(removeTable.getTable(), equalTo(someTable));

    assertThat(adaptedChanges.get(2), instanceOf(RenameTable.class));
    RenameTable renameTable = (RenameTable) adaptedChanges.get(2);
    assertThat(renameTable.getOldTableName(), equalTo("CTAS_"+someTable.getName()));
    assertThat(renameTable.getNewTableName(), equalTo(someTable.getName()));
  }


  @Test
  public void testNoPkOnTable() {
    UpdateToCtasAdaptor adaptor = new UpdateToCtasAdaptor();
    upgradeConfigAndContext.setSchemaChangeToSchemaAdaptor(adaptor);

    Table someTable = table("SomeTable")
      .columns(
          column("id", DataType.BIG_INTEGER),
          column("someIntValue", DataType.DECIMAL, 13),
          column("someStrValue", DataType.STRING, 10),
          column("someDateValue", DataType.DATE),
          versionColumn())
      .indexes(
          index("SomeTable_1").columns("someIntValue", "someStrValue"));

    Table randomTable1 = randomTable("R1");
    Table randomTable2 = randomTable("R2");
    Table randomTable3 = randomTable("R3");

    Schema initialSchema = schema(randomTable1, randomTable2, someTable, randomTable3);
    Schema targetSchema = initialSchema;
    List<UpgradeStep> steps = List.of(new UpdateSomeTable());

    Pair<List<SchemaChange>, List<SchemaChange>> result =
        findAndAdaptUpgradePath(initialSchema, targetSchema, steps);

    List<SchemaChange> originalChanges = result.getLeft();
    assertThat(originalChanges.size(), equalTo(1));

    assertThat(originalChanges.get(0), instanceOf(ExecuteStatement.class));
    ExecuteStatement executeStatement = (ExecuteStatement) originalChanges.get(0);
    assertThat(executeStatement.getStatement(), instanceOf(UpdateStatement.class));
    assertThat(executeStatement.getStatement().toString(), equalTo(
        "SQL UPDATE [[CtasDuringUpgrade{useForDatabaseType=<ANY>}]SomeTable]"
        + " SET [someIntValue + 1 AS someIntValue, LEFT_PAD(someStrValue, 5, \"0\") AS someStrValue]"));

    List<SchemaChange> adaptedChanges = result.getRight();
    assertThat(adaptedChanges.size(), equalTo(4));

    assertThat(adaptedChanges.get(0), instanceOf(AddTableFrom.class));
    AddTableFrom addTableFrom = (AddTableFrom) adaptedChanges.get(0);
    assertThat(addTableFrom.getTable().getName(), equalTo("CTAS_SomeTable"));
    assertThat(addTableFrom.getTable().primaryKey().toString(), equalTo("[]"));
    assertThat(addTableFrom.getTable().columns().toString(), equalTo(
        "[Column-id-BIG_INTEGER---notNull---0-, "
        + "Column-someIntValue-DECIMAL-13-0-notNull---0-, "
        + "Column-someStrValue-STRING-10--notNull---0-, "
        + "Column-someDateValue-DATE---notNull---0-, "
        + "Column-version-INTEGER---null---0-0]"));
    assertThat(addTableFrom.getTable().indexes().toString(), equalTo("[]"));
    assertThat(addTableFrom.getTable().isTemporary(), equalTo(false));
    assertThat(addTableFrom.getSelectStatement().toString(), equalTo(
        "SQL SELECT [id, "
        + "CAST(someIntValue + 1 AS someIntValue AS DECIMAL(13, 0)),"
        + " CAST(LEFT_PAD(someStrValue, 5, \"0\") AS someStrValue AS STRING(10, 0)),"
        + " someDateValue, version] FROM [SomeTable]"));

    assertThat(adaptedChanges.get(1), instanceOf(RemoveTable.class));
    RemoveTable removeTable = (RemoveTable) adaptedChanges.get(1);
    assertThat(removeTable.getTable(), equalTo(someTable));

    assertThat(adaptedChanges.get(2), instanceOf(RenameTable.class));
    RenameTable renameTable = (RenameTable) adaptedChanges.get(2);
    assertThat(renameTable.getOldTableName(), equalTo("CTAS_"+someTable.getName()));
    assertThat(renameTable.getNewTableName(), equalTo(someTable.getName()));

    assertThat(adaptedChanges.get(3), instanceOf(AddIndex.class));
    AddIndex addIndex1 = (AddIndex) adaptedChanges.get(3);
    assertThat(addIndex1.getTableName(), equalTo(someTable.getName()));
    assertThat(addIndex1.getNewIndex().toString(), equalTo("Index-SomeTable_1--someIntValue,someStrValue"));
  }


  @Test
  public void testCtasWithAllColumnsUpdated() {
    UpdateToCtasAdaptor adaptor = new UpdateToCtasAdaptor();
    upgradeConfigAndContext.setSchemaChangeToSchemaAdaptor(adaptor);

    Table someTable = table("SomeTable")
      .columns(
          column("someIntValue", DataType.DECIMAL, 13),
          column("someStrValue", DataType.STRING, 10))
      .indexes(
          index("SomeTable_1").columns("someIntValue", "someStrValue"),
          index("someStrValue_idx").columns("someStrValue"));

    Table randomTable1 = randomTable("R1");
    Table randomTable2 = randomTable("R2");
    Table randomTable3 = randomTable("R3");

    Schema initialSchema = schema(randomTable1, randomTable2, someTable, randomTable3);
    Schema targetSchema = initialSchema;
    List<UpgradeStep> steps = List.of(new UpdateSomeTable());

    Pair<List<SchemaChange>, List<SchemaChange>> result =
        findAndAdaptUpgradePath(initialSchema, targetSchema, steps);

    List<SchemaChange> originalChanges = result.getLeft();
    assertThat(originalChanges.size(), equalTo(1));

    assertThat(originalChanges.get(0), instanceOf(ExecuteStatement.class));
    ExecuteStatement executeStatement = (ExecuteStatement) originalChanges.get(0);
    assertThat(executeStatement.getStatement(), instanceOf(UpdateStatement.class));
    assertThat(executeStatement.getStatement().toString(), equalTo(
        "SQL UPDATE [[CtasDuringUpgrade{useForDatabaseType=<ANY>}]SomeTable]"
        + " SET [someIntValue + 1 AS someIntValue, LEFT_PAD(someStrValue, 5, \"0\") AS someStrValue]"));

    List<SchemaChange> adaptedChanges = result.getRight();
    assertThat(adaptedChanges.size(), equalTo(5));

    assertThat(adaptedChanges.get(0), instanceOf(AddTableFrom.class));
    AddTableFrom addTableFrom = (AddTableFrom) adaptedChanges.get(0);
    assertThat(addTableFrom.getTable().getName(), equalTo("CTAS_SomeTable"));
    assertThat(addTableFrom.getTable().primaryKey().toString(), equalTo("[]"));
    assertThat(addTableFrom.getTable().columns().toString(), equalTo(
        "[Column-someIntValue-DECIMAL-13-0-notNull---0-, "
        + "Column-someStrValue-STRING-10--notNull---0-]"));
    assertThat(addTableFrom.getTable().indexes().toString(), equalTo("[]"));
    assertThat(addTableFrom.getTable().isTemporary(), equalTo(false));
    assertThat(addTableFrom.getSelectStatement().toString(), equalTo(
        "SQL SELECT [CAST(someIntValue + 1 AS someIntValue AS DECIMAL(13, 0)),"
        + " CAST(LEFT_PAD(someStrValue, 5, \"0\") AS someStrValue AS STRING(10, 0))] FROM [SomeTable]"));

    assertThat(adaptedChanges.get(1), instanceOf(RemoveTable.class));
    RemoveTable removeTable = (RemoveTable) adaptedChanges.get(1);
    assertThat(removeTable.getTable(), equalTo(someTable));

    assertThat(adaptedChanges.get(2), instanceOf(RenameTable.class));
    RenameTable renameTable = (RenameTable) adaptedChanges.get(2);
    assertThat(renameTable.getOldTableName(), equalTo("CTAS_"+someTable.getName()));
    assertThat(renameTable.getNewTableName(), equalTo(someTable.getName()));

    assertThat(adaptedChanges.get(3), instanceOf(AddIndex.class));
    AddIndex addIndex1 = (AddIndex) adaptedChanges.get(3);
    assertThat(addIndex1.getTableName(), equalTo(someTable.getName()));
    assertThat(addIndex1.getNewIndex().toString(), equalTo("Index-SomeTable_1--someIntValue,someStrValue"));

    assertThat(adaptedChanges.get(4), instanceOf(AddIndex.class));
    AddIndex addIndex2 = (AddIndex) adaptedChanges.get(4);
    assertThat(addIndex2.getTableName(), equalTo(someTable.getName()));
    assertThat(addIndex2.getNewIndex().toString(), equalTo("Index-someStrValue_idx--someStrValue"));
  }


  @Test
  public void testUpdateWithWhereClause() {
    UpdateToCtasAdaptor adaptor = new UpdateToCtasAdaptor();
    upgradeConfigAndContext.setSchemaChangeToSchemaAdaptor(adaptor);

    Table someTable = table("SomeTable")
      .columns(
          idColumn(),
          column("someIntValue", DataType.DECIMAL, 13),
          column("someStrValue", DataType.STRING, 10),
          column("someDateValue", DataType.DATE),
          versionColumn())
      .indexes(
          index("SomeTable_1").columns("someIntValue", "someStrValue"),
          index("someStrValue_idx").columns("someStrValue"));

    Table randomTable1 = randomTable("R1");
    Table randomTable2 = randomTable("R2");
    Table randomTable3 = randomTable("R3");

    Schema initialSchema = schema(randomTable1, randomTable2, someTable, randomTable3);
    Schema targetSchema = initialSchema;
    List<UpgradeStep> steps = List.of(new UpdateSomeTableWithWhere());

    RuntimeException exception = assertThrows(RuntimeException.class,
      () -> findAndAdaptUpgradePath(initialSchema, targetSchema, steps));

    assertThat(exception.getMessage(), allOf(
        containsString("Failed to apply change"),
        containsString("ExecuteStatement"),
        containsString("from upgrade step class org.alfasoftware.morf.upgrade.TestUpdateToCtasAdaptor$UpdateSomeTable")));

    assertThat(exception.getCause(), instanceOf(RuntimeException.class));

    assertThat(exception.getCause().getMessage(), allOf(
        containsString("Cannot translate an UPDATE to CTAS"),
        containsString("WHERE clause found"),
        containsString("SQL UPDATE [[CtasDuringUpgrade{useForDatabaseType=<ANY>}]SomeTable]"),
        containsString("WHERE [someIntValue = someIntValue]")));
  }


  @Test
  public void testUpdateNonExistingTable() {
    UpdateToCtasAdaptor adaptor = new UpdateToCtasAdaptor();
    upgradeConfigAndContext.setSchemaChangeToSchemaAdaptor(adaptor);

    Table randomTable1 = randomTable("R1");
    Table randomTable2 = randomTable("R2");
    Table randomTable3 = randomTable("R3");

    Schema initialSchema = schema(randomTable1, randomTable2, randomTable3);
    Schema targetSchema = initialSchema;
    List<UpgradeStep> steps = List.of(new UpdateSomeTable());

    RuntimeException exception = assertThrows(RuntimeException.class,
      () -> findAndAdaptUpgradePath(initialSchema, targetSchema, steps));

    assertThat(exception.getMessage(), allOf(
        containsString("Failed to apply change"),
        containsString("ExecuteStatement"),
        containsString("from upgrade step class org.alfasoftware.morf.upgrade.TestUpdateToCtasAdaptor$UpdateSomeTable")));

    assertThat(exception.getCause(), instanceOf(RuntimeException.class));

    assertThat(exception.getCause().getMessage(), allOf(
        containsString("Cannot translate an UPDATE to CTAS"),
        containsString("table [SomeTable] not found"),
        containsString("SQL UPDATE [[CtasDuringUpgrade{useForDatabaseType=<ANY>}]SomeTable]")));
  }


  @Test
  public void testUpdateNonExistingColumn() {
    UpdateToCtasAdaptor adaptor = new UpdateToCtasAdaptor();
    upgradeConfigAndContext.setSchemaChangeToSchemaAdaptor(adaptor);

    Table someTable = table("SomeTable")
      .columns(
          idColumn(),
          column("someIntValue", DataType.DECIMAL, 13),
          column("someDateValue", DataType.DATE),
          versionColumn())
      .indexes(
          index("SomeTable_1").columns("someIntValue"));

    Table randomTable1 = randomTable("R1");
    Table randomTable2 = randomTable("R2");
    Table randomTable3 = randomTable("R3");

    Schema initialSchema = schema(randomTable1, randomTable2, someTable, randomTable3);
    Schema targetSchema = initialSchema;
    List<UpgradeStep> steps = List.of(new UpdateSomeTable());

    RuntimeException exception = assertThrows(RuntimeException.class,
      () -> findAndAdaptUpgradePath(initialSchema, targetSchema, steps));

    assertThat(exception.getMessage(), allOf(
        containsString("Failed to apply change"),
        containsString("ExecuteStatement"),
        containsString("from upgrade step class org.alfasoftware.morf.upgrade.TestUpdateToCtasAdaptor$UpdateSomeTable")));

    assertThat(exception.getCause(), instanceOf(RuntimeException.class));

    assertThat(exception.getCause().getMessage(), allOf(
        containsString("Cannot translate an UPDATE to CTAS"),
        containsString("Cannot find SOMESTRVALUE amongst {"
            + "SOMEINTVALUE=Column-someIntValue-DECIMAL-13-0-notNull---0-, "
            + "VERSION=Column-version-INTEGER---null---0-0, "
            + "ID=Column-id-BIG_INTEGER---notNull-pk--0-, "
            + "SOMEDATEVALUE=Column-someDateValue-DATE---notNull---0-}")));
  }


  private Pair<List<SchemaChange>, List<SchemaChange>> findAndAdaptUpgradePath(Schema initialSchema, Schema targetSchema, List<UpgradeStep> steps) {
    SchemaChangeSequence originalChangesSequence = new SchemaChangeSequence(upgradeConfigAndContext, steps);
    List<SchemaChange> originalChanges = originalChangesSequence.getAllChanges();

    SchemaChangeSequence adaptedChangeSequence = originalChangesSequence.adaptToSchema(initialSchema);
    List<SchemaChange> adaptedChanges = adaptedChangeSequence.getAllChanges();

    Schema upgradedSchema = adaptedChangeSequence.applyToSchema(initialSchema);
    SchemaHomology homology1 = new SchemaHomology(log::warn, "targetSchema", "upgradedSchema");
    assertTrue(homology1.schemasMatch(targetSchema, upgradedSchema, List.of()));

    Schema reversedSchema = adaptedChangeSequence.applyInReverseToSchema(upgradedSchema);
    SchemaHomology homology2 = new SchemaHomology(log::warn, "initialSchema", "reversedSchema");
    assertTrue(homology2.schemasMatch(initialSchema, reversedSchema, List.of()));

    return Pair.of(originalChanges, adaptedChanges);
  }


  private class UpdateSomeTable extends AbstractTestingUpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      data.executeStatement(
          update(tableRef("SomeTable"))
            .set(field("someIntValue").plus(literal(1)).as("someIntValue"))
            .set(leftPad(field("someStrValue"), 5, "0").as("someStrValue"))
            .useCtasDuringUpgrade(true)
            .build());
    }
  }


  private class UpdateSomeTableWithWhere extends AbstractTestingUpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {
      data.executeStatement(
          update(tableRef("SomeTable"))
            .set(field("someIntValue").plus(literal(1)).as("someIntValue"))
            .set(leftPad(field("someStrValue"), 5, "0").as("someStrValue"))
            .where(field("someIntValue").eq(field("someIntValue")))
            .useCtasDuringUpgrade(true)
            .build());
    }
  }


  private class UpgradeSomeTable extends AbstractTestingUpgradeStep {
    @Override
    public void execute(SchemaEditor schema, DataEditor data) {

      schema.removeColumn("SomeTable", column("someDateValue", DataType.DATE));
      schema.addColumn("SomeTable", column("someBoolValue", DataType.BOOLEAN).nullable());

      schema.removeIndex("SomeTable", index("someStrValue_idx").columns("someStrValue"));
      schema.addIndex("SomeTable", index("someBoolValue_idx").columns("someBoolValue").unique());
    }
  }


  private abstract class AbstractTestingUpgradeStep implements UpgradeStep {
    @Override public String getJiraId() { return "XXX-000"; }
    @Override public String getDescription() { return ""; }
  }


  private final Random random = new Random();

  private Table randomTable(String prefix) {
    List<Column> columns = randomColumns(prefix);
    List<Index> indexes = randomIndexes(prefix, columns);
    TableBuilder table = table(randomName(prefix)).columns(columns).indexes(indexes);
    if (random.nextBoolean()) table.temporary();
    return table;
  }

  private String randomName(String prefix) {
    String name = prefix;
    name += Character.toString('A' + random.nextInt(20));
    while(random.nextInt(10) > 0) {
      name += Character.toString('a' + random.nextInt(20));
    }
    return name;
  }

  private List<Column> randomColumns(String prefix) {
    ImmutableList.Builder<Column> columns = ImmutableList.builder();
    int counter = 1;
    while(random.nextInt(5) > 0) {
      columns.add(column(randomName(prefix + counter), randomDataType(), random.nextInt(5), random.nextInt(5)));
      counter++;
    }
    return columns.build();
  }

  private List<Index> randomIndexes(String prefix, List<Column> columns) {
    ImmutableList.Builder<Index> indexes = ImmutableList.builder();
    int counter = 1;
    while(random.nextInt(5) > 0) {
      List<String> randomColumns = columns.stream().filter(c -> random.nextBoolean()).map(Column::getName).collect(toList());
      IndexBuilder index = index(randomName(prefix + counter)).columns(randomColumns);
      if (random.nextBoolean()) index.unique();
      indexes.add(index);
      counter++;
    }
    return indexes.build();
  }

  private DataType randomDataType() {
    switch(random.nextInt(5)) {
      case 0: return DataType.BIG_INTEGER;
      case 1: return DataType.DECIMAL;
      case 2: return DataType.STRING;
      case 3: return DataType.DATE;
      default: return DataType.BOOLEAN;
    }
  }
}
