package org.alfasoftware.morf.integration.testdatabaseupgradeintegration.upgrade.v1_0_0;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.sql.SelectStatement;
import org.alfasoftware.morf.upgrade.DataEditor;
import org.alfasoftware.morf.upgrade.SchemaEditor;
import org.alfasoftware.morf.upgrade.Sequence;
import org.alfasoftware.morf.upgrade.UUID;

import static org.alfasoftware.morf.metadata.SchemaUtils.column;
import static org.alfasoftware.morf.metadata.SchemaUtils.table;
import static org.alfasoftware.morf.sql.SqlUtils.*;

/**
 * Upgrade step for creating a table as select with a sequence to populate the id column of the table for testing.
 *
 * @author Copyright (c) Alfa Financial Software 2024
 */
@Sequence(1)
@UUID("2354042d-024d-4e56-a6ce-57c8ccc01af9")
public class CreateTableAsSelectWithSequence extends AbstractTestUpgradeStep {

  public static Table tableToAdd() {
    return table("TableAsSelect").columns(
      column("idCol", DataType.INTEGER, 4),
      column("stringCol", DataType.STRING, 25).primaryKey(),
      column("stringColNullable", DataType.STRING, 30).nullable(),
      column("decimalTenZeroCol", DataType.DECIMAL, 15),
      column("nullableBigIntegerCol", DataType.BIG_INTEGER).nullable()
    );
  }

  public static org.alfasoftware.morf.metadata.Sequence sequenceToAdd() {
    return SchemaUtils.sequence("TableAsSelectSeq");
  }

  @Override
  public void execute(SchemaEditor schema, DataEditor data) {

    schema.addSequence(sequenceToAdd());

    SelectStatement selectDataFromBasicTable =
      select(
        sequenceRef("TableAsSelectSeq").nextValue().as("idCol"),
        cast(field("stringCol")).asString(25),
        cast(field("stringCol")).asString(30),
        cast(field("decimalTenZeroCol")).asType(DataType.DECIMAL, 15),
        field("nullableBigIntegerCol")
      ).from(tableRef("BasicTable"));

    schema.addTableFrom(tableToAdd(), selectDataFromBasicTable);
  }
}
