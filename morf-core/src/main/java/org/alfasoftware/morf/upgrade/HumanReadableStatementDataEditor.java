package org.alfasoftware.morf.upgrade;

import org.alfasoftware.morf.sql.Statement;

public class HumanReadableStatementDataEditor implements DataEditor {
  private final HumanReadableStatementConsumer consumer;
  private final boolean reportDataChanges;
  private final String preferredSQLDialect;

  HumanReadableStatementDataEditor(HumanReadableStatementConsumer consumer, boolean reportDataChanges, String preferredSQLDialect) {
    this.consumer = consumer;
    this.reportDataChanges = reportDataChanges;
    this.preferredSQLDialect = preferredSQLDialect;
  }


  @Override
  public void executeStatement(Statement statement) {
    if (reportDataChanges) {
      consumer.dataChange(HumanReadableStatementHelper.generateDataUpgradeString(statement, preferredSQLDialect));
    }
  }
}
