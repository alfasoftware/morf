package org.alfasoftware.morf.upgrade;

import java.util.Collection;
import java.util.Map;
import java.util.UUID;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.Table;

/**
 * Graph Based Upgrade implementation of the {@link SchemaChangeVisitor} which
 * is responsible for generation of all schema and data modifying statements for
 * each upgrade step.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
class GraphBasedUpgradeSchemaChangeVisitor extends AbstractSchemaChangeVisitor implements SchemaChangeVisitor {

  private final Map<String, GraphBasedUpgradeNode> upgradeNodes;
  GraphBasedUpgradeNode currentNode;


  /**
   * Default constructor.
   *
   * @param currentSchema schema prior to upgrade step.
   * @param upgradeConfigAndContext upgrade config
   * @param sqlDialect   dialect to generate statements for the target database.
   * @param idTable      table for id generation.
   * @param upgradeNodes all the {@link GraphBasedUpgradeNode} instances in the
   *                       upgrade for which the visitor will generate statements
   */
  GraphBasedUpgradeSchemaChangeVisitor(Schema currentSchema, UpgradeConfigAndContext upgradeConfigAndContext, SqlDialect sqlDialect, Table idTable, Map<String, GraphBasedUpgradeNode> upgradeNodes) {
    super(currentSchema, upgradeConfigAndContext, sqlDialect, idTable);
    this.currentSchema = currentSchema;
    this.sqlDialect = sqlDialect;
    this.upgradeNodes = upgradeNodes;
  }


  /**
   * Write statements to the current node
   */
  @Override
  protected void writeStatements(Collection<String> statements) {
    currentNode.addAllUpgradeStatements(statements);
  }


  /**
   * Write statement to the current node
   */
  @Override
  protected void writeStatement(String statement) {
    currentNode.addUpgradeStatements(statement);
  }


  @Override
  public void addAuditRecord(UUID uuid, String description) {
    AuditRecordHelper.addAuditRecord(this, currentSchema, uuid, description);
  }


  /**
   * Set the current {@link GraphBasedUpgradeNode} which is being processed.
   *
   * @param upgradeClass upgrade which is currently being processed
   */
  @Override
  public void startStep(Class<? extends UpgradeStep> upgradeClass) {
    currentNode = upgradeNodes.get(upgradeClass.getName());
    if (currentNode == null) {
      throw new IllegalStateException("UpgradeNode: " + upgradeClass.getName() + " doesn't exist.");
    }
  }


  /**
   * Factory of {@link GraphBasedUpgradeSchemaChangeVisitor} instances.
   *
   * @author Copyright (c) Alfa Financial Software Limited. 2022
   */
  static class GraphBasedUpgradeSchemaChangeVisitorFactory {

    /**
     * Creates {@link GraphBasedUpgradeSchemaChangeVisitor} instance.
     *
     * @param currentSchema schema prior to upgrade step
     * @param upgradeConfigAndContext upgrade config
     * @param sqlDialect   dialect to generate statements for the target database
     * @param idTable      table for id generation
     * @param upgradeNodes all the {@link GraphBasedUpgradeNode} instances in the upgrade for
     *                       which the visitor will generate statements
     * @return new {@link GraphBasedUpgradeSchemaChangeVisitor} instance
     */
    GraphBasedUpgradeSchemaChangeVisitor create(Schema currentSchema, UpgradeConfigAndContext upgradeConfigAndContext, SqlDialect sqlDialect, Table idTable,
                                                Map<String, GraphBasedUpgradeNode> upgradeNodes) {
      return new GraphBasedUpgradeSchemaChangeVisitor(currentSchema, upgradeConfigAndContext, sqlDialect, idTable, upgradeNodes);
    }
  }
}

