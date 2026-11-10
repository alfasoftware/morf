package org.alfasoftware.morf.upgrade;

import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

import org.alfasoftware.morf.changelog.EntityHumanReadableStatementConsumer;

class ListBackedEntityHumanReadableStatementConsumer extends EntityHumanReadableStatementConsumer {

  /**
   * List of strings written to the consumer.
   */
  private final List<String> list = new ArrayList<>();

  public ListBackedEntityHumanReadableStatementConsumer(String versionStart, PrintWriter outputStream) {
    super(versionStart, outputStream);
  }

  /**
   * @see EntityHumanReadableStatementConsumer#entityStart(String)
   */
  @Override
  public void entityStart(String entityName) {
    list.add("ENTITYSTART:[" + entityName + "]");
  }

  /**
   * @see EntityHumanReadableStatementConsumer#entityEnd(String)
   */
  @Override
  public void entityEnd(String entityName) {
    list.add("ENTITYEND:[]");
  }


  /**
   * @see HumanReadableStatementConsumer#upgradeStepStart(String, String, String)
   */
  @Override
  public void upgradeStepStart(String name, String description, String jiraId) {
    list.add("STEPSTART:[" + jiraId + "]-[" + name + "]-[" + description + "]");
  }


  /**
   * @see HumanReadableStatementConsumer#schemaChange(String)
   */
  @Override
  public void schemaChange(String description) {
    list.add("CHANGE:[" + description + "]");
  }


  /**
   * @see HumanReadableStatementConsumer#upgradeStepEnd(String)
   */
  @Override
  public void upgradeStepEnd(String name) {
    list.add("STEPEND:[]");
  }


  /**
   * @see HumanReadableStatementConsumer#dataChange(String)
   */
  @Override
  public void dataChange(String description) {
    list.add("CHANGE:[" + description + "]");
  }


  /**
   * Gets the list of items sent to the consumer.
   *
   * @return the list
   */
  public List<String> getList() {
    return list;
  }

}
