package org.alfasoftware.morf.upgrade;

import java.util.ArrayList;
import java.util.List;

/**
 * A statement consumer which adds any consumed items to an internal list.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class ListBackedHumanReadableStatementConsumer implements HumanReadableStatementConsumer {

  /**
   * List of strings written to the consumer.
   */
  private final List<String> list = new ArrayList<>();


  /**
   * @see HumanReadableStatementConsumer#versionStart(String)
   */
  @Override
  public void versionStart(String versionNumber) {
    list.add("VERSIONSTART:[" + versionNumber + "]");
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
    list.add("STEPEND:[" + name + "]");
  }


  /**
   * @see HumanReadableStatementConsumer#versionEnd(String)
   */
  @Override
  public void versionEnd(String versionNumber) {
    list.add("VERSIONEND:[" + versionNumber + "]");
  }


  /**
   * @see HumanReadableStatementConsumer#dataChange(String)
   */
  @Override
  public void dataChange(String description) {
    list.add("DATACHANGE:[" + description + "]");
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
