package org.alfasoftware.morf.changelog;

import java.io.PrintWriter;

import org.alfasoftware.morf.upgrade.HumanReadableStatementConsumer;
import org.apache.commons.lang3.StringUtils;

/**
 * Class to consume strings produced by the entity human readable statement generator and
 * passes them to the target output stream after applying word-wrapping logic.
 *
 * @author Copyright (c) Alfa Financial Software 2026
 */
public class EntityHumanReadableStatementConsumer implements HumanReadableStatementConsumer {
  private final ConsumerUtils utils;
  private final PrintWriter outputStream;
  private final String versionStart;


  public EntityHumanReadableStatementConsumer(String versionStart, PrintWriter outputStream) {
    utils = new ConsumerUtils(outputStream);
    this.versionStart = versionStart;
    this.outputStream = outputStream;
  }

  public String getVersionStart() {
    return versionStart;
  }

  public void entityStart(String entityStart) {
    outputStream.println(entityStart);
    outputStream.println(StringUtils.repeat("=", entityStart.length()));
  }

  public void entityEnd(String entityEnd) {
    // nothing to write
  }

  /**
   * @see HumanReadableStatementConsumer#upgradeStepStart(String, String, String)
   */
  @Override
  public void upgradeStepStart(String name, String description, String jiraId) {
      utils.writeWrapped("* " + jiraId + ": " + description);
  }

  /**
   * @see HumanReadableStatementConsumer#upgradeStepEnd(String)
   */
  @Override
  public void upgradeStepEnd(String name) {
      outputStream.println();
  }

  /**
   * @see HumanReadableStatementConsumer#schemaChange(String)
   */
  @Override
  public void schemaChange(String description) {
      utils.writeWrapped("  o " + description);
  }

  /**
   * @see HumanReadableStatementConsumer#dataChange(String)
   */
  @Override
  public void dataChange(String description) {
      utils.writeWrapped("  o " + description);
  }

  /**
   * @see HumanReadableStatementConsumer#versionStart(String)
   */
  @Override
  public void versionStart(String versionNumber) {
    outputStream.println(versionNumber);
    outputStream.println(StringUtils.repeat("=", versionNumber.length()));
  }

  /**
   * @see HumanReadableStatementConsumer#versionEnd(String)
   */
  @Override
  public void versionEnd(String versionNumber) {
    //nothing to write
  }
}
