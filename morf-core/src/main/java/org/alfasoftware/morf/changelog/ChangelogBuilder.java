/* Copyright 2017 Alfa Financial Software
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.alfasoftware.morf.changelog;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.alfasoftware.morf.upgrade.HumanReadableStatementProducer;
import org.alfasoftware.morf.upgrade.UpgradeStep;

/**
 * <p>
 * Class to generate changelog text based on a given collection of
 * {@link UpgradeStep}'s
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software 2016
 */
public class ChangelogBuilder {

  private boolean includeDataChanges;

  /* Note that if this isn't registered, it won't work without a registered dialect being specified */
  private String preferredSQLDialect = "ORACLE";

  private PrintWriter outputStream = new PrintWriter(new OutputStreamWriter(System.out, Charset.forName("UTF-8")));
  private final Set<Class<? extends UpgradeStep>> upgradeSteps = new HashSet<>();

  public static ChangelogBuilder changelogBuilder(){
    return new ChangelogBuilder();
  }


  /**
   * Set whether to include data changes in the log output. The default is to
   * omit data changes.
   *
   * @param includeDataChanges Indicates whether data changes should be included or not.
   * @return This builder for chaining
   */
  public ChangelogBuilder withIncludeDataChanges(boolean includeDataChanges) {
    this.includeDataChanges = includeDataChanges;
    return this;
  }


  /**
   * Set which SQL dialect to use for data change statements that cannot be
   * converted to a human- readable form. The default is ORACLE.
   *
   * @param preferredSQLDialect The SQL dialect to use
   * @return This builder for chaining
   */
  public ChangelogBuilder withPreferredSQLDialect(String preferredSQLDialect) {
    this.preferredSQLDialect = preferredSQLDialect;
    return this;
  }


  /**
   * Add to the collection of {@link UpgradeStep}'s to include in this
   * changelog.
   *
   * @param upgradeSteps The upgrade steps to add
   * @return This builder for chaining
   */
  public ChangelogBuilder withUpgradeSteps(Collection<Class<? extends UpgradeStep>> upgradeSteps) {
    this.upgradeSteps.addAll(upgradeSteps);
    return this;
  }


  /**
   * Set the {@link PrintWriter} target for this changelog. Default is the
   * console.
   *
   * @param outputStream The {@link PrintWriter} to output to.
   * @return This builder for chaining
   */
  public ChangelogBuilder withOutputTo(PrintWriter outputStream) {
    this.outputStream = outputStream;
    return this;
  }


  /**
   * Produces the Changelog based on the given input settings.
   */
  public void produceChangelog() {
    HumanReadableStatementProducer producer = new HumanReadableStatementProducer(upgradeSteps, includeDataChanges, preferredSQLDialect);
    producer.produceFor(new ChangelogStatementConsumer(outputStream));
  }

}