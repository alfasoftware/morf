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

import static org.alfasoftware.morf.changelog.ChangelogBuilder.changelogBuilder;
import static org.junit.Assert.assertTrue;

import java.io.PrintWriter;
import java.io.StringWriter;

import com.google.common.collect.ImmutableList;

import org.alfasoftware.morf.upgrade.testupgrade.upgrade.v1_0_0.ChangeCar;
import org.alfasoftware.morf.upgrade.testupgrade.upgrade.v1_0_0.ChangeDriver;
import org.junit.Test;

/**
 * Tests for {@link ChangelogBuilder}.
 *
 * @author Copyright (c) Alfa Financial Software 2016
 */
public class TestChangelogBuilder {

  /**
   * Test database changelog generation.
   *
   * The default behaviour, reporting on just structural database changes, will be used. There
   * will be no mention of the data changes from ChangeDriver.
   *
   * @throws Exception on error
   */
  @Test
  public void testDefaultChangeLog() throws Exception {
    StringWriter writer = new StringWriter();

    changelogBuilder()
    .withOutputTo(new PrintWriter(writer))
    .withUpgradeSteps(ImmutableList.of(ChangeCar.class, ChangeDriver.class))
    .produceChangelog();

    String changeLog = writer.toString();

    assertTrue(changeLog.contains("ChangeCar"));
    assertTrue(changeLog.contains("engineVolume [DECIMAL(20,0)]"));
    assertTrue(changeLog.contains("* xxx-123:"));

    assertTrue(changeLog.contains("ChangeDriver"));
    assertTrue(changeLog.contains("postCode [STRING(8)]"));
  }


  /**
   * Test database changelog generation.
   */
  @Test
  public void testFullChangeLog() {
    StringWriter writer = new StringWriter();

    changelogBuilder()
      .withIncludeDataChanges(true)
      .withOutputTo(new PrintWriter(writer))
      .withUpgradeSteps(ImmutableList.of(ChangeCar.class, ChangeDriver.class))
      .produceChangelog();

    String changeLog = writer.toString();

    assertTrue(changeLog.contains("ChangeCar"));
    assertTrue(changeLog.contains("engineVolume [DECIMAL(20,0)]"));
    assertTrue(changeLog.contains("* xxx-123:"));

    assertTrue(changeLog.contains("ChangeDriver"));
    assertTrue(changeLog.contains("postCode [STRING(8)]"));

    assertTrue(changeLog.contains("Add record into Driver:"));
    assertTrue(changeLog.contains("Set name to 'Dave'"));
    assertTrue(changeLog.contains("Set address to 'Address'"));
    assertTrue(changeLog.contains("Set postCode to 'postCode'"));
  }
}
