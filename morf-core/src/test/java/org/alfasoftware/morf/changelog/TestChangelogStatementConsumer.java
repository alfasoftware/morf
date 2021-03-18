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



import static org.junit.Assert.assertArrayEquals;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.apache.commons.lang3.SystemUtils;
import org.junit.Test;


/**
 * Tests for {@link ChangelogStatementConsumer}
 *
 * @author Copyright (c) Alfa Financial Software 2016
 */
public class TestChangelogStatementConsumer {


  /**
   * Test the line wrapping logic.
   */
  @Test
  public void testLineWrapping() {
    final StringWriter writer = new StringWriter();
    final ChangelogStatementConsumer test = new ChangelogStatementConsumer(new PrintWriter(writer));

    test.upgradeStepStart("Upgrade1", "Short description", "WEB-1234");
    test.schemaChange("Short schema change");
    test.schemaChange("Long schema change string - lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua");
    test.dataChange("Short data change");
    test.dataChange("Long data change - lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua");
    test.upgradeStepEnd("Upgrade1");

    test.upgradeStepStart("Upgrade2", "Long description - lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua", "WEB-1234");
    test.dataChange("Multiple lines" +
        System.lineSeparator() + "    - long sub-line 1 - lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua ut enim ad minim veniam, quis nostrud exercitation ullamco laboris" +
        System.lineSeparator() + "    - short sub-line" +
        System.lineSeparator() + "    - long sub-line 2 - lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et dolore magna aliqua ut enim ad minim veniam, quis nostrud exercitation ullamco laboris");
    test.upgradeStepEnd("Upgrade2");

    final String[] lines = writer.toString().split(System.lineSeparator());

    assertArrayEquals("Line wrapping", new String[] {
      "* WEB-1234: Short description",
      "  o Short schema change",
      "  o Long schema change string - lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore",
      "    et dolore magna aliqua",
      "  o Short data change",
      "  o Long data change - lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et",
      "    dolore magna aliqua",
      "",
      "* WEB-1234: Long description - lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore",
      "  et dolore magna aliqua",
      "  o Multiple lines",
      "    - long sub-line 1 - lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et",
      "      dolore magna aliqua ut enim ad minim veniam, quis nostrud exercitation ullamco laboris",
      "    - short sub-line",
      "    - long sub-line 2 - lorem ipsum dolor sit amet, consectetur adipisicing elit, sed do eiusmod tempor incididunt ut labore et",
      "      dolore magna aliqua ut enim ad minim veniam, quis nostrud exercitation ullamco laboris" },
      lines);
  }

}
