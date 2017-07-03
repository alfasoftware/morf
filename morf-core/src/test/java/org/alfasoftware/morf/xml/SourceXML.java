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

package org.alfasoftware.morf.xml;

/**
 * Test XML strings for the XML dataset testing.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class SourceXML {

  /**
   * Sample XML with all fields
   */
  public static String FULL_SAMPLE =

    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
    "<table version=\"2\">\n" +
    "  <metadata name=\"Test\">\n" +
    "    <column name=\"id\" type=\"BIG_INTEGER\" primaryKey=\"true\" autoNum=\"123\"/>\n" +
    "    <column name=\"version\" type=\"INTEGER\" default=\"0\" nullable=\"true\"/>\n" +
    "    <column name=\"bar\" type=\"STRING\" width=\"10\" nullable=\"true\"/>\n" +
    "    <column name=\"baz\" type=\"STRING\" width=\"10\" nullable=\"true\"/>\n" +
    "    <column name=\"bob\" type=\"DECIMAL\" width=\"13\" scale=\"2\" nullable=\"true\"/>\n" +
    "    <index name=\"fizz\" unique=\"true\" columns=\"bar,baz\"/>\n" +
    "  </metadata>\n" +
    "  <data>\n" +
    "    <record id=\"1\" version=\"1\" bar=\"abc\" baz=\"123\" bob=\"456.78\"/>\n" +
    "  </data>\n" +
    "</table>";


  /**
   * Sample XML with all fields
   */
  public static String MASKED_SAMPLE =

    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
    "<table version=\"2\">\n" +
    "  <metadata name=\"Test\">\n" +
    "    <column name=\"id\" type=\"BIG_INTEGER\" primaryKey=\"true\" autoNum=\"123\"/>\n" +
    "    <column name=\"version\" type=\"INTEGER\" default=\"0\" nullable=\"true\"/>\n" +
    "    <column name=\"bar\" type=\"STRING\" width=\"10\" nullable=\"true\"/>\n" +
    "    <column name=\"baz\" type=\"STRING\" width=\"10\" nullable=\"true\"/>\n" +
    "    <column name=\"bob\" type=\"DECIMAL\" width=\"13\" scale=\"2\" nullable=\"true\"/>\n" +
    "    <index name=\"fizz\" unique=\"true\" columns=\"bar,baz\"/>\n" +
    "  </metadata>\n" +
    "  <data>\n" +
    "    <record id=\"1\" version=\"1\" baz=\"123\" bob=\"456.78\"/>\n" +
    "  </data>\n" +
    "</table>";


  /**
   * Sample XML with all fields
   */
  public static String FULL_SAMPLE_V1 =

    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
    "<table>\n" +
    "  <metadata name=\"Test\">\n" +
    "    <column name=\"bar\" type=\"STRING\" width=\"10\" nullable=\"true\"/>\n" +
    "    <column name=\"baz\" type=\"STRING\" width=\"10\" nullable=\"true\"/>\n" +
    "    <column name=\"bob\" type=\"DECIMAL\" width=\"13\" scale=\"2\" nullable=\"true\"/>\n" +
    "    <index name=\"fizz\" unique=\"true\" columns=\"bar,baz\"/>\n" +
    "  </metadata>\n" +
    "  <data>\n" +
    "    <record id=\"1\" version=\"1\" bar=\"abc\" baz=\"123\" bob=\"456.78\"/>\n" +
    "  </data>\n" +
    "</table>\n";


  /**
   * Sample XML with all fields
   */
  public static String FULL_SAMPLE_V1_UPGRADED =

    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
        "<table version=\"2\">\n" +
        "  <metadata name=\"Test\">\n" +
        "    <column name=\"id\" type=\"BIG_INTEGER\" primaryKey=\"true\"/>\n" +
        "    <column name=\"version\" type=\"INTEGER\" default=\"0\" nullable=\"true\"/>\n" +
        "    <column name=\"bar\" type=\"STRING\" width=\"10\" nullable=\"true\"/>\n" +
        "    <column name=\"baz\" type=\"STRING\" width=\"10\" nullable=\"true\"/>\n" +
        "    <column name=\"bob\" type=\"DECIMAL\" width=\"13\" scale=\"2\" nullable=\"true\"/>\n" +
        "    <index name=\"fizz\" unique=\"true\" columns=\"bar,baz\"/>\n" +
        "  </metadata>\n" +
        "  <data>\n" +
        "    <record id=\"1\" version=\"1\" bar=\"abc\" baz=\"123\" bob=\"456.78\"/>\n" +
        "  </data>\n" +
        "</table>";

  /**
   * Sample XML with reduced fields (some column fields missing, no data)
   */
  public static String REDUCED_SAMPLE =

    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
    "<table version=\"2\">\n" +
    "  <metadata name=\"Test\">\n" +
    "    <column name=\"id\" type=\"BIG_INTEGER\" primaryKey=\"true\"/>\n" +
    "    <column name=\"version\" type=\"INTEGER\" default=\"0\"/>\n" +
    "    <column name=\"bar\" type=\"STRING\"/>\n" +
    "    <column name=\"baz\" type=\"STRING\"/>\n" +
    "    <column name=\"bob\" type=\"DECIMAL\"/>\n" +
    "  </metadata>\n" +
    "  <data/>\n" +
    "</table>\n";

  /**
   * Sample XML with blob field
   */
  public static String BLOBBY_SAMPLE =

    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
    "<table version=\"2\">\n" +
    "  <metadata name=\"Test\">\n" +
    "    <column name=\"id\" type=\"BIG_INTEGER\" primaryKey=\"true\"/>\n" +
    "    <column name=\"version\" type=\"INTEGER\" default=\"0\" nullable=\"true\"/>\n" +
    "    <column name=\"noel\" type=\"STRING\" width=\"10\" nullable=\"true\"/>\n" +
    "    <column name=\"edmonds\" type=\"STRING\" width=\"10\" nullable=\"true\"/>\n" +
    "    <column name=\"blobby\" type=\"BLOB\" nullable=\"true\"/>\n" +
    "  </metadata>\n" +
    "  <data>\n" +
    "    <record id=\"1\" version=\"1\" noel=\"noel\" edmonds=\"edmonds\" blobby=\"YmxvYmJ5\"/>\n" +
    "  </data>\n" +
    "</table>";

  /**
   * Sample XML with all fields
   */
  public static String COMPOSITE_PRIMARY_KEY =

    "<?xml version=\"1.0\" encoding=\"UTF-8\"?>" +
    "<table version=\"2\">\n" +
    "  <metadata name=\"Test\">\n" +
    "    <column name=\"id\" type=\"BIG_INTEGER\" primaryKey=\"true\"/>\n" +
    "    <column name=\"version\" type=\"INTEGER\" default=\"0\" nullable=\"true\"/>\n" +
    "    <column name=\"normalColumn\" type=\"STRING\" width=\"10\" nullable=\"true\"/>\n" +
    "    <column name=\"col2\" type=\"STRING\" width=\"10\" primaryKey=\"true\"/>\n" +
    "  </metadata>\n" +
    "  <data/>\n" +
    "</table>";
}
