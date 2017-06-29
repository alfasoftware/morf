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

package org.alfasoftware.morf.diagnostics;

import java.io.File;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import org.alfasoftware.morf.dataset.DataSetConnector;
import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.jdbc.DatabaseDataSetProducer;
import org.alfasoftware.morf.xml.XmlDataSetConsumer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.inject.Inject;

/**
 * Class that will dump the current state of an ALFA database.
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class DatabaseDumper {

  /**
   * Standard logger.
   */
  private static final Log log = LogFactory.getLog(DatabaseDumper.class);

  /**
   * Default location for the database dumps.
   */
  public static final String DEFAULT_PATH = "/var/database.dumps";

  /**
   * Input database resources.
   */
  private final ConnectionResources connectionResources;


  /**
   * Creates a {@link DatabaseDumper}, that has the ability to dump
   * the database state at an arbitrary point.
   *
   * @param connectionResources Input database resources
   */
  @Inject
  public DatabaseDumper(ConnectionResources connectionResources) {
    this.connectionResources = connectionResources;
  }


  /**
   * Dumps the current database state to a file, located in {@link #DEFAULT_PATH}
   *
   * @param dumpName A string to prepend to the dump name, to aid in identifying it.
   * @throws IOException If the output directory cannot be created.
   */
  public void dump(String dumpName) throws IOException{
    dump(new File(DEFAULT_PATH), dumpName);
  }


  /**
   * Dumps the current database state to a file.
   *
   * @param outputRootDirectory The directory where database dumps are to be written to.
   * @param dumpName A string to prepend to the dump name, to aid in identifying it.
   *
   * @throws IOException If the output directory cannot be created.
   */
  public void dump(File outputRootDirectory, String dumpName) throws IOException {
    log.warn("********************************************************");
    log.warn("***********************WARNING!!!!!!********************");
    log.warn("**************DATABASE DUMP BEING EXECUTED**************");
    log.warn("********************************************************");
    log.warn("***IF YOU DO NOT EXPECT THIS MESSAGE CONTACT SUPPORT****");
    log.warn("********************************************************");

    if (!outputRootDirectory.exists()) {
      boolean success = outputRootDirectory.mkdirs();
      if(!success) {
        throw new IOException(String.format("Could not create root directory: [%s]", outputRootDirectory.getPath()));
      }
    } else if(outputRootDirectory.isFile()) {
      throw new IllegalArgumentException(String.format("Input file: [%s] was not a directory.", outputRootDirectory.getPath()));
    }

    // Create the input
    DataSetProducer input = new DatabaseDataSetProducer(connectionResources);

    // Create the output
    DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd-HHmm");
    File outputFile = new File(outputRootDirectory, dumpName + "-" + dateFormat.format(System.currentTimeMillis()) + ".zip");

    boolean success = outputFile.createNewFile();
    if(!success) {
      throw new IOException(String.format("Could not create output file: [%s]", outputFile.getPath()));
    }

    log.info(String.format("Output file will be: [%s]", outputFile.getAbsolutePath()));
    DataSetConsumer output = new XmlDataSetConsumer(outputFile);

    // Run the extraction.
    log.info(String.format("Starting database dump at [%s]", DateFormat.getInstance().format(System.currentTimeMillis())));
    new DataSetConnector(input, output).connect();
    log.info(String.format("Completed database dump at [%s]", DateFormat.getInstance().format(System.currentTimeMillis())));
  }
}