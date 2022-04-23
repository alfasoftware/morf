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

package org.alfasoftware.morf.directory;

import java.io.*;
import java.nio.file.Files;
import java.util.Arrays;

/**
 * Allows reading of a data set from a directory.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class DirectoryDataSet extends BaseDataSetReader implements DirectoryStreamProvider.DirectoryInputStreamProvider, DirectoryStreamProvider.DirectoryOutputStreamProvider {

  /**
   * The directory to read from.
   */
  private final File directory;

  /**
   * Creates a directory based data set reader based on the directory <var>file</var>.
   *
   * @param directory The directory to access.
   */
  public DirectoryDataSet(File directory) {
    super();
    this.directory = directory;

    if (!directory.isDirectory()) {
      throw new IllegalArgumentException("[" + directory + "] is not a directory");
    }

    // read the files in the directory
    // Do it here because DirectoryDataSet historically did not have to be "open" to be used.
    for (File file : directory.listFiles()) {
      if (file.getName().matches(".*\\.xml")) {
        addTableName(file.getName().replaceAll("\\.xml", ""), file.getName());
      }
    }
  }


  /**
   * @see DirectoryOutputStreamProvider#clearDestination()
   */
  @Override
  public void clearDestination() {
    for (File file : directory.listFiles()) {
      // skip files/folders that start with . such as .svn
      if (file.getName().startsWith(".")) continue;

      deleteFileOrDirectory(file);
    }
  }


  private void deleteFileOrDirectory(File file) {
    if (file.isDirectory()) {
      // clear it out (recursively) if needed...
      if (!Files.isSymbolicLink(file.toPath())) {
        Arrays.stream(file.listFiles()).forEach(this::deleteFileOrDirectory);
      }
    }
    if (!file.delete()) {
      throw new RuntimeException("Exception cleaning output directory, file [" + file + "]");
    }
  }


  /**
   * @see DirectoryInputStreamProvider#openInputStreamForTable(String)
   */
  @Override
  public InputStream openInputStreamForTable(String tableName) {
    try {
      return new FileInputStream(new File(directory, fileNameForTable(tableName)));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Error opening output stream", e);
    }
  }


  /**
   * @see DirectoryOutputStreamProvider#openOutputStreamForTable(String)
   */
  @Override
  public OutputStream openOutputStreamForTable(String tableName) {
    try {
      return new FileOutputStream(new File(directory, tableName + ".xml"));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Error opening output stream", e);
    }
  }
}
