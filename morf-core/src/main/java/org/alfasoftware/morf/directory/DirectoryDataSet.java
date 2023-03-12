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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Allows reading of a data set from a directory.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public class DirectoryDataSet extends BaseDataSetReader implements DirectoryStreamProvider.DirectoryInputStreamProvider, DirectoryStreamProvider.DirectoryOutputStreamProvider {

  /**
   * The directory to read from.
   */
  private final Path directory;
  private final String suffix;

  /**
   * Creates a directory based data set reader based on the directory <var>file</var>.
   *
   * @param directory The directory to access.
   */
  public DirectoryDataSet(String suffix, Path directory) {
    super();
    this.suffix = suffix;
    this.directory = directory;

    if (!Files.isDirectory(directory)) {
      throw new IllegalArgumentException("[" + directory + "] is not a directory");
    }

    // read the files in the directory
    // Do it here because DirectoryDataSet historically did not have to be "open" to be used.
    try {
      Files.list(directory).filter(path -> path.endsWith(suffix))
              .forEach(path -> addTableName(
                      path.getFileName().toString().replaceAll("\\." + suffix, ""),
                      path.getFileName().toString()));
    } catch (IOException e) {
      e.printStackTrace();
    }
  }


  /**
   * @see DirectoryOutputStreamProvider#clearDestination()
   */
  @Override
  public void clearDestination() {
    try {
      Files.list(directory)
              .filter(path -> !path.getFileName().startsWith("."))
              .forEach(this::deleteFileOrDirectory);
        // skip files/folders that start with . such as .svn
    } catch (IOException e) {
      throw new RuntimeException("Exception occurred when trying to clear existing directory", e);
    }
  }


  private void deleteFileOrDirectory(Path path) {
    if (Files.isDirectory(path)) {
      // clear it out (recursively) if needed...
      if (!Files.isSymbolicLink(path)) {
        try {
          Files.list(path).forEach(this::deleteFileOrDirectory);
        } catch (IOException ex) {
          throw new RuntimeException("Exception occurred when trying to clear existing directory", ex);
        }
      }
    }
    try {
      Files.delete(path);
    } catch (IOException e) {
      throw new RuntimeException("Exception cleaning output directory, file [" + path + "]");
    }

  }


  /**
   * @see DirectoryInputStreamProvider#openInputStreamForTable(String)
   */
  @Override
  public InputStream openInputStreamForTable(String tableName) {
    try {
      return Files.newInputStream(directory.resolve(fileNameForTable(tableName)));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Error opening output stream", e);
    } catch (IOException e) {
      throw new RuntimeException("Error opening output stream", e);
    }
  }


  /**
   * @see DirectoryOutputStreamProvider#openOutputStreamForTable(String)
   */
  @Override
  public OutputStream openOutputStreamForTable(String tableName) {
    try {
      return Files.newOutputStream(directory.resolve(tableName + "." + suffix));
    } catch (FileNotFoundException e) {
      throw new RuntimeException("Error opening output stream", e);
    } catch (IOException e) {
      throw new RuntimeException("Error opening output stream", e);
    }
  }
}
