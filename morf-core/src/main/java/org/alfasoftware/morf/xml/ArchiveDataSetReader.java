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

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.alfasoftware.morf.directory.BaseDataSetReader;
import org.alfasoftware.morf.directory.DirectoryStreamProvider;

/**
 * Allows reading of data sets based on an archive (zip) file.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class ArchiveDataSetReader extends BaseDataSetReader implements DirectoryStreamProvider.DirectoryInputStreamProvider {

  /**
   * The file to read the archive from.
   */
  private final File file;

  /**
   * References the zip archive.
   */
  private ZipFile zipFile;

  private final Pattern filenamePattern = Pattern.compile("(\\w+)\\.xml");

  /**
   * Creates an archive data set linked to the specified <var>file</var>.
   *
   * @param file The archive file to use.
   */
  public ArchiveDataSetReader(File file) {
    super();
    this.file = file;
  }


  /**
   * @see org.alfasoftware.morf.xml.XmlStreamProvider#open()
   */
  @Override
  public void open() {
    super.open();

    if (zipFile != null) {
      throw new IllegalStateException("Archive data set instance for [" + file + "] already open");
    }

    try {
      zipFile = new ZipFile(file);
    } catch (IOException e) {
      throw new RuntimeException("Error opening zip archive [" + file + "]", e);
    }

    ArrayList<? extends ZipEntry> list = Collections.list(zipFile.entries());

    if (list.isEmpty()) {
      throw new IllegalArgumentException("Archive file [" + file + "] is empty");
    }

    boolean tableAdded = false;
    // add all the table names
    for (ZipEntry entry : Collections.list(zipFile.entries())) {
      Matcher matcher = filenamePattern.matcher(entry.getName());
      if (matcher.matches()) {
        addTableName(matcher.group(1), entry.getName());
        tableAdded = true;
      }
    }

    if (!tableAdded) {
      throw new IllegalArgumentException("Archive file [" + file + "] contains no tables in root directory");
    }
  }


  /**
   * @see org.alfasoftware.morf.xml.XmlStreamProvider#close()
   */
  @Override
  public void close() {
    super.close();

    clear();

    if (zipFile == null) {
      throw new IllegalStateException("Archive data set has not been opened");
    }

    try {
      zipFile.close();
    } catch (IOException e) {
      throw new RuntimeException("Error closing zip archive [" + file + "]", e);
    }
  }


  /**
   * @see org.alfasoftware.morf.xml.XmlStreamProvider.XmlInputStreamProvider#openInputStreamForTable(java.lang.String)
   */
  @Override
  public InputStream openInputStreamForTable(String tableName) {
    if (zipFile == null) {
      throw new IllegalStateException("Archive data set has not been opened");
    }

    String fileName = fileNameForTable(tableName);

    // Find the right zip entry
    ZipEntry entry = zipFile.getEntry(fileName);

    if (entry == null) {
      throw new IllegalArgumentException("Could not find zip entry [" + fileName + "] for table [" + tableName + "]");
    }

    try {
      return zipFile.getInputStream(entry);
    } catch (IOException e) {
      throw new RuntimeException("Error accessing zip entry [" + tableName + "]", e);
    }
  }
}
