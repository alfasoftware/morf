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

import com.google.common.io.ByteStreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/**
 * Allows reading of data sets based on an archive (zip) file.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
class ArchiveDataSetWriter implements DirectoryStreamProvider.DirectoryOutputStreamProvider {

  /**
   * A read me entry to be included in all created zip files.
   */
  private static final String READ_ME = "This is a data set archive file.";

  /**
   * Identifies the archive to access.
   */
  private final Path path;

  private final String suffix;

  /**
   * References the zip archive.
   */
  private AdaptedZipOutputStream zipOutput;

  /**
   * Creates an archive data set linked to the specified <var>path</var>.
   *
   * @param path The archive file to use.
   */
  ArchiveDataSetWriter(String suffix, Path path) {
    super();
    this.suffix = suffix;
    this.path = path;
  }


  /**
   * @see org.alfasoftware.morf.directory.DirectoryStreamProvider.DirectoryOutputStreamProvider#clearDestination()
   */
  @Override
  public void clearDestination() {
    // No-op. Done by the open method.
  }


  /**
   * @see org.alfasoftware.morf.directory.DirectoryStreamProvider#open()
   */
  @Override
  public void open() {
    if (zipOutput != null) {
      throw new IllegalStateException("Archive data set instance for [" + path + "] already open");
    }

    try {
      zipOutput = new AdaptedZipOutputStream(Files.newOutputStream(path));

      // Put the read me entry in
      ZipEntry entry = new ZipEntry("_ReadMe.txt");
      zipOutput.putNextEntry(entry);
      ByteStreams.copy(new ByteArrayInputStream(READ_ME.getBytes("UTF-8")), zipOutput);
    } catch (Exception e) {
      throw new RuntimeException("Error opening zip archive [" + path + "]", e);
    }
  }


  /**
   * @see org.alfasoftware.morf.xml.XmlStreamProvider#close()
   */
  @Override
  public void close() {
    if (zipOutput == null) {
      throw new IllegalStateException("Archive data set has not been opened");
    }

    try {
      zipOutput.reallyClose();
    } catch (IOException e) {
      throw new RuntimeException("Error closing zip archive [" + path + "]", e);
    }
  }


  /**
   * @see org.alfasoftware.morf.directory.DirectoryStreamProvider.DirectoryOutputStreamProvider#openOutputStreamForTable(java.lang.String)
   */
  @Override
  public OutputStream openOutputStreamForTable(String tableName) {
    if (zipOutput == null) {
      throw new IllegalStateException("Archive data set has not been opened");
    }

    try {
      ZipEntry entry = new ZipEntry(tableName + "." + suffix);
      zipOutput.putNextEntry(entry);

      // Make sure the caller can't actually close the underlying stream
      return zipOutput;
    } catch (IOException e) {
      throw new RuntimeException("Error creating new zip entry in archive [" + path + "]", e);
    }
  }


  /**
   * Allows a zip stream to be returned to callers who can safely call
   * {@link #close()} without actually closing the stream.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private static final class AdaptedZipOutputStream extends ZipOutputStream {

    /**
     * @param out The output stream to write to.
     */
    public AdaptedZipOutputStream(OutputStream out) {
      super(out);
    }


    /**
     * @see java.util.zip.ZipOutputStream#close()
     */
    @Override
    public void close() throws IOException {
      // Suppress the close
    }


    /**
     * @see java.util.zip.ZipOutputStream#close()
     * @throws IOException If the exception is thrown from {@link ZipOutputStream#close()}
     */
    public void reallyClose() throws IOException {
      super.close();
    }
  }
}
