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

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;

import org.alfasoftware.morf.xml.XmlStreamProvider.XmlOutputStreamProvider;

/**
 * Testing implementation to catch result XML.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
public final class DummyXmlOutputStreamProvider implements XmlOutputStreamProvider {

  /**
   * Holds the output stream for test data.
   */
  private final ByteArrayOutputStream testOutputStream = new ByteArrayOutputStream();

  /**
   * Track whether the clear has been called.
   */
  private boolean cleared;

  /**
   * @see org.alfasoftware.morf.xml.XmlStreamProvider#close()
   */
  @Override
  public void close() {
    // Nothing to do
  }


  /**
   * @see org.alfasoftware.morf.xml.XmlStreamProvider#open()
   */
  @Override
  public void open() {
    // Nothing to do
  }


  /**
   * @see org.alfasoftware.morf.xml.XmlStreamProvider.XmlOutputStreamProvider#openOutputStreamForTable(java.lang.String)
   */
  @Override
  public OutputStream openOutputStreamForTable(String tableName) {
    return testOutputStream;
  }


  /**
   * @see org.alfasoftware.morf.xml.XmlStreamProvider.XmlOutputStreamProvider#clearDestination()
   */
  @Override
  public void clearDestination() {
    cleared = true;
  }


  /**
   * @return Convert the output to a String
   */
  public String getXmlString() {
    return new String(testOutputStream.toByteArray());
  }


  /**
   * @return Whether clearDestination was called.
   */
  public boolean cleared() {
    return cleared;
  }
}
