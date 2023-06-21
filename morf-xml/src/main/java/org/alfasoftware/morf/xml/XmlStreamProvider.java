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

import org.alfasoftware.morf.directory.DirectoryStreamProvider;

/**
 * Provides streams for accessing XML data sets.
 *
 * Deprecated - use DirectoryStreamProvider. This class will be removed from 2.0.0.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
@Deprecated
public interface XmlStreamProvider extends DirectoryStreamProvider {



  /**
   * Provides input streams for reading data sets from XML.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  @Deprecated
  interface XmlInputStreamProvider extends DirectoryStreamProvider.DirectoryInputStreamProvider {
  }


  /**
   * Provides output streams for writing XML.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  @Deprecated
  interface XmlOutputStreamProvider extends DirectoryStreamProvider.DirectoryOutputStreamProvider {
  }
}
