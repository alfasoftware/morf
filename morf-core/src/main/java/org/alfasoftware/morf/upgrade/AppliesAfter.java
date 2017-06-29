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

package org.alfasoftware.morf.upgrade;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @deprecated No longer used. Remove all uses.
 *
 * @author Copyright (c) Alfa Financial Software 2010
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
@Deprecated
public @interface AppliesAfter {

  /**
   * @return Unordered list of upgrade step names.
   */
  public Class<?>[] value();

  /**
   * This is a marker class. In any given build exactly one Schema upgrade should
   * specify this class in its {@link AppliesAfter} list.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  public static final class START extends VersionMarkerUpgradeStep {}

}
