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

import java.util.Collection;

import org.alfasoftware.morf.metadata.Table;

/**
 * Defines tables within a module which are not created via Domain classes,
 * and any upgrade steps on them.
 *
 * <p>It is intended that this is used infrequently, and for infrastructural
 * tables which do not have alternative mechanisms of deployment. For example,
 * if using <var>Aether</var>, {@code DomainClassContribution} is usually better.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public interface TableContribution {

  /**
   * @return Non-domain-class-derived tables present in the module.
   */
  public Collection<Table> tables();

  /**
   * @return Schema upgrades associated with those tables.
   */
  public Collection<Class<? extends UpgradeStep>> schemaUpgradeClassses();

}
