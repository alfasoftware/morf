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
package org.alfasoftware.morf.guicesupport;

import org.alfasoftware.morf.upgrade.TableContribution;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;
import org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution;

import com.google.inject.AbstractModule;
import com.google.inject.multibindings.Multibinder;

/**
 * Guice bindings for Morf module.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
public class MorfModule extends AbstractModule {
  /**
   * @see com.google.inject.AbstractModule#configure()
   */
  @Override
  protected void configure() {
    Multibinder.newSetBinder(binder(), UpgradeScriptAddition.class);

    Multibinder<TableContribution> tableMultibinder = Multibinder.newSetBinder(binder(), TableContribution.class);
    tableMultibinder.addBinding().to(DatabaseUpgradeTableContribution.class);
  }
}

