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

package org.alfasoftware.morf.metadata;

import java.util.Arrays;

import org.alfasoftware.morf.sql.SelectStatement;

/**
 * Implements {@linkplain View} as a bean
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
class ViewBean implements View {

  private final String name;
  private final SelectStatement selectStatement;
  private final boolean knowsSelectStatement;
  private final String[] dependencies;
  private final boolean knowsDependencies;


  /**
   * Create a new view
   *
   * @param name The view name
   * @param selectStatement The select statement which defines both the view
   *          metadata and its behaviour.
   */
  ViewBean(String name, SelectStatement selectStatement, String... dependencies) {
    super();
    this.name = name;
    this.selectStatement = selectStatement;
    this.knowsSelectStatement = selectStatement != null;
    this.knowsDependencies = true;
    this.dependencies = dependencies.clone();
  }


  /**
   * Copy an existing {@link View}.
   *
   * @param view The view to copy.
   */
  ViewBean(View view) {
    super();
    this.name = view.getName();
    this.selectStatement = view.knowsSelectStatement() ? view.getSelectStatement().deepCopy() : null;
    this.knowsSelectStatement = selectStatement != null;
    this.knowsDependencies = view.knowsDependencies();
    this.dependencies = view.knowsDependencies() ? view.getDependencies() : null;
  }


  /**
   * @see org.alfasoftware.morf.metadata.View#getName()
   */
  @Override
  public String getName() {
    return name;
  }


  /**
   * @see org.alfasoftware.morf.metadata.View#getSelectStatement()
   */
  @Override
  public SelectStatement getSelectStatement() {
    if (!knowsSelectStatement)
      throw new UnsupportedOperationException("Unable to return select statement for view [" + name + "]");

    return selectStatement;
  }


  /**
   * @see org.alfasoftware.morf.metadata.View#knowsSelectStatement()
   */
  @Override
  public boolean knowsDependencies() {
    return knowsDependencies;
  }


  /**
   * @see org.alfasoftware.morf.metadata.View#knowsSelectStatement()
   */
  @Override
  public boolean knowsSelectStatement() {
    return knowsSelectStatement;
  }


  /**
   * @see org.alfasoftware.morf.metadata.View#getDependencies()
   */
  @Override
  public String[] getDependencies() {
    return Arrays.copyOf(dependencies, dependencies.length);
  }

  @Override
  public String toString() {
    return "View-" + getName();
  }
}
