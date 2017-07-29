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

package org.alfasoftware.morf.sql;

import org.alfasoftware.morf.util.Builder;

/**
 * Temporary class to wrap instances in a builder until all elements have
 * corresponding builders. This is to ensure other methods can return builders
 * rather than instances.
 *
 * @author Copyright (c) Alfa Financial Software
 */
public class TempTransitionalBuilderWrapper {

  /**
   * Temporary builder wrapper. This is to ensure other methods can return builders rather than instances, until
   *
   *
   * @param instance the instance to wrap.
   * @return a builder that wraps the given instance.
   * @param <T> the type of the instance being wrapped
   */
  public static <T> Builder<T> wrapper(final T instance){
    return new BuilderWrapper<>(instance);
  }

  private static final class BuilderWrapper<T> implements Builder<T> {

    private final T instance;

    private BuilderWrapper(T instance) {
      this.instance = instance;
    }

    @Override
    public T build() {
      return instance;
    }


    @Override
    public String toString() {
      return instance == null ? null : instance.toString();
    }
  }
}