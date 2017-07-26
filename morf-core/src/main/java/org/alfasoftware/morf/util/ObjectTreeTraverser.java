/*
 * Copyright 2017 Alfa Financial Software Licensed under the Apache License,
 * Version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law
 * or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

package org.alfasoftware.morf.util;

/**
 * Supports the iteration of a {@link Callback} over any type, propagating the
 * call if the type implements {@link Driver}.
 *
 * For example, given:
 * new Driver(){
 *  void drive(ObjectTreeTraverser dispatcher){
 *    dispatcher
 *      .dispatch(this.childElementOne)
 *      .dispatch(this.childElementTwo);
 *  }
 * }
 *
 * The object graph can be traversed:
 * ObjectTreeTraverser.forCallback(new Callback(){
 *  void visit(Object object){
 *    // Do something with nodes
 *  }
 * }).dispatch(driver);
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public final class ObjectTreeTraverser {

  private final Callback callback;


  private ObjectTreeTraverser(Callback callback) {
    this.callback = callback;
  }


  /**
   * Creates a new traverser for a callback.
   * @param callback The callback
   * @return the resulting traverser
   */
  public static ObjectTreeTraverser forCallback(Callback callback) {
    return new ObjectTreeTraverser(callback);
  }


  /**
   * Invokes the callback on the object. If the object implements Driver then
   * its drive method is invoked, propagating this traverser. Iterables and
   * arrays will have the callback invoked on their elements.
   *
   * @param object the object node in the object graph.
   * @return this, for method chaining.
   * @param <T> the type of object to traverse
   */
  public <T> ObjectTreeTraverser dispatch(T object) {
    if (object == null) {
      return this;
    }

    if (object instanceof Iterable<?>) {
      for (Object element : (Iterable<?>) object) {
        dispatch(element);
      }
      return this;
    }

    if (object instanceof Object[]) {
      for (Object element : (Object[]) object) {
        dispatch(element);
      }
      return this;
    }

    callback.visit(object);
    if (object instanceof Driver) {
      ((Driver) object).drive(this);
    }
    return this;
  }

  /**
   * Implement on any object which can drive a {@link ObjectTreeTraverser},
   * pointing it at more elements to traverse.
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  public interface Driver {
    /**
     * Any class implementing this method will use the
     * {@link ObjectTreeTraverser} to parse its contents through the
     * {@link Callback}.
     * @param dispatcher The strategy to parse content
     */
    void drive(ObjectTreeTraverser dispatcher);
  }

  /**
   * Defines the generic callback interface.
   *
   * @author Copyright (c) Alfa Financial Software 2017
   */
  public interface Callback {


    /**
     * Invoked on each element of the object tree that is being traversed.
     * This similar to the Visitor pattern but ObjectTreeTraverser has the
     * calling responsibility.
     *
     * @param object the object to invoke the callback on.
     */
    void visit(Object object);
  }
}
