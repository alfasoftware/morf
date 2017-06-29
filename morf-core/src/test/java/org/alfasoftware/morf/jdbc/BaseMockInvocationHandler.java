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

package org.alfasoftware.morf.jdbc;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;

/**
 * A simple invocation handler for dynamic proxies which can be used to return
 * "dummy" values for primitive types.
 *
 * <p>Typically, specific usages would require an anonymous inner subclass:</p>
 * <pre><code>
 * return BaseMockInvocationHandler.createProxy(ResultSet.class, new BaseMockInvocationHandler() {
 *   @Override
 *   public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
 *     if (method.getName().equals("next"))
 *       return StringUtils.contains(sqlStm, "0fde0d93-f57e-405c-81e9-245ef1ba0594");
 *
 *     return super.invoke(proxy, method, args);
 *   }
 * });
 * </code></pre>
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class BaseMockInvocationHandler implements InvocationHandler {

  /**
   * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object,
   *      java.lang.reflect.Method, java.lang.Object[])
   */
  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
    Class<?> rt = method.getReturnType();
    if (rt.equals(Boolean.TYPE)) {
      return false;
    } else if (rt.equals(Long.TYPE) || rt.equals(Integer.TYPE) || rt.equals(Short.TYPE) || rt.equals(Double.TYPE)) {
      return 0;
    }

    return null;
  }


  /**
   * Convenience method for use with this, or other, {@link InvocationHandler}s where
   * a single interface is required.
   *
   * @param <T> Type.
   * @param clazz Class of interface.
   * @param handler Handler to use.
   * @return Instance of {@code clazz}.
   */
  public static <T> T createProxy(Class<T> clazz, InvocationHandler handler) {
    return clazz.cast(Proxy.newProxyInstance(BaseMockInvocationHandler.class.getClassLoader(), new Class<?>[] { clazz }, handler));
  }
}
