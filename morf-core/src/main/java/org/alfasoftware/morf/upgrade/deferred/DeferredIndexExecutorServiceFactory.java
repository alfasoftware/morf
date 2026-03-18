/* Copyright 2026 Alfa Financial Software
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

package org.alfasoftware.morf.upgrade.deferred;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.inject.ImplementedBy;

/**
 * Factory for creating the {@link ExecutorService} used by
 * {@link DeferredIndexExecutor} to build indexes asynchronously.
 *
 * <p>The default implementation creates a fixed-size thread pool with
 * daemon threads, which is suitable for standalone JVM processes. In a
 * managed environment such as a servlet container (e.g. Jetty), the
 * adopting application should override this binding to provide a
 * container-managed {@link ExecutorService} (e.g. wrapping a commonj
 * {@code WorkManager}) so that threads participate in the container's
 * lifecycle and classloader management.</p>
 *
 * <p>Override example in a Guice module:</p>
 * <pre>
 * bind(DeferredIndexExecutorServiceFactory.class)
 *     .toInstance(size -&gt; new CommonJExecutorService(workManager, size));
 * </pre>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
@ImplementedBy(DeferredIndexExecutorServiceFactory.Default.class)
public interface DeferredIndexExecutorServiceFactory {

  /**
   * Creates an {@link ExecutorService} with the given thread pool size.
   *
   * @param threadPoolSize the number of threads in the pool.
   * @return a new {@link ExecutorService}.
   */
  ExecutorService create(int threadPoolSize);


  /**
   * Default implementation that creates a fixed-size thread pool with
   * daemon threads named {@code DeferredIndexExecutor-N}.
   */
  class Default implements DeferredIndexExecutorServiceFactory {

    private int threadCount;

    @Override
    public ExecutorService create(int threadPoolSize) {
      return Executors.newFixedThreadPool(threadPoolSize, r -> {
        Thread t = new Thread(r, "DeferredIndexExecutor-" + ++threadCount);
        t.setDaemon(true);
        return t;
      });
    }
  }
}
