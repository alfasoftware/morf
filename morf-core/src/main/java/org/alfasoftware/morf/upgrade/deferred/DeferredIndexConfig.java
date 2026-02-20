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

/**
 * Configuration for the deferred index execution mechanism.
 *
 * <p>All time values are in seconds.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class DeferredIndexConfig {

  /**
   * Maximum number of retry attempts before marking an operation as permanently FAILED.
   */
  private int maxRetries = 3;

  /**
   * Number of threads in the executor thread pool.
   */
  private int threadPoolSize = 1;

  /**
   * Operations that have been IN_PROGRESS for longer than this threshold (in seconds)
   * are considered stale — i.e. the executor that claimed them has crashed — and will
   * be recovered by {@code DeferredIndexRecoveryService}.
   *
   * <p>This threshold must be set high enough to avoid interfering with legitimately
   * running index builds on other nodes (e.g. a live PostgreSQL
   * {@code CREATE INDEX CONCURRENTLY} also produces an {@code indisvalid=false} index
   * mid-build). Default: 4 hours (14400 seconds).</p>
   */
  private long staleThresholdSeconds = 14_400L;

  /**
   * Maximum time in seconds to wait for a single index build operation to complete
   * before treating it as failed. Default: 4 hours (14400 seconds).
   */
  private long operationTimeoutSeconds = 14_400L;


  /**
   * @see #maxRetries
   */
  public int getMaxRetries() {
    return maxRetries;
  }


  /**
   * @see #maxRetries
   */
  public void setMaxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
  }


  /**
   * @see #threadPoolSize
   */
  public int getThreadPoolSize() {
    return threadPoolSize;
  }


  /**
   * @see #threadPoolSize
   */
  public void setThreadPoolSize(int threadPoolSize) {
    this.threadPoolSize = threadPoolSize;
  }


  /**
   * @see #staleThresholdSeconds
   */
  public long getStaleThresholdSeconds() {
    return staleThresholdSeconds;
  }


  /**
   * @see #staleThresholdSeconds
   */
  public void setStaleThresholdSeconds(long staleThresholdSeconds) {
    this.staleThresholdSeconds = staleThresholdSeconds;
  }


  /**
   * @see #operationTimeoutSeconds
   */
  public long getOperationTimeoutSeconds() {
    return operationTimeoutSeconds;
  }


  /**
   * @see #operationTimeoutSeconds
   */
  public void setOperationTimeoutSeconds(long operationTimeoutSeconds) {
    this.operationTimeoutSeconds = operationTimeoutSeconds;
  }
}
