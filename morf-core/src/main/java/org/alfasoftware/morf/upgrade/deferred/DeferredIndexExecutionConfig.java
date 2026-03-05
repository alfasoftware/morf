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
 * <p>Controls runtime behaviour of the {@link DeferredIndexExecutor}:
 * thread pool sizing, retry policy, and timeout limits.</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class DeferredIndexExecutionConfig {

  /**
   * Maximum number of retry attempts before marking an operation as permanently FAILED.
   */
  private int maxRetries = 3;

  /**
   * Number of threads in the executor thread pool.
   */
  private int threadPoolSize = 1;

  /**
   * Maximum time in seconds to wait for all deferred index operations to complete
   * via {@link DeferredIndexService#awaitCompletion(long)}.
   * Default: 8 hours (28800 seconds).
   */
  private long executionTimeoutSeconds = 28_800L;

  /**
   * Base delay in milliseconds between retry attempts. Each successive retry doubles
   * this delay (exponential backoff). Default: 5000 ms (5 seconds).
   */
  private long retryBaseDelayMs = 5_000L;

  /**
   * Maximum delay in milliseconds between retry attempts. The exponential backoff
   * will never exceed this value. Default: 300000 ms (5 minutes).
   */
  private long retryMaxDelayMs = 300_000L;


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
   * @see #executionTimeoutSeconds
   */
  public long getExecutionTimeoutSeconds() {
    return executionTimeoutSeconds;
  }


  /**
   * @see #executionTimeoutSeconds
   */
  public void setExecutionTimeoutSeconds(long executionTimeoutSeconds) {
    this.executionTimeoutSeconds = executionTimeoutSeconds;
  }


  /**
   * @see #retryBaseDelayMs
   */
  public long getRetryBaseDelayMs() {
    return retryBaseDelayMs;
  }


  /**
   * @see #retryBaseDelayMs
   */
  public void setRetryBaseDelayMs(long retryBaseDelayMs) {
    this.retryBaseDelayMs = retryBaseDelayMs;
  }


  /**
   * @see #retryMaxDelayMs
   */
  public long getRetryMaxDelayMs() {
    return retryMaxDelayMs;
  }


  /**
   * @see #retryMaxDelayMs
   */
  public void setRetryMaxDelayMs(long retryMaxDelayMs) {
    this.retryMaxDelayMs = retryMaxDelayMs;
  }
}
