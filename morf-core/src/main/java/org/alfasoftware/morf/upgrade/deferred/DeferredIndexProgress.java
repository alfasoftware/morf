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
 * Snapshot of deferred index execution progress, returned by
 * {@link DeferredIndexService#getProgress()}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
public class DeferredIndexProgress {

  private final int total;
  private final int completed;
  private final int failed;
  private final int remaining;


  /**
   * Constructs a progress snapshot.
   *
   * @param total     total number of deferred indexes to build.
   * @param completed number of indexes successfully built.
   * @param failed    number of indexes that failed permanently.
   * @param remaining number of indexes not yet attempted or still in progress.
   */
  public DeferredIndexProgress(int total, int completed, int failed, int remaining) {
    this.total = total;
    this.completed = completed;
    this.failed = failed;
    this.remaining = remaining;
  }


  /**
   * @return total number of deferred indexes to build.
   */
  public int getTotal() {
    return total;
  }


  /**
   * @return number of indexes successfully built.
   */
  public int getCompleted() {
    return completed;
  }


  /**
   * @return number of indexes that failed permanently.
   */
  public int getFailed() {
    return failed;
  }


  /**
   * @return number of indexes not yet attempted or still in progress.
   */
  public int getRemaining() {
    return remaining;
  }


  /**
   * @see java.lang.Object#toString()
   */
  @Override
  public String toString() {
    return "DeferredIndexProgress [total=" + total + ", completed=" + completed
        + ", failed=" + failed + ", remaining=" + remaining + "]";
  }
}
