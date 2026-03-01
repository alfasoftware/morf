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

import java.time.LocalDateTime;

/**
 * Shared timestamp utilities for the deferred index subsystem.
 *
 * <p>Timestamps are stored as {@code long} values in the format
 * {@code yyyyMMddHHmmss} (e.g. {@code 20260301143022} for
 * 2026-03-01 14:30:22).</p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2026
 */
final class DeferredIndexTimestamps {

  private DeferredIndexTimestamps() {
    // Utility class
  }


  /**
   * @return the current date-time as a {@code yyyyMMddHHmmss} long.
   */
  static long currentTimestamp() {
    return toTimestamp(LocalDateTime.now());
  }


  /**
   * Converts a {@link LocalDateTime} to the {@code yyyyMMddHHmmss} long format.
   *
   * @param dt the date-time to convert.
   * @return the timestamp as a long.
   */
  static long toTimestamp(LocalDateTime dt) {
    return dt.getYear() * 10_000_000_000L
        + dt.getMonthValue() * 100_000_000L
        + dt.getDayOfMonth() * 1_000_000L
        + dt.getHour() * 10_000L
        + dt.getMinute() * 100L
        + dt.getSecond();
  }
}
