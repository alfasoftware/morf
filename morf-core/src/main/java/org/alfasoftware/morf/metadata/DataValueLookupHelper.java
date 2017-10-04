package org.alfasoftware.morf.metadata;

import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Helper code for {@link DataValueLookup} default methods, which we don't want to appear on the interface.
 *
 * @author Copyright (c) CHP Consulting Ltd. 2017
 */
class DataValueLookupHelper {
  static final DateTimeFormatter FROM_YYYY_MM_DD = DateTimeFormat.forPattern("yyyy-MM-dd");
}

