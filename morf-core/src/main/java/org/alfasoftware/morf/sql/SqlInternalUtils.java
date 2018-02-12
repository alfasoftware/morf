package org.alfasoftware.morf.sql;

import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Direction;
import org.alfasoftware.morf.sql.element.FieldReference;

/**
 * Utility methods for SQL classes.
 *
 * @author Copyright (c) Alfa Financial Software 2017
 */
class SqlInternalUtils {

  /**
   * Sets the fields in an ORDER BY to use ascending order if not specified.
   *
   * @param orderBys The order by criteria
   */
  static void defaultOrderByToAscending(Iterable<AliasedField> orderBys) {
    for (AliasedField currentField : orderBys) {
      if (currentField instanceof FieldReference && ((FieldReference) currentField).getDirection() == Direction.NONE) {
        ((FieldReference) currentField).setDirection(Direction.ASCENDING);
      }
    }
  }
}