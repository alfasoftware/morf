package org.alfasoftware.morf.jdbc.nuodb;

import java.math.BigDecimal;

import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.sql.SqlUtils;
import org.alfasoftware.morf.sql.element.AliasedField;
import org.alfasoftware.morf.sql.element.Cast;

/**
 * Helper class for NuoDB fixes and workarounds. The methods on here should
 * be removed once fixes are delivered by NuoDB in later releases.
 *
 * @author Copyright (c) CHP Consulting Ltd. 2017
 */
public class NuoDBHelper {

  /**
   * Convert a string to its double value. This is used as often
   * NuoDB returns a varchar rather than the correct numeric type.
   */
  public static Double nuoDBConvertStringToDouble(String stringOfDouble) {
    return Double.valueOf(stringOfDouble);
  }

  /**
   * Cast an aliased field to a String. Even though NuoDB will often do
   * this itself, we need to ensure all databases also perform the cast.
   * This is because we often have make changes in the associated java
   * to workaround NuoDB's return types and this then affects the other
   * databases.
   */
  public static Cast nuoDBCastToString(AliasedField referenceToCast, int length) {
    return SqlUtils.cast(referenceToCast).asType(DataType.STRING, length);
  }


  /**
   * Converts a BigDecimal to its string value. This is because NuoDB often returns
   * a string rather than a numeric value and we need to be able compare the result
   * of the database query to a big decimal amount.
   */
  public static String nuoDBConvertBigDecimalToPlainString(BigDecimal bigDecimal) {
    return bigDecimal.toPlainString();
  }
}

