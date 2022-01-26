package org.alfasoftware.morf.upgrade;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Carries information about the tables (table names) which are read (are not
 * modified in <b>any</b> way) by the annotated upgrade step.
 * </p>
 * In the context of Graph Based Upgrade this annotation is mutually exclusive
 * to {@link ExclusiveExecution} but may be complemented by
 * {@link UpgradeModifies}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface UpgradeReads {

  /**
   * @return names of the tables which are read (are not modified in <b>any</b>
   *         way) by the annotated upgrade step.
   */
  String[] value();
}
