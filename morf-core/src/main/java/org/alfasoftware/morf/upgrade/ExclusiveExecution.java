package org.alfasoftware.morf.upgrade;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Directs Graph Based Upgrade to execute the annotated upgrade step in an
 * exclusive way. It means that the upgrade step execution will be the only one
 * that will be processed and it will have exclusive access to the resources
 * with no need to share those with other upgrade steps. No other upgrade step
 * will be running while annotated upgrade step is executed.
 * <p>
 * It is also possible to use configuration (so no new app build required) to
 * mark upgrades for exclusive execution. See
 * {@link org.alfasoftware.morf.upgrade.Upgrade#findPath(org.alfasoftware.morf.metadata.Schema, java.util.Collection, java.util.Collection, java.util.Set)
 * findPath} for the entry point.
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2022
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface ExclusiveExecution {
  // nothing
}
