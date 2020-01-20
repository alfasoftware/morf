/* Copyright 2017 Alfa Financial Software
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

package org.alfasoftware.morf.upgrade;

import static org.alfasoftware.morf.upgrade.db.DatabaseUpgradeTableContribution.DEPLOYED_VIEWS_NAME;

import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;

import org.alfasoftware.morf.jdbc.SqlDialect;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.View;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Loads the set of view changes which need to be made to match the target schema.
 *
 * @author Copyright (c) Alfa Financial Software 2011 - 2013
 */
class ExistingViewStateLoader {
  private static final Log log = LogFactory.getLog(ExistingViewStateLoader.class);

  private final SqlDialect dialect;
  private final ExistingViewHashLoader existingViewHashLoader;


  /**
   * Injection constructor.
   */
  ExistingViewStateLoader(SqlDialect dialect, ExistingViewHashLoader existingViewHashLoader) {
    super();
    this.dialect = dialect;
    this.existingViewHashLoader = existingViewHashLoader;
  }

  /**
   * Loads the set of view changes which need to be made to match the target schema.
   *
   * @param sourceSchema the existing schema.
   * @param targetSchema the target schema.
   * @return the views which should not be present, and the views which need deploying.
   */
  public Result viewChanges(Schema sourceSchema, Schema targetSchema) {

    // Default to dropping all the views in the source schema unless we decide otherwise
    Map<String, View> viewsToDrop = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    for (View view : sourceSchema.views()) {
      viewsToDrop.put(view.getName().toUpperCase(), view);
    }

    // And creating all the ones in the target schema.
    Set<View> viewsToDeploy = new HashSet<>(targetSchema.views());

    // Work out if existing views which are deployed are OK, because we really
    // don't want to refresh the views on every startup.
    Optional<Map<String, String>> deployedViews = existingViewHashLoader.loadViewHashes(sourceSchema);
    if (deployedViews.isPresent()) {
      for (View view : targetSchema.views()) {
        String targetViewName = view.getName().toUpperCase();
        if (viewsToDrop.containsKey(targetViewName)) {
          String existingHash = deployedViews.get().get(targetViewName);
          String newHash = dialect.convertStatementToHash(view.getSelectStatement());
          if (existingHash == null) {
            log.info(String.format("View [%s] exists but hash not present in %s", targetViewName, DEPLOYED_VIEWS_NAME));
          } else if (newHash.equals(existingHash)) {
            // All good - leave it in place.
            viewsToDrop.remove(targetViewName);
            viewsToDeploy.remove(view);
          } else {
            log.info(
              String.format(
                "View [%s] exists in %s, but hash [%s] does not match target schema [%s]",
                targetViewName,
                DEPLOYED_VIEWS_NAME,
                existingHash,
                newHash
              )
            );
          }
        } else {
          if (deployedViews.get().containsKey(targetViewName)) {
            log.info(String.format("View [%s] is missing, but %s entry exists. The view may have been deleted.", targetViewName, DEPLOYED_VIEWS_NAME));
            viewsToDrop.put(targetViewName, view);
          } else {
            log.info(String.format("View [%s] is missing", targetViewName));
          }
        }
      }
    }

    return new Result(viewsToDrop.values(), viewsToDeploy);
  }


  /**
   * The result from a call to {@link ExistingViewStateLoader#viewChanges(Schema, Schema)}.
   *
   * @author Copyright (c) Alfa Financial Software 2014
   */
  public static final class Result {

    private final Collection<View> viewsToDrop;
    private final Collection<View> viewsToDeploy;


    /**
     * Private constructor.
     */
    private Result(Collection<View> viewsToDrop, Collection<View> viewsToDeploy) {
      super();
      this.viewsToDrop = viewsToDrop;
      this.viewsToDeploy = viewsToDeploy;
    }

    /**
     * @return the views which need to be dropped.
     */
    public Collection<View> getViewsToDrop() {
      return viewsToDrop;
    }

    /**
     * @return the views which need to be deployed.
     */
    public Collection<View> getViewsToDeploy() {
      return viewsToDeploy;
    }

    /**
     * @return true if there are no views to deploy or drop.
     */
    public boolean isEmpty() {
      return viewsToDrop.isEmpty() && viewsToDeploy.isEmpty();
    }


    @Override
    public String toString() {
      return this.getClass().getSimpleName()
          + "; drop " + viewsToDrop
          + "; add " + viewsToDeploy;
    }
  }
}
