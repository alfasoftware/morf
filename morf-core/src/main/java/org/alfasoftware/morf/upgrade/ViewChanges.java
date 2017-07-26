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

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.Sets.newHashSet;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import org.alfasoftware.morf.metadata.View;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

/**
 * Encapsulate the changes needed to the views.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */

/**
 * ViewChanges organises the lists of views to drop and deploy.
 *
 * <p>Callers pass the complete set of views (used for dependency analysis)
 * together with the views they wish to drop and deploy. Dependencies are
 * analysed and the lists expanded and ordered according to the dependencies.</p>
 *
 * <p>Not that deployment set is expanded conservatively - we don't deploy all
 * dependencies of a view if it is to be deployed - it is assumed that it is
 * already in the deployment set as it is required and has been found to be absent.
 * The deployment set is only expanded to ensure that we re-deploy views once
 * we've added them to the drop set.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2013
 */
public class ViewChanges {

  /**
   * Logger.
   */
  private static final Log log = LogFactory.getLog(ViewChanges.class);

  /**
   * All views that are known from the source code - i.e. our ultimate source of dependency information.
   */
  private final Collection<View> allViews;

  /**
   * Set of names of views to drop.
   */
  private final Set<String> dropSet;

  /**
   * Set of names of views to deploy.
   */
  private final Set<String> deploySet;

  /**
   * Set of names of views that are known (i.e. the views declared in the source - nothing obsolete found in the DB.
   */
  private final Set<String> knownSet;

  /**
   * Map view names to views so we can convert back to View collections - includes knownSet together with obsolete views.
   */
  private final Map<String, View> viewIndex = new HashMap<>();

  /**
   * The topological sort of names based on dependencies.
   */
  private final List<String> viewCreationOrder;

  /**
   * Construct a description of view changes based on the supplied drop and deploy requirements
   * together with some dependency analysis.
   *
   * <p>The drop set is expanded based on dependency analysis: any view that depends on a dropped
   * view will also be dropped.</p>
   *
   * <p>The deployment set is expanded only to include the items that are added to the drop set -
   * i.e. if we determine that a view needs to be recreated because a dependencies is dropped,
   * then it will be added to both the drop and deploy sets. Views will not be added to the
   * deploy set based on dependency analysis - it is assumed that all missing views will
   * already be in the deploy set.</p>
   *
   * <p>All parameters may be immutable.</p>
   *
   * @param allViews all the views as defined in the codeset and bound in to the application. This is the 'source of truth' as to which views are available.
   * @param viewsToDrop Views which should be dropped from the current schema. Caller must ensure names are unique within the collection.
   * @param viewsToDeploy Views which should be deployed from the target schema. Caller must ensure names are unique within the collection.
   */
  public ViewChanges(Collection<View> allViews, Collection<View> viewsToDrop, Collection<View> viewsToDeploy) {
    super();

    // -- Store our dependency information as we may need to pass it on...
    //
    this.allViews = allViews;

    // -- Work with sets of strings internally, we switch back to views when we return ordered lists...
    //
    this.knownSet = newHashSet(Collections2.transform(allViews, viewToName()));
    this.dropSet = newHashSet(correctCase(Collections2.transform(viewsToDrop, viewToName())));
    this.deploySet = newHashSet(Collections2.transform(viewsToDeploy, viewToName()));

    // -- Maintain an index of name to view so we can convert back. Must contain allViews + viewsToDrop for obsolete views...
    //
    viewIndex.putAll(uniqueIndex(viewsToDrop, viewToName()));
    viewIndex.putAll(uniqueIndex(allViews, viewToName())); // known views take precedence as we have more complete info

    // -- Perform a topological sort of all dependent views (based on bound in views, not the contents of the DB)...
    //
    viewCreationOrder = topoSortViews(allViews, viewIndex);

    // -- Add any obsolete views back into the order in an arbitrary position (they need to be dropped
    //    but we have no dependency information to indicate where)...
    //
    viewCreationOrder.addAll(Sets.difference(newHashSet(this.dropSet), newHashSet(this.knownSet)));
  }


  /**
   * Allows us to create a new {@link ViewChanges} internally.
   */
  private ViewChanges(Collection<View> allViews, Set<String> dropSet, Set<String> deploySet, Map<String, View> index) {
    super();
    this.allViews = allViews;
    this.knownSet = newHashSet(Collections2.transform(allViews, viewToName()));
    this.dropSet = newHashSet(correctCase(dropSet));
    this.deploySet = newHashSet(deploySet);

    viewIndex.putAll(index);
    viewCreationOrder = topoSortViews(allViews, index);
    viewCreationOrder.addAll(Sets.difference(newHashSet(this.dropSet), newHashSet(this.knownSet)));
  }


  /**
   * @return sorted list of the views to drop in the order they should be dropped.
   */
  public List<View> getViewsToDrop() {
    // Sort the list in reverse order of creation for dropping
    List<String> sortedViewNamesToDrop = newArrayList(dropSet);
    Collections.sort(sortedViewNamesToDrop, Ordering.explicit(viewCreationOrder).reverse());

    // Transform the sorted list back into a set of sorted views and return
    return Lists.transform(sortedViewNamesToDrop, nameToView());
  }


  /**
   * @return sorted list of the views to deploy in the order they should be deployed.
   */
  public List<View> getViewsToDeploy() {
    // Sort the list into creation order based on toposort result
    List<String> sortedViewNamesToDeploy = newArrayList(deploySet);
    Collections.sort(sortedViewNamesToDeploy, Ordering.explicit(viewCreationOrder));

    // Transform the sorted list back into a set of sorted views and return
    return Lists.transform(sortedViewNamesToDeploy, nameToView());
  }


  /**
   * @return true if both sets are empty, false otherwise.
   */
  public boolean isEmpty() {
    return dropSet.isEmpty() && deploySet.isEmpty();
  }


  /**
   * @param extraViewsToDrop Additional views to drop
   * @return a new {@link ViewChanges} which also drops the specified views.
   */
  public ViewChanges droppingAlso(Collection<View> extraViewsToDrop) {
    Set<String> extraViewNames = ImmutableSet.copyOf(Collections2.transform(extraViewsToDrop, viewToName()));

    // -- In case we have any new obsolete views in here, we need to make sure they're in the index...
    //
    Map<String, View> newIndex = new HashMap<>();
    newIndex.putAll(uniqueIndex(extraViewsToDrop, viewToName()));
    newIndex.putAll(viewIndex);

    return new ViewChanges(allViews,
      Sets.union(dropSet, extraViewNames),
      Sets.union(deploySet, Sets.intersection(extraViewNames, knownSet)),
      newIndex);
  }


  /**
   * @param extraViewsToDeploy Additional views to deploy
   * @return a new {@link ViewChanges} which also deploys the specified views.
   */
  public ViewChanges deployingAlso(Collection<View> extraViewsToDeploy) {
    Set<String> extraViewNames = ImmutableSet.copyOf(Collections2.transform(extraViewsToDeploy, viewToName()));
    return new ViewChanges(allViews,
      dropSet,
      Sets.union(deploySet, extraViewNames),
      viewIndex);
  }


  /**
   * Correct case of names with respect to all the views we know about.
   *
   * @return equivalent collection with names case-corrected where possible.
   */
  private Collection<String> correctCase(Collection<String> names) {
    return newHashSet(Collections2.transform(names, new Function<String, String>() {
      @Override public String apply(String name) {
        for (View view: allViews) {
          if (view.getName().equalsIgnoreCase(name)) {
            return view.getName();
          }
        }
        return name;
      }
    }));
  }


  /**
   * Performs a topological sort using a depth-first search algorithm and returns a sorted list of
   * database views for the schema upgrade.
   *
   * @param allViews all of the database views bound into the target schema.
   * @param index a complete index of all views in both the source and target schema.
   * @return a topologically sorted list {@link http://en.wikipedia.org/wiki/Topological_sorting} of view names
   */
  private List<String> topoSortViews(Collection<View> allViews, Map<String, View> index) {
    if (log.isDebugEnabled()) {
      log.debug("Toposorting: " + Joiner.on(", ").join(Collections2.transform(allViews, viewToName())));
    }

    // The set of views we want to perform the sort on.
    Set<String> unmarkedViews = newHashSet(Collections2.transform(allViews, viewToName()));
    Set<String> temporarilyMarkedRecords = newHashSet();
    List<String> sortedList = newLinkedList();

    while (!unmarkedViews.isEmpty()) {
      String node = Iterables.getFirst(unmarkedViews, null);
      visit(node, temporarilyMarkedRecords, sortedList, index);
      unmarkedViews.remove(node);
    }

    return sortedList;
  }


  /**
   * Visit the selected node for the topological sort. If it has been marked start working
   * back up the list. Otherwise, mark it and then try visiting all of its dependent nodes.
   *
   * @param node the node being visited.
   * @param sortedList the list of sorted results. Items in this list are 'permanently' marked e.g. node is sorted.
   * @param temporarilyMarkedRecords a set of nodes we've already visited. Items in this list are 'temporarily' marked e.g. node is visited.
   */
  private void visit(String node, Set<String> temporarilyMarkedRecords, List<String> sortedList, Map<String, View> viewIndex) {
    if (log.isDebugEnabled()) {
      log.debug("Visiting node: " + node);
    }

    // Check if we have hit a temporary mark. We should not have done this as we cannot sort collections which contain circular dependencies.
    // we can only sort trees and Directed Acyclic Graphs.
    if (temporarilyMarkedRecords.contains(node)) {
      throw new IllegalStateException("Views requested have a circular dependency.");
    }

    // If the node isn't marked at all. Mark it
    if (sortedList.contains(node)) {
      return;
    }

    temporarilyMarkedRecords.add(node);

    for (String dependentView: viewIndex.get(node).getDependencies()) {

      visit(dependentView, temporarilyMarkedRecords, sortedList, viewIndex);

      if (dropSet.contains(dependentView)) {
        if (log.isDebugEnabled()) log.debug("Expanding views to drop to include " + node + " because it depends on " + dependentView);
        dropNode(node);
      }
    }

    sortedList.add(node); // Permanently mark the node as sorted
    temporarilyMarkedRecords.remove(node); // remove temporary mark
  }


  /**
   * Add a node to the drop set
   *
   * @param node the node to drop.
   */
  private void dropNode(String node) {
    dropSet.add(node);
    if (knownSet.contains(node)) {
      if (log.isDebugEnabled()) log.debug("Expanding views to deploy to include " + node + "because it is now dropped and exists in all views");
      deploySet.add(node);
    }
  }


  /**
   * @return the name of a given view.
   */
  private Function<View, String> viewToName() {
    return new Function<View, String>() {
      @Override
      public String apply(View view) {
        return view.getName();
      }
    };
  }


  /**
   * @return the view for a given name.
   */
  private Function<String, View> nameToView() {
    return new Function<String, View>() {
      @Override
      public View apply(String name) {
        return viewIndex.get(name);
      }
    };
  }
}