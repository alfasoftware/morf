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

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import org.alfasoftware.morf.metadata.Sequence;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Lists.newLinkedList;
import static com.google.common.collect.Maps.uniqueIndex;
import static com.google.common.collect.Sets.newHashSet;

/**
 * SequenceChanges organises the lists of sequences to drop and deploy.
 *
 * <p>Callers pass the complete set of sequences together with the sequences they wish to drop and deploy.</p>
 *
 * @author Copyright (c) Alfa Financial Software Ltd. 2024
 */
public class SequenceChanges {

  /**
   * Logger.
   */
  private static final Log log = LogFactory.getLog(SequenceChanges.class);

  /**
   * All sequences that are known from the source code - i.e. our ultimate source of dependency information.
   */
  private final Collection<Sequence> allSequences;

  /**
   * A map representation of allSequences in with the sequence name lowercased as the key and the original text as
   * the value
   */
  private final Map<String, String> allSequencesMap;

  /**
   * Set of names of sequences to drop.
   */
  private final Set<String> dropSet;

  /**
   * Set of names of sequences to deploy.
   */
  private final Set<String> deploySet;

  /**
   * Set of names of sequences that are known (i.e. the sequences declared in the source - nothing obsolete found in the DB.
   */
  private final Set<String> knownSet;

  /**
   * Map sequences names to sequences so we can convert back to Sequence collections - includes knownSet together with
   * obsolete sequences.
   */
  private final Map<String, Sequence> sequenceIndex = new HashMap<>();

  /**
   * The topological sort of names based on dependencies.
   */
  private final List<String> sequenceCreationOrder;


  /**
   * Construct a description of sequence changes based on the supplied drop and deploy requirements
   * together with some dependency analysis.
   *
   * <p>The drop set is expanded based on dependency analysis: any sequence that depends on a dropped
   * sequence will also be dropped.</p>
   *
   * <p>The deployment set is expanded only to include the items that are added to the drop set -
   * i.e. if we determine that a sequence needs to be recreated because a dependencies is dropped,
   * then it will be added to both the drop and deploy sets. Sequences will not be added to the
   * deploy set based on dependency analysis - it is assumed that all missing sequences will
   * already be in the deploy set.</p>
   *
   * <p>All parameters may be immutable.</p>
   *
   * @param allSequences all the sequences as defined in the codeset and bound in to the application. This is the
   *                     'source of truth' as to which sequences are available.
   * @param sequencesToDrop Sequences which should be dropped from the current schema. Caller must ensure names are
   *                        unique within the collection.
   * @param sequencesToDeploy Sequences which should be deployed from the target schema. Caller must ensure names are
   *                          unique within the collection.
   */
  public SequenceChanges(Collection<Sequence> allSequences, Collection<Sequence> sequencesToDrop,
                         Collection<Sequence> sequencesToDeploy) {
    super();

    // -- Store our dependency information as we may need to pass it on...
    //
    this.allSequences = allSequences;
    this.allSequencesMap = allSequences.stream().collect(Collectors.toMap(sequence -> sequence.getName().toLowerCase(), Sequence::getName, (first, second) -> first));

    // -- Work with sets of strings internally, we switch back to sequences when we return ordered lists...
    //
    this.knownSet = newHashSet(Collections2.transform(allSequences, sequenceToName()));
    this.dropSet = newHashSet(correctCase(Collections2.transform(sequencesToDrop, sequenceToName())));
    this.deploySet = newHashSet(Collections2.transform(sequencesToDeploy, sequenceToName()));

    // -- Maintain an index of name to sequence so we can convert back. Must contain allSequences + sequencesToDrop for obsolete sequences...
    //
    sequenceIndex.putAll(uniqueIndex(sequencesToDrop, sequenceToName()));
    sequenceIndex.putAll(uniqueIndex(allSequences, sequenceToName())); // known sequences take precedence as we have more
    // complete info

    // -- Perform a topological sort of all dependent sequences (based on bound in sequences, not the contents of the DB)...
    //
    sequenceCreationOrder = topoSortSequences(allSequences);

    // -- Add any obsolete sequences back into the order in an arbitrary position (they need to be dropped
    //    but we have no dependency information to indicate where)...
    //
    sequenceCreationOrder.addAll(Sets.difference(newHashSet(this.dropSet), newHashSet(this.knownSet)));
  }


  /**
   * @return sorted list of the sequences to drop in the order they should be dropped.
   */
  public List<Sequence> getSequencesToDrop() {
    // Sort the list in reverse order of creation for dropping
    List<String> sortedSequenceNamesToDrop = newArrayList(dropSet);
    Collections.sort(sortedSequenceNamesToDrop, Ordering.explicit(sequenceCreationOrder).reverse());

    // Transform the sorted list back into a set of sorted sequences and return
    return Lists.transform(sortedSequenceNamesToDrop, nameToSequence());
  }


  /**
   * @return sorted list of the sequences to deploy in the order they should be deployed.
   */
  public List<Sequence> getSequencesToDeploy() {
    // Sort the list into creation order based on toposort result
    List<String> sortedSequenceNamesToDeploy = newArrayList(deploySet);
    Collections.sort(sortedSequenceNamesToDeploy, Ordering.explicit(sequenceCreationOrder));

    // Transform the sorted list back into a set of sorted sequences and return
    return Lists.transform(sortedSequenceNamesToDeploy, nameToSequence());
  }


  /**
   * Correct case of names with respect to all the sequences we know about.
   *
   * @return equivalent collection with names case-corrected where possible.
   */
  private Collection<String> correctCase(Collection<String> names) {
    Map<String, String> namesMap = names.stream().collect(Collectors.toMap(String::toLowerCase, name -> name, (first, second) -> first));
    namesMap.replaceAll(allSequencesMap::getOrDefault);
    return namesMap.values();
  }


  /**
   * Performs a topological sort using a depth-first search algorithm and returns a sorted list of
   * database sequences for the schema upgrade.
   *
   * @param allSequences all of the database sequences bound into the target schema.
   * @return a {@link <a href="http://en.wikipedia.org/wiki/Topological_sorting">topologically sorted list</a>} of sequence names
   */
  private List<String> topoSortSequences(Collection<Sequence> allSequences) {
    if (log.isDebugEnabled()) {
      log.debug("Toposorting: " + Joiner.on(", ").join(Collections2.transform(allSequences, sequenceToName())));
    }

    // The set of sequences we want to perform the sort on.
    Set<String> unmarkedSequences = newHashSet(Collections2.transform(allSequences, sequenceToName()));
    Set<String> temporarilyMarkedRecords = newHashSet();
    List<String> sortedList = newLinkedList();

    while (!unmarkedSequences.isEmpty()) {
      String node = Iterables.getFirst(unmarkedSequences, null);
      visit(node, temporarilyMarkedRecords, sortedList);
      unmarkedSequences.remove(node);
    }

    return sortedList;
  }


  /**
   * @return the name of a given sequence.
   */
  private Function<Sequence, String> sequenceToName() {
    return new Function<Sequence, String>() {
      @Override
      public String apply(Sequence sequence) {
        return sequence.getName();
      }
    };
  }


  /**
   * @return the sequence for a given name.
   */
  private Function<String, Sequence> nameToSequence() {
    return new Function<String, Sequence>() {
      @Override
      public Sequence apply(String name) {
        return sequenceIndex.get(name);
      }
    };
  }


  /**
   * Visit the selected node for the topological sort. If it has been marked start working
   * back up the list. Otherwise, mark it and then try visiting all of its dependent nodes.
   *
   * @param node the node being visited.
   * @param sortedList the list of sorted results. Items in this list are 'permanently' marked e.g. node is sorted.
   * @param temporarilyMarkedRecords a set of nodes we've already visited. Items in this list are 'temporarily' marked e.g. node is visited.
   */
  private void visit(String node, Set<String> temporarilyMarkedRecords, List<String> sortedList) {
    if (log.isDebugEnabled()) {
      log.debug("Visiting node: " + node);
    }

    // Check if we have hit a temporary mark. We should not have done this as we cannot sort collections which contain circular dependencies.
    // we can only sort trees and Directed Acyclic Graphs.
    if (temporarilyMarkedRecords.contains(node)) {
      throw new IllegalStateException("Sequences requested have a circular dependency.");
    }

    // If the node isn't marked at all. Mark it
    if (sortedList.contains(node)) {
      return;
    }

    temporarilyMarkedRecords.add(node);

    sortedList.add(node); // Permanently mark the node as sorted
    temporarilyMarkedRecords.remove(node); // remove temporary mark
  }
}
