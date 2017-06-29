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

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Represents the graph of upgrade steps ordered by prerequisite relations.
 *
 * <p>Tracks ignored steps and can provided a deterministic total ordering of
 * steps which preserves the partial ordering defined by the prerequisite
 * relation. This is used by {@link UpgradePathFinder}.</p>
 *
 * @author Copyright (c) Alfa Financial Software 2011
 */
public class UpgradeGraph {
  private static final Log log = LogFactory.getLog(UpgradeGraph.class);

  /**
   * An total ordering preserving the partial ordering in the graph.
   */
  private final SortedMap<Long, Class<? extends UpgradeStep>> orderedSteps = new TreeMap<>();


  /**
   * Construct a graph from the steps.
   *
   * @param steps upgrade steps.
   */
  public UpgradeGraph(Iterable<Class<? extends UpgradeStep>> steps) {
    super();

    List<String> errors = new LinkedList<>();

    for (Class<? extends UpgradeStep> stepClass : steps) {

      Sequence sequenceAnnotation = stepClass.getAnnotation(Sequence.class);

      if (sequenceAnnotation == null) {
        errors.add(stepClass + " does not have an @Sequence annotation");
        continue;
      }

      long sequence = sequenceAnnotation.value();

      Class<? extends UpgradeStep> displacedStepClass = orderedSteps.put(sequence, stepClass);

      if (displacedStepClass != null) {
        errors.add(String.format("%s and %s sh  are the same @Sequence annotation value of [%d]", stepClass, displacedStepClass, sequence));
      }


      //Check first if the @Version annotation is present and valid, otherwise check the version from the package name
      if (stepClass.isAnnotationPresent(Version.class)) {
        if (!isVersionAnnotationValid(stepClass)) {
          errors.add(String.format("%s has an invalid @Version annotation", stepClass));
        }
      } else if (!isPackageNameValid(stepClass)) {
        errors.add(String.format("%s does not have a valid @Version annotation and is not contained in a package named after the release version number it was added in", stepClass));
      }

    }

    if (!errors.isEmpty()) {
      throw new IllegalStateException("Cannot build upgrade graph:\n"+ StringUtils.join(errors, "\n"));
    }

    if (log.isDebugEnabled()) log.debug("Sorted upgrade steps [" + orderedSteps.values() + "]");
  }


  //Determine if the Version annotation value is valid
  private boolean isVersionAnnotationValid(Class<? extends UpgradeStep> stepClass) {
    return stepClass.getAnnotation(Version.class).value().matches("[0-9]+\\.[0-9]+(\\.[0-9]+[a-z]?)*$");
  }


  //Determine if the package name version is valid
  private boolean isPackageNameValid(Class<? extends UpgradeStep> stepClass) {
    return stepClass.getPackage().getName().matches(".*\\.upgrade\\.v[0-9]+_[0-9]+(_[0-9]+[a-z]?)*$");
  }


  /**
   * @return total ordering of upgrade steps.
   */
  public Collection<Class<? extends UpgradeStep>> orderedSteps() {
    return Collections.unmodifiableCollection(orderedSteps.values());
  }
}
