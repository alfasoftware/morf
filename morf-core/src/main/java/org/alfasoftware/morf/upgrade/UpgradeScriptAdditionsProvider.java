package org.alfasoftware.morf.upgrade;


import com.google.inject.ImplementedBy;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;

import java.lang.annotation.Annotation;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@ImplementedBy(UpgradeScriptAdditionsProvider.FilteredScriptAdditions.class)
public interface UpgradeScriptAdditionsProvider {

    default Set<UpgradeScriptAddition> getUpgradeScriptAdditions() {
        return Collections.emptySet(); //NoOp
    }

    default  void excludeAnnotatedBy(List<Class<? extends Annotation>> annotations) {
        //NoOp;
    }


    /**
     * Always provides an empty set.
     */
    class NoOpScriptAdditions implements UpgradeScriptAdditionsProvider {}


    /**
     * Provides a filtered set of script addition implementations.
     * Filtering is based on exclusion annotations.
     */
    @Singleton
    class FilteredScriptAdditions implements UpgradeScriptAdditionsProvider {

        private final Set<UpgradeScriptAddition> upgradeScriptAdditions;

        private List<Class<? extends Annotation>> excludeAnnotations = new ArrayList<>();

        @Inject
        public FilteredScriptAdditions(Set<UpgradeScriptAddition> upgradeScriptAdditions) {
            this.upgradeScriptAdditions = upgradeScriptAdditions;
        }

        @Override
        public Set<UpgradeScriptAddition> getUpgradeScriptAdditions(){
            return upgradeScriptAdditions
                    .stream()
                    .filter(s -> !isExcluded(s.getClass()))
                    .collect(Collectors.toSet());
        }

        @Override
        public void excludeAnnotatedBy(List<Class<? extends Annotation>> annotations) {
            excludeAnnotations = annotations;
        }


        private boolean isExcluded(Class additionClass) {
            return excludeAnnotations.stream().anyMatch(additionClass::isAnnotationPresent);
        }
    }
}
