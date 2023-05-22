package org.alfasoftware.morf.upgrade;


import com.google.inject.ImplementedBy;
import com.google.inject.Inject;
import org.alfasoftware.morf.upgrade.additions.UpgradeScriptAddition;

import java.util.Collections;
import java.util.Set;
import java.util.function.Predicate;

@ImplementedBy(UpgradeScriptAdditionsProvider.DefaultScriptAdditions.class)
public interface UpgradeScriptAdditionsProvider {


    /**
     * Returns all script additions with the filtering criteria applied.
     * The filtering logic should be provided by calling {@link #setAllowedPredicate(Predicate)} first.
     * @return
     */
    default Set<UpgradeScriptAddition> getUpgradeScriptAdditions() {
        return Collections.emptySet();
    }


    /**
     * Allows for filtering of script additions.
     * @param scriptAdditionsPredicate
     */
    default void setAllowedPredicate(Predicate<UpgradeScriptAddition> scriptAdditionsPredicate) {
    }


    /**
     * Always provides an empty set.
     */
    class NoOpScriptAdditions implements UpgradeScriptAdditionsProvider {}


    /**
     * Provides a default set of script addition implementations.
     */
    class DefaultScriptAdditions implements UpgradeScriptAdditionsProvider {

        private final Set<UpgradeScriptAddition> upgradeScriptAdditions;

        @Inject
        public DefaultScriptAdditions(Set<UpgradeScriptAddition> upgradeScriptAdditions) {
            this.upgradeScriptAdditions = upgradeScriptAdditions;
        }

        @Override
        public Set<UpgradeScriptAddition> getUpgradeScriptAdditions() {
            return upgradeScriptAdditions;
        }
    }
}