package org.alfasoftware.morf.upgrade;

import com.google.common.collect.ImmutableList;
import com.google.inject.ImplementedBy;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.View;

/**
 *
 * Listener for calls to {@link ViewChangesDeploymentHelper#dropViewIfExists(View, boolean, UpgradeSchemas)}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
@ImplementedBy(DropViewListener.NoOp.class)
public interface DropViewListener {

  /**
   * Called during {@link ViewChangesDeploymentHelper#dropViewIfExists(View, boolean, UpgradeSchemas)}.
   *
   * @param view View being de-registered.
   * @param upgradeSchemas source and target schemas for the upgrade.
   * @return Should return statements to be part of view removal, after the view is de-registered.
   */

  Iterable<String> deregisterView(View view, UpgradeSchemas upgradeSchemas);


  /**
   * Called during {@link ViewChangesDeploymentHelper#dropViewIfExists(View, boolean, UpgradeSchemas)}.
   *
   * @param upgradeSchemas source and target schemas for the upgrade.
   * @return Should return statements to be part of view removal, after the view is de-registered.
   */
  Iterable<String> deregisterAllViews(UpgradeSchemas upgradeSchemas);

  /**
   * Called during {@link ViewChangesDeploymentHelper#dropViewIfExists(View)}.
   *
   * @param view View being de-registered.
   * @return Should return statements to be part of view removal, after the view is de-registered.
   */
  @Deprecated
  default Iterable<String> deregisterView(View view){
    return deregisterView(view, new UpgradeSchemas());
  };


  /**
   * Called during {@link ViewChangesDeploymentHelper#deregisterAllViews()}.
   *
   * @return Should return statements to be part of all views removal.
   */
  @Deprecated
  default Iterable<String> deregisterAllViews(){
    return deregisterAllViews(new UpgradeSchemas());
  }

  /**
   * Empty implementation.
   */
  class NoOp implements DropViewListener {

    @Override
    public Iterable<String> deregisterView(View view, UpgradeSchemas upgradeSchemas) {
      return ImmutableList.of();
    }

    @Override
    public Iterable<String> deregisterAllViews(UpgradeSchemas upgradeSchemas) {
      return ImmutableList.of();
    }

    @Override
    public Iterable<String> deregisterView(View view) {
      return ImmutableList.of();
    }

    @Override
    public Iterable<String> deregisterAllViews() {
      return ImmutableList.of();
    }
  }


  /**
   * Factory that could be used to create {@link DropViewListener}s.
   *
   * @author Copyright (c) Alfa Financial Software 2022
   */
  @ImplementedBy(Factory.NoOpFactory.class)
  interface Factory  {

    /**
     * Creates a {@link DropViewListener} implementation for the given connection details.
     * @param connectionResources the ConnectionResources for the data source.
     * @return DropViewListener.
     */
    DropViewListener createDropViewListener(ConnectionResources connectionResources);

    /**
     * NoOp factory implementation.
     */
    class NoOpFactory implements DropViewListener.Factory {

      @Override
      public DropViewListener createDropViewListener(ConnectionResources connectionResources) {
        return new NoOp();
      }
    }
  }
}
