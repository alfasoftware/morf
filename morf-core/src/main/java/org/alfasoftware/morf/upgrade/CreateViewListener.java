package org.alfasoftware.morf.upgrade;

import com.google.common.collect.ImmutableList;
import com.google.inject.ImplementedBy;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.View;

/**
 *
 * Listener for calls to {@link ViewChangesDeploymentHelper#createView(View, UpgradeSchemas)}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
@ImplementedBy(CreateViewListener.NoOp.class)
public interface CreateViewListener {

  /**
   * Called during {@link ViewChangesDeploymentHelper#createView(View, UpgradeSchemas)}.
   *
   * @param view View being created.
   * @param upgradeSchemas source and target schemas for the upgrade.
   * @return Should return statements to be part of view creation, after the view has been created.
   */
  Iterable<String> registerView(View view, UpgradeSchemas upgradeSchemas);

  /**
   * Called during {@link ViewChangesDeploymentHelper#createView(View)}.
   *
   * @param view View being created.
   * @return Should return statements to be part of view creation, after the view has been created.
   * @deprecated Deprecated to ensure backwards compatibility.
   */
  @Deprecated
  default Iterable<String> registerView(View view) {
    return registerView(view, new UpgradeSchemas());
  }

  /**
   * Empty implementation.
   */
  class NoOp implements CreateViewListener {

    @Override
    public Iterable<String> registerView(View view, UpgradeSchemas upgradeSchemas) {
      return ImmutableList.of();
    }

    @Override
    public Iterable<String> registerView(View view) {
      return ImmutableList.of();
    }
  }

  /**
   * Factory that could be used to create {@link CreateViewListener}s.
   *
   * @author Copyright (c) Alfa Financial Software 2022
   */

  @ImplementedBy(Factory.NoOpFactory.class)
  interface Factory  {

    /**
     * Creates a {@link CreateViewListener} implementation for the given connection details.
     * @param connectionResources the ConnectionResources for the data source.
     * @return CreateViewListener.
     */
    CreateViewListener createCreateViewListener(ConnectionResources connectionResources);

    /**
     * NoOp factory implementation.
     */
    class NoOpFactory implements CreateViewListener.Factory {

      @Override
      public CreateViewListener createCreateViewListener(ConnectionResources connectionResources) {
        return new NoOp();
      }
    }
  }
}
