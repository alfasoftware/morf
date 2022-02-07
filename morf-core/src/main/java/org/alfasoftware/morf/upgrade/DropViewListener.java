package org.alfasoftware.morf.upgrade;

import org.alfasoftware.morf.metadata.View;

import com.google.common.collect.ImmutableList;
import com.google.inject.ImplementedBy;

/**
 *
 * Listener for calls to {@link ViewChangesDeploymentHelper#dropViewIfExists(View)}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
@ImplementedBy(DropViewListener.NoOp.class)
public interface DropViewListener {

  /**
   * Called during {@link ViewChangesDeploymentHelper#dropViewIfExists(View)}.
   *
   * @param view View being de-registered.
   * @return Should return statements to be part of view removal, after the view is de-registered.
   */
  public Iterable<String> deregisterView(View view);


  /**
   * Called during {@link ViewChangesDeploymentHelper#deregisterAllViews()}.
   *
   * @return Should return statements to be part of all views removal.
   */
  public Iterable<String> deregisterAllViews();


  /**
   * Empty implementation.
   */
  class NoOp implements DropViewListener {

    @Override
    public Iterable<String> deregisterView(View view) {
      return ImmutableList.of();
    }

    @Override
    public Iterable<String> deregisterAllViews() {
      return ImmutableList.of();
    }
  }
}
