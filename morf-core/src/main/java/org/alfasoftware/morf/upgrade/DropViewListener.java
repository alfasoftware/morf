package org.alfasoftware.morf.upgrade;

import org.alfasoftware.morf.metadata.View;

import com.google.common.collect.ImmutableList;
import com.google.inject.ImplementedBy;

/**
 *
 * Listener for calls to {@link ViewChangesDeploymentHelper#dropView(View)}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
@ImplementedBy(DropViewListener.NoOp.class)
public interface DropViewListener {

  /**
   * Called during {@link ViewChangesDeploymentHelper#dropView(View)}.
   *
   * @param view View being dropped.
   * @return Should return statements to be part of view removal.
   */
  public Iterable<String> dropView(View view);


  class NoOp implements DropViewListener {

    @Override
    public Iterable<String> dropView(View view) {
      return ImmutableList.of();
    }
  }
}
