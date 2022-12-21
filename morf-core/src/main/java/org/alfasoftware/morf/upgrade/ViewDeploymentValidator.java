package org.alfasoftware.morf.upgrade;

import com.google.inject.ImplementedBy;
import org.alfasoftware.morf.jdbc.ConnectionResources;
import org.alfasoftware.morf.metadata.View;

/**
 * External view validator for {@link ExistingViewStateLoader#viewChanges(org.alfasoftware.morf.metadata.Schema, org.alfasoftware.morf.metadata.Schema)}.
 *
 * @author Copyright (c) Alfa Financial Software Limited. 2021
 */
@ImplementedBy(ViewDeploymentValidator.AlwaysValidate.class)
public interface ViewDeploymentValidator {

  /**
   * Called during {@link ExistingViewStateLoader#viewChanges(org.alfasoftware.morf.metadata.Schema, org.alfasoftware.morf.metadata.Schema)},
   * for each view that exists and appears to be valid. Validators should return true to further validate the view from their point of view.
   * If fully validated, the view will be left in it's current state, otherwise, if a validator responds with false, any old view records will
   * first need to be de-registered, to clear any potential remnants, and only then the new view will be created.
   *
   * @param view View being examined. This is an existing schema view.
   * @return This method should return true, if the view is validated and does not need updating.
   *         This method can return false to trigger full view re-definition.
   */
  public boolean validateExistingView(View view);

  /**
   * Called during {@link ExistingViewStateLoader#viewChanges(org.alfasoftware.morf.metadata.Schema, org.alfasoftware.morf.metadata.Schema)},
   * for each view that does not yet exist in the database, and is to be created. Validators should return true to acknowledge they also do not
   * yet recognise the view. If acknowledged, the view will be created, otherwise, if a validator responds with false, any old view records will
   * first need to be de-registered, to clear any potential remnants, and only then the new view will be created.
   *
   * @param view View being examined. This is a non-existing schema view.
   * @return This method should return true, if the view can simply be created.
   *         This method can return false to trigger full view re-definition.
   */
  public boolean validateMissingView(View view);


  /**
   * Empty implementation which always agrees with the given view.
   */
  class AlwaysValidate implements ViewDeploymentValidator {

    @Override
    public boolean validateExistingView(View view) {
      return true; // the given existing view is okay as is
    }

    @Override
    public boolean validateMissingView(View view) {
      return true; // the given new view can be created right away
    }
  }


  /**
   * Factory that could be used to create {@link ViewDeploymentValidator}s.
   *
   * @author Copyright (c) Alfa Financial Software 2022
   */
  @ImplementedBy(Factory.AlwaysValidateFactory.class)
  interface Factory  {

    /**
     * Creates a {@link ViewDeploymentValidator} implementation for the given connection details.
     */
    ViewDeploymentValidator create(ConnectionResources connectionResources);

    /**
     * Factory implementation always returning a {@link AlwaysValidate} implementation.
     */
    class AlwaysValidateFactory implements ViewDeploymentValidator.Factory {

      @Override
      public ViewDeploymentValidator create(ConnectionResources connectionResources) {
        return new AlwaysValidate();
      }
    }
  }
}
