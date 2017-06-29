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

package org.alfasoftware.morf.dataset;

import org.alfasoftware.morf.metadata.Schema;

/**
 * Allows decoration of a {@link DataSetProducer}. Override methods as required.
 *
 * @author Copyright (c) Alfa Financial Software 2012
 */
public abstract class DataSetProducerAdapter implements DataSetProducer {

  /**
   * The producer to delegate to.
   */
  protected final DataSetProducer delegate;

  /**
   * @param delegate The {@link DataSetProducer} to delegate calls to.
   */
  protected DataSetProducerAdapter(DataSetProducer delegate) {
    super();
    this.delegate = delegate;
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#open()
   */
  @Override
  public void open() {
    delegate.open();
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#close()
   */
  @Override
  public void close() {
    delegate.close();
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#getSchema()
   */
  @Override
  public Schema getSchema() {
    return delegate.getSchema();
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#records(java.lang.String)
   */
  @Override
  public Iterable<Record> records(String tableName) {
    return delegate.records(tableName);
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#isTableEmpty(java.lang.String)
   */
  @Override
  public boolean isTableEmpty(String tableName) {
    return delegate.isTableEmpty(tableName);
  }
}
