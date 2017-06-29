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

import org.alfasoftware.morf.metadata.Table;

/**
 * Simple implementation of {@link DataSetConsumer} that delegates to a destination consumer.
 *
 * <p>This class is designed to be sub classed in order to modify the event stream to a
 * data set consumer.</P>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public abstract class DataSetAdapter implements DataSetConsumer {

  /**
   * The consumer to delegate to.
   */
  protected final DataSetConsumer consumer;


  /**
   * Creates an adapter that will delegate all calls to the specified consumer.
   *
   * @param consumer The target consumer to delegate to.
   */
  public DataSetAdapter(DataSetConsumer consumer) {
    super();
    this.consumer = consumer;
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#close(org.alfasoftware.morf.dataset.DataSetConsumer.CloseState)
   */
  @Override
  public void close(CloseState closeState) {
    consumer.close(closeState);
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#open()
   */
  @Override
  public void open() {
    consumer.open();
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#table(org.alfasoftware.morf.metadata.Table, java.lang.Iterable)
   */
  @Override
  public void table(Table table, Iterable<Record> records) {
    consumer.table(table, records);
  }


}
