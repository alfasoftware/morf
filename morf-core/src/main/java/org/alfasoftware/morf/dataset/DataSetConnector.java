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

import org.alfasoftware.morf.dataset.DataSetConsumer.CloseState;

/**
 * Transmits all data from a {@link DataSetProducer} to a {@link DataSetConsumer}. This
 * class acts as a bridge between the producer interface which is fundamentally a data
 * pull operation and the consumer interface which is fundamentally a data push.
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class DataSetConnector {

  /**
   * The producer from which all data will be retrieved and pushed to the {@link #consumer}.
   */
  protected final DataSetProducer producer;

  /**
   * The consumer for all data retrieved from the {@link #producer}.
   */
  protected final DataSetConsumer consumer;


  /**
   * @param producer The data to transmit.
   * @param consumer The target to which the data should be sent.
   */
  public DataSetConnector(DataSetProducer producer, DataSetConsumer consumer) {
    super();
    this.producer = producer;
    this.consumer = consumer;
  }


  /**
   * Transmits all data from the producer to the consumer.
   */
  public void connect() {
    CloseState closeState = CloseState.INCOMPLETE;

    consumer.open();
    try {
      producer.open();
      try {
        for (String tableName : producer.getSchema().tableNames()) {
          try {
            consumer.table(producer.getSchema().getTable(tableName), producer.records(tableName));
          } catch (Exception e) {
            throw new RuntimeException("Error connecting table [" + tableName + "]", e);
          }
        }
        // once we've read all the tables without exception, we're complete
        closeState = CloseState.COMPLETE;
      } finally {
        producer.close();
      }
    } finally {
      consumer.close(closeState);
    }
  }
}
