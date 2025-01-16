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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.alfasoftware.morf.dataset.DataSetConsumer.CloseState;

/**
 * Transmits all data from a {@link DataSetProducer} to a {@link DataSetConsumer}. This
 * class acts as a bridge between the producer interface which is fundamentally a data
 * pull operation and the consumer interface which is fundamentally a data push.
 *
 * <p>
 * This version of the connector works using an {@link ExecutorService} to provide
 * work management and concurrency.
 * </p>
 * <p>
 * @deprecated
 * This class is deprecated due to incorrect reuse of the same connection between threads,
 * use {@link ConcurrentDataSetConnector} instead
 * </p>
 * @author Copyright (c) Alfa Financial Software 2011
 */
@Deprecated
public class DataSetConnectorMultiThreaded {

  /**
   * Number of threads in the executor pool.
   */
  private final int threadCount;

  /**
   * The producer from which all data will be retrieved and pushed to the {@link #consumer}.
   */
  protected final DataSetProducer producer;

  /**
   * The consumer for all data retrieved from the {@link #producer}.
   */
  protected final DataSetConsumer consumer;


  /**
   * Creates a new instance of this class.
   *
   * @param producer The data to transmit.
   * @param consumer The target to which the data should be sent.
   */
  public DataSetConnectorMultiThreaded(DataSetProducer producer, DataSetConsumer consumer) {
    super();
    this.producer = producer;
    this.consumer = consumer;
    threadCount = calculateThreadCount();
  }


  /**
   * Calculates the number of threads to use
   *
   * @return The number of threads to use for dumping the database.
   */
  private int calculateThreadCount() {
    int processorCount = Runtime.getRuntime().availableProcessors();

    switch(processorCount) {
      case 0:
        throw new RuntimeException("Could not find at least 1 processor");
      case 1:
        return 1;
      case 2:
        return 2;
      default:
        return 8;
    }
  }


  /**
   * Transmits all data from the producer to the consumer.
   */
  public void connect() {
    CloseState closeState = CloseState.INCOMPLETE;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);
    try {
      consumer.open();
      producer.open();
      try {
        for (String tableName : producer.getSchema().tableNames()) {
          try {
            executor.execute(new DataSetConnectorRunnable(producer, consumer, tableName));
          } catch (Exception e) {
            executor.shutdownNow();
            throw new RuntimeException("Error connecting table [" + tableName + "]", e);
          }
        }
        executor.shutdown();
        executor.awaitTermination(60, TimeUnit.MINUTES);

        // once we've read all the tables without exception, we're complete
        closeState = CloseState.COMPLETE;
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        producer.close();
      }
    } finally {
      executor.shutdownNow();
      consumer.close(closeState);
    }
  }


  /**
   * Runnable for use in {@link ExecutorService} instances. "Cryo"-s one
   * table.
   *
   * @author Copyright (c) Alfa Financial Software 2011
   */
  private final class DataSetConnectorRunnable implements Runnable {

    /**
     * The source of the data to transmit.
     */
    private final DataSetProducer producer;

    /**
     * The target to which the data should be sent.
     */
    private final DataSetConsumer consumer;

    /**
     * Name of the table to transmit.
     */
    private final String tableName;

    /**
     * Create new instance of this class.
     *
     * @param producer The data to transmit.
     * @param consumer The target to which the data should be sent.
     * @param tableName The name of the table to transmit.
     */
    private DataSetConnectorRunnable(
        DataSetProducer producer,
        DataSetConsumer consumer,
        String tableName) {

      this.producer = producer;
      this.consumer = consumer;
      this.tableName = tableName;
    }

    /**
     * Cryo-s the table with the name given in the constructor from the producer to
     * the consumer.
     */
    @Override
    public void run() {
      consumer.table(producer.getSchema().getTable(tableName), producer.records(tableName));
    }
  }
}
