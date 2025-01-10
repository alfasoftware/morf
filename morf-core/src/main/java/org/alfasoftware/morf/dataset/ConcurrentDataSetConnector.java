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
import java.util.function.Supplier;


/**
 * Transmits all data from a {@link DataSetProducer} to a {@link DataSetConsumer}. This
 * class acts as a bridge between the producer interface which is fundamentally a data
 * pull operation and the consumer interface which is fundamentally a data push.
 *
 * <p>
 * This version of the connector works using an {@link ExecutorService} to provide
 * work management and concurrency.
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software 2025
 */

public class ConcurrentDataSetConnector {

  /**
   * Number of threads in the executor pool.
   */
  private final int threadCount;

  /**
   * The supplier for producers from which all data will be retrieved and pushed to the {@link #consumerSupplier}.
   */
  protected final Supplier<DataSetProducer> producerSupplier;

  /**
   * The supplier for  consumers for all data retrieved from the {@link #producerSupplier}.
   */
  protected final Supplier<DataSetConsumer> consumerSupplier;


  /**
   * Creates a new instance of this class.
   *
   * @param producerSupplier The supplier for data to transmit.
   * @param consumerSupplier The supplier for the target to which the data should be sent.
   */
  public ConcurrentDataSetConnector(Supplier<DataSetProducer> producerSupplier, Supplier<DataSetConsumer> consumerSupplier) {
    super();
    this.producerSupplier = producerSupplier;
    this.consumerSupplier = consumerSupplier;
    this.threadCount = calculateDefaultThreadCount();
  }


  /**
   * Creates a new instance of this class.
   *
   * @param producerSupplier The supplier for data to transmit.
   * @param consumerSupplier The supplier for the target to which the data should be sent.
   * @param threadCount Uses provided number of threads instead of default
   */
  public ConcurrentDataSetConnector(Supplier<DataSetProducer> producerSupplier, Supplier<DataSetConsumer> consumerSupplier, int threadCount) {
    super();
    this.producerSupplier = producerSupplier;
    this.consumerSupplier = consumerSupplier;
    this.threadCount = threadCount;
  }


  /**
   * Calculates the number of threads to use
   */
  private int calculateDefaultThreadCount() {
    int processorCount = Runtime.getRuntime().availableProcessors();

    switch(processorCount) {
      case 0:
        throw new IllegalStateException("Could not find at least 1 processor");
      case 1:
        return 1;
      default:
        //This is consistent with Cryo calculation
        return Math.max(1,  Math.min(8, processorCount/2));
    }
  }


  /**
   * Transmits all data from the producer to the consumer.
   */
  public void connect() {
    DataSetConsumer.CloseState closeState = DataSetConsumer.CloseState.INCOMPLETE;
    ExecutorService executor = Executors.newFixedThreadPool(threadCount);

    DataSetProducer producerMetadata =  producerSupplier.get();
    DataSetConsumer consumerMetadata = consumerSupplier.get();
    try {
      for (String tableName : producerMetadata.getSchema().tableNames()) {
        DataSetConsumer dataSetConsumer = consumerSupplier.get();
        DataSetProducer dataSetProducer = producerSupplier.get();
        try {
          dataSetProducer.open();
          dataSetConsumer.open();
          executor.execute(new DataSetConnectorRunnable(dataSetProducer, dataSetConsumer, tableName));
        } catch (Exception e) {
          executor.shutdownNow();
          throw new RuntimeException("Error connecting table [" + tableName + "]", e);
        }
        finally {
          if(dataSetConsumer != null) dataSetConsumer.close(DataSetConsumer.CloseState.COMPLETE);
          //TODO - do not close producer as it is reused ??
        }
      }
      executor.shutdown();
      executor.awaitTermination(60, TimeUnit.MINUTES);

      // once we've read all the tables without exception, we're complete and ready to release all resources
      closeState = DataSetConsumer.CloseState.FINALLY_COMPLETE;

    }
    catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }
    finally {
      executor.shutdownNow();
      if(consumerMetadata != null) consumerMetadata.close(closeState);
      if(producerMetadata != null) producerMetadata.close();
    }
  }


  /**
   * Runnable for use in {@link ExecutorService} instances. Connects one table.
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
    private DataSetConnectorRunnable(DataSetProducer producer, DataSetConsumer consumer, String tableName) {
      this.producer = producer;
      this.consumer = consumer;
      this.tableName = tableName;
    }

    /**
     * Transfers the table with the name given in the constructor from the producer to
     * the consumer.
     */
    @Override
    public void run() {
      consumer.table(producer.getSchema().getTable(tableName), producer.records(tableName));
    }
  }
}