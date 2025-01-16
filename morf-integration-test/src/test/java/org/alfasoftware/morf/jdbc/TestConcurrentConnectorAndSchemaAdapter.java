package org.alfasoftware.morf.jdbc;

import static com.google.common.io.Resources.getResource;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import org.alfasoftware.morf.dataset.ConcurrentDataSetConnector;
import org.alfasoftware.morf.dataset.DataSetHomology;
import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.xml.XmlDataSetProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import net.jcip.annotations.NotThreadSafe;


/**
 * Verifies that concurrent connector and schema adapter work together
 * to produce a correct result.
 *
 * @author Copyright (c) Alfa Financial Software 2025
 */
@NotThreadSafe
public class TestConcurrentConnectorAndSchemaAdapter {

  private static final Log log = LogFactory.getLog(TestConcurrentConnectorAndSchemaAdapter.class);

  ConnectionResourcesBean connectionResources;

  @Rule
  public TestRule synchronisation = (base, description) -> new Statement() {

    @Override
    public void evaluate() throws Throwable {
      synchronized (InjectMembersRule.class) {
        base.evaluate();
      }
    }
  };

  @Before
  public void setup() {
    connectionResources = new ConnectionResourcesBean(getResource("morf.properties"));
  }


  /**
   * Verifies that concurrent extract provides correct result at the end.
   */
  @Test
  public void testExtract() throws IOException, URISyntaxException {
    URL sourceExtract = getResource("org/alfasoftware/morf/dataset/concurrentConnectorAndSchemaAdapter/sourceDataset1/").toURI().toURL();

    SqlScriptExecutorProvider sqlScriptExecutorProvider = new SqlScriptExecutorProvider(connectionResources.getDataSource(), connectionResources.sqlDialect());

    log.info("Creating concurrent connector");
    XmlDataSetProducer xmlDataSetProducer = new XmlDataSetProducer(sourceExtract);
    int threadCount = 4; //use a fixed number of threads
     new ConcurrentDataSetConnector(
      xmlDataSetProducer,
      () -> new ConcurrentSchemaModificationAdapter(new DatabaseDataSetConsumer(connectionResources, sqlScriptExecutorProvider)), threadCount).connect();

    // ... the resulting dataset matches the source one
    assertTrue("Resulting dataSet does not match the source", new DataSetHomology().dataSetProducersMatch(new DatabaseDataSetProducer(connectionResources), new XmlDataSetProducer(sourceExtract)));
  }
}
