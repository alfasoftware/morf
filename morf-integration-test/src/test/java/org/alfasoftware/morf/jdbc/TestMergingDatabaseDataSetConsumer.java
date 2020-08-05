package org.alfasoftware.morf.jdbc;

import static com.google.common.io.Resources.getResource;
import static org.alfasoftware.morf.xml.MorfXmlDatasetMatchers.sameXmlFileAndLengths;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;

import net.jcip.annotations.NotThreadSafe;

import org.alfasoftware.morf.dataset.DataSetConnector;
import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.guicesupport.InjectMembersRule;
import org.alfasoftware.morf.xml.XmlDataSetConsumer;
import org.alfasoftware.morf.xml.XmlDataSetProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.rules.TestRule;
import org.junit.runners.model.Statement;

import com.google.inject.util.Providers;

/**
 * Verifies that two start position merge into
 */
@NotThreadSafe
public class TestMergingDatabaseDataSetConsumer {

  private static final Log log = LogFactory.getLog(TestMergingDatabaseDataSetConsumer.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  ConnectionResourcesBean connectionResources;

  @Rule
  public TestRule syncronisation = (base, description) -> new Statement() {

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


  private File getDatabaseAsFile() throws IOException {
    DatabaseDataSetProducer databaseDataSetProducer = new DatabaseDataSetProducer(connectionResources);
    File mergedExtractAsFile = temporaryFolder.newFile("merged-extract.zip");
    mergedExtractAsFile.createNewFile();

    XmlDataSetConsumer fileConsumer = new XmlDataSetConsumer(mergedExtractAsFile);
    new DataSetConnector(databaseDataSetProducer, fileConsumer).connect();

    return mergedExtractAsFile;
  }

  /**
   * Verifies that merging two extracts containing both overlapping and non-overlapping records results in having the overlapping records
   * overwrite the already-present ones and the non-overlapping to be inserted.
   */
  @Test
  public void testMergeTwoExtracts() throws IOException, URISyntaxException {
    URL controlExtract = getResource("org/alfasoftware/morf/dataset/mergingDatabaseDatasetConsumer/simple/controlDataset/").toURI().toURL();
    URL initialDataset = getResource("org/alfasoftware/morf/dataset/mergingDatabaseDatasetConsumer/simple/sourceDataset1/").toURI().toURL();
    URL datasetToMerge = getResource("org/alfasoftware/morf/dataset/mergingDatabaseDatasetConsumer/simple/sourceDataset2/").toURI().toURL();

    log.info("initialDataset URL: " + initialDataset.toString());
    log.info("datasetToMerge URL: " + datasetToMerge.toString());
    // GIVEN

    // ... a control extract (provided)

    // ... a database with some data
    SqlScriptExecutorProvider sqlScriptExecutorProvider = new SqlScriptExecutorProvider(connectionResources.getDataSource(), Providers.of(connectionResources.sqlDialect()));

    log.info("Creating the initial DataSet");

    DataSetConsumer firstDatabaseDataSetConsumer = new SchemaModificationAdapter(new DatabaseDataSetConsumer(connectionResources, sqlScriptExecutorProvider));
    new DataSetConnector(new XmlDataSetProducer(initialDataset), firstDatabaseDataSetConsumer).connect();

    log.info("Initial DataSet creation complete");

    // WHEN

    // ... we merge a datasource having both overlapping and non-overlapping tables and records into it

    DataSetConsumer mergingDatabaseDatasetConsumer = new MergingDatabaseDataSetConsumer(connectionResources, sqlScriptExecutorProvider);
    new DataSetConnector(new XmlDataSetProducer(datasetToMerge), mergingDatabaseDatasetConsumer).connect();

    // ... and we pipe the result into a zip file
    log.info("Creating an XML extract from the merged database tables.");
    File mergedExtractsAsFile = getDatabaseAsFile();
    log.info("Merged XML file creation complete.");

    // THEN

    // ... the resulting dataset matches the control one
    assertThat("the merged dataset should match the control one", mergedExtractsAsFile, sameXmlFileAndLengths(controlExtract));
  }
}