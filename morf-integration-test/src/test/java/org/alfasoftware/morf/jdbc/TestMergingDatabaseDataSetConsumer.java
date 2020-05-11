package org.alfasoftware.morf.jdbc;

import static org.hamcrest.collection.IsEmptyCollection.empty;
import static org.junit.Assert.assertThat;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;

import org.alfasoftware.morf.dataset.DataSetConnector;
import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.dataset.DataSetHomology;
import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.xml.XmlDataSetConsumer;
import org.alfasoftware.morf.xml.XmlDataSetProducer;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import com.google.inject.util.Providers;
import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

/**
 * Verifies that two start position merge into
 */
@RunWith(JUnitParamsRunner.class)
public class TestMergingDatabaseDataSetConsumer {

  private static final Log log = LogFactory.getLog(TestMergingDatabaseDataSetConsumer.class);

  @Rule
  public TemporaryFolder temporaryFolder = new TemporaryFolder();

  /**
   * Resource path for the extract of agreemnt 000000005001
   */
  private static final String AGREEMENT_000000005001_PATH = "/000000005001.zip";

  /**
   * Resource path for the extract of agreemnt 000000022001
   */
  private static final String AGREEMENT_000000022001_PATH = "/000000022001.zip";

  /**
   * Resource path for the extract of both agreement 000000005001 and 000000022001
   */
  private static final String AGREEMENT_000000005001_AND_000000022001_PATH = "/000000005001_AND_000000022001.zip";

  ConnectionResourcesBean connectionResources;

  @Before
  public void setup() {
    connectionResources = new ConnectionResourcesBean();
    connectionResources.setDatabaseType("H2");
  }


  private File cryoTargetDatabaseIntoFile() throws IOException {
    DatabaseDataSetProducer databaseDataSetProducer = new DatabaseDataSetProducer(connectionResources);
    File mergedExtractAsFile = temporaryFolder.newFile("merged-extract.zip");
    mergedExtractAsFile.createNewFile();

    XmlDataSetConsumer fileConsumer = new XmlDataSetConsumer(mergedExtractAsFile);
    new DataSetConnector(databaseDataSetProducer, fileConsumer).connect();

    return mergedExtractAsFile;
  }

  /**
   * Verifies that merging two extracts containing a single agreement results in the same contents as the extract
   * produced by extracting the two agreements at once.
   */
  @Test
  @Parameters(method = "mergeParameters")
  public void testMergeTwoExtracts(File controlExtract, File initialDataset, File datasetToMerge) throws IOException {
    // GIVEN

    // ... a control extract (provided)

    // ... a database with some data
    SqlScriptExecutorProvider sqlScriptExecutorProvider = new SqlScriptExecutorProvider(connectionResources.getDataSource(), Providers.of(connectionResources.sqlDialect()));

    log.info("Creating the initial DataSet");

    DataSetConsumer firstDatabaseDataSetConsumer = new SchemaModificationAdapter(new DatabaseDataSetConsumer(connectionResources, sqlScriptExecutorProvider));
    new DataSetConnector(toDataSetProducer(initialDataset), firstDatabaseDataSetConsumer).connect();

    log.info("Initial DataSet creation complete");

    // WHEN

    // ... we merge a datasource having overlapping tables and records into it

    DataSetConsumer mergingDatabaseDatasetConsumer = new MergingDatabaseDataSetConsumer(connectionResources, sqlScriptExecutorProvider);
    new DataSetConnector(toDataSetProducer(datasetToMerge), mergingDatabaseDatasetConsumer).connect();

    // ... and we cryo to target schema into a zip
    log.info("Creating an XML extract from the merged database tables.");
    File mergedExtractsAsFile = cryoTargetDatabaseIntoFile();
    log.info("Merged XML file creation complete.");

    // THEN

    // ... the resulting dataset matches the control one.
    // CAVEAT: requires H2 and the merged extract to have the same record order
    DataSetHomology dataSetHomology = new DataSetHomology();
    dataSetHomology.dataSetProducersMatch(new DatabaseDataSetProducer(connectionResources), toDataSetProducer(mergedExtractsAsFile));
    assertThat("the merged dataset should match the control one", dataSetHomology.getDifferences(), empty());
  }


  private DataSetProducer toDataSetProducer(File file) {
    try {
      return new XmlDataSetProducer(file.toURI().toURL());
    } catch (MalformedURLException e) {
      throw new IllegalArgumentException(e);
    }
  }


  /**
   * Parameters for merge extract tests.
   *
   * @return each array item is an array of File where the first item is the control extract, the remaining ones are the extracts to merge.
   */
  private Object[] mergeParameters() {
    return new Object[] {
        new File[] { // merge two extract with only non-overlapping content in agreement.xml and both overlapping and non overlapping records in thirdparty.xml
            new File(getClass().getClassLoader().getResource("org/alfasoftware/morf/dataset/mergingDatabaseDatasetConsumer/simple/controlDataset").getFile()),
            new File(getClass().getClassLoader().getResource("org/alfasoftware/morf/dataset/mergingDatabaseDatasetConsumer/simple/sourceDataset1").getFile()),
            new File(getClass().getClassLoader().getResource("org/alfasoftware/morf/dataset/mergingDatabaseDatasetConsumer/simple/sourceDataset2").getFile())
        },
        new File[] { // cases which revealed problematic when merging two extracts from allydev
            new File(getClass().getClassLoader().getResource("org/alfasoftware/morf/dataset/mergingDatabaseDatasetConsumer/problematic-cases/controlDataset").getFile()),
            new File(getClass().getClassLoader().getResource("org/alfasoftware/morf/dataset/mergingDatabaseDatasetConsumer/problematic-cases/sourceDataset1").getFile()),
            new File(getClass().getClassLoader().getResource("org/alfasoftware/morf/dataset/mergingDatabaseDatasetConsumer/problematic-cases/sourceDataset2").getFile())
        }
    };
  }
}