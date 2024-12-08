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

package org.alfasoftware.morf.xml;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.function.Supplier;

import javax.xml.stream.FactoryConfigurationError;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;

import org.alfasoftware.morf.dataset.DataSetProducer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataSetUtils;
import org.alfasoftware.morf.metadata.DataSetUtils.RecordBuilder;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Schema;
import org.alfasoftware.morf.metadata.SchemaUtils;
import org.alfasoftware.morf.metadata.Sequence;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.xml.XmlStreamProvider.XmlInputStreamProvider;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Charsets;
import com.google.common.base.Suppliers;
import com.google.common.io.Closeables;

/**
 * Reads XML and provides it as a data set. Uses XML pull-processing to get the
 * XML efficiently.
 */
public class XmlDataSetProducer implements DataSetProducer {

  private static final XMLInputFactory FACTORY = XMLInputFactory.newFactory();

  private static final Log log = LogFactory.getLog(XmlDataSetProducer.class);

  /**
   * Source of streams from which to read XMl.
   */
  private final XmlInputStreamProvider xmlStreamProvider;


  /**
   * Provides a file given a URL, abstracting us from the specific protocol.
   */
  private final ViewURLAsFile urlHandler;


  /**
   * Creates a data set producer that will read data from the file system.
   * <p>
   * <var>source</var> may point to either a zip file or a directory.
   * </p>
   *
   * @param source The location from which to read the data set.
   */
  public XmlDataSetProducer(URL source) {
    this(source, null, null);
  }


  /**
   * Creates a data set producer that will read data from the file system.
   * <p>
   * <var>source</var> may point to either a zip file or a directory.
   * </p>
   *
   * @param source The location from which to read the data set.
   * @param sourceUsername The username for the location from which to read the data set.
   * @param sourcePassword The password for the location from which to read the data set.
   */
  public XmlDataSetProducer(URL source, String sourceUsername, String sourcePassword) {
    super();

    if (source == null) {
      throw new IllegalArgumentException("null source passed to XmlDataSetProducer");
    }

    urlHandler = new ViewURLAsFile();
    File file = urlHandler.getFile(source, sourceUsername, sourcePassword);
    if (file.isDirectory()) {
      this.xmlStreamProvider = new DirectoryDataSet(file);
    }
    else if (file.isFile()) {
      this.xmlStreamProvider = new ArchiveDataSetReader(file);
    }
    else {
      throw new RuntimeException("Could not find [" + file + "] from [" + source + "]");
    }
  }

  /**
   * Creates a data set producer that will read Xml data from streams provided
   * by <var>xmlStreamProvider</var>.
   *
   * @param xmlStreamProvider Provides streams from which to read XML data.
   */
  public XmlDataSetProducer(XmlInputStreamProvider xmlStreamProvider) {
    super();
    this.xmlStreamProvider = xmlStreamProvider;
    this.urlHandler = new ViewURLAsFile();
  }


  /**
   * Creates an XML pull parser based on the XML reader specified at
   * construction.
   *
   * @see org.alfasoftware.morf.dataset.DataSetProducer#open()
   */
  @Override
  public void open() {
    xmlStreamProvider.open();
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#close()
   */
  @Override
  public void close() {
    xmlStreamProvider.close();
    urlHandler.close();
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#records(java.lang.String)
   */
  @Override
  public Iterable<Record> records(final String tableName) {

    return new Iterable<>() {
      @Override
      public Iterator<Record> iterator() {
        final InputStream inputStream = xmlStreamProvider.openInputStreamForTable(tableName);

        XMLStreamReader xmlStreamReader = openPullParser(inputStream);

        return new PullProcessorRecordIterator(xmlStreamReader) {
          @Override
          public boolean hasNext() {
            boolean result = super.hasNext();
            if (!result) {
              try {
                inputStream.close();
              } catch (IOException e) {
                throw new RuntimeException("Error closing input stream", e);
              }
            }

            return result;
          }

        };
      }
    };
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#isTableEmpty(java.lang.String)
   */
  @Override
  public boolean isTableEmpty(String tableName) {
    final InputStream inputStream = xmlStreamProvider.openInputStreamForTable(tableName);

    try {
      final XMLStreamReader pullParser = openPullParser(inputStream);
      PullProcessorRecordIterator pullProcessorRecordIterator = new PullProcessorRecordIterator(pullParser);
      return !pullProcessorRecordIterator.hasNext();
    } finally {
      Closeables.closeQuietly(inputStream);
    }
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#getSchema()
   */
  @Override
  public Schema getSchema() {
    return new PullProcessorMetaDataProvider(xmlStreamProvider);
  }


  /**
   * @param inputStream The inputstream to read from
   * @return A new pull parser
   */
  private static XMLStreamReader openPullParser(InputStream inputStream) {
    try {
      BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream, Charsets.UTF_8));
      Reader reader;
      int version = Version2to4TransformingReader.readVersion(bufferedReader);

      if (version == 2 || version == 3) {
        reader = new Version2to4TransformingReader(bufferedReader, version);
      } else {
        reader = bufferedReader;
      }

      if (version > 4) {
        throw new IllegalStateException("Unknown XML dataset format: "+version +"  This dataset has been produced by a later version of Morf");
      }
      return FACTORY.createXMLStreamReader(reader);
    } catch (XMLStreamException|FactoryConfigurationError e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Provides meta data based on an {@link XmlInputStreamProvider}.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private static final class PullProcessorMetaDataProvider implements Schema {

    /**
     * The source for data.
     */
    private final XmlInputStreamProvider xmlStreamProvider;


    /**
     * @param xmlStreamProvider The source stream provider.
     */
    public PullProcessorMetaDataProvider(XmlInputStreamProvider xmlStreamProvider) {
      super();
      this.xmlStreamProvider = xmlStreamProvider;
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#getTable(java.lang.String)
     */
    @Override
    public Table getTable(String name) {
      // Read the meta data for the specified table
      InputStream inputStream = xmlStreamProvider.openInputStreamForTable(name);
      try {
        XMLStreamReader xmlStreamReader = openPullParser(inputStream);
        XmlPullProcessor.readTag(xmlStreamReader, XmlDataSetNode.TABLE_NODE);

        String version = xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.VERSION_ATTRIBUTE);
        if (StringUtils.isNotEmpty(version)) {
          return new PullProcessorTableMetaData(xmlStreamReader, Integer.parseInt(version));
        } else {
          return new PullProcessorTableMetaData(xmlStreamReader, 1);
        }
      } finally {
        // abandon any remaining content
        Closeables.closeQuietly(inputStream);
      }
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#isEmptyDatabase()
     */
    @Override
    public boolean isEmptyDatabase() {
      return xmlStreamProvider.availableStreamNames().isEmpty();
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#tableExists(java.lang.String)
     */
    @Override
    public boolean tableExists(String name) {
      return xmlStreamProvider.tableExists(name);
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#tableNames()
     */
    @Override
    public Collection<String> tableNames() {
      return xmlStreamProvider.availableStreamNames();
    }

    @Override
    public Collection<String> partitionedTableNames() {
      return List.of();
    }

    @Override
    public Collection<String> partitionTableNames() {
      return List.of();
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#tables()
     */
    @Override
    public Collection<Table> tables() {
      List<Table> tables = new ArrayList<>();
      for (String tableName : tableNames()) {
        tables.add(getTable(tableName));
      }
      return tables;
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#viewExists(java.lang.String)
     */
    @Override
    public boolean viewExists(String name) {
      return false;
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#getView(java.lang.String)
     */
    @Override
    public View getView(String name) {
      throw new IllegalArgumentException("No view named [" + name + "]. Views not supported in XML datasets");
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#viewNames()
     */
    @Override
    public Collection<String> viewNames() {
      return Collections.emptySet();
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#views()
     */
    @Override
    public Collection<View> views() {
      return Collections.emptySet();
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#sequenceExists(String)
     */
    @Override
    public boolean sequenceExists(String name) {
      return false;
    }


    /**
     * @see org.alfasoftware.morf.metadata.Schema#getSequence(String)
     */
    @Override
    public Sequence getSequence(String name) {
      throw new IllegalArgumentException("No sequence named [" + name + "]. Sequences not supported in XML datasets");
    }


    /**
     * @see Schema#sequenceNames()
     */
    @Override
    public Collection<String> sequenceNames() {
      return Collections.emptySet();
    }


    /**
     * @see Schema#sequences()
     */
    @Override
    public Collection<Sequence> sequences() {
      return Collections.emptySet();
    }

  }


  /**
   * Provides table meta data from an XML data source.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private static final class PullProcessorTableMetaData extends XmlPullProcessor implements Table {

    /**
     * Holds the column meta data.
     */
    private final List<Column> columns = new LinkedList<>();

    /**
     * Holds the index data
     */
    private final List<Index>  indexes = new LinkedList<>();

    /**
     * Holds the table name.
     */
    private final String       tableName;


    /**
     * @param xmlStreamReader pull parser that provides the xml data
     * @param xmlFormatVersion The format version.
     */
    public PullProcessorTableMetaData(XMLStreamReader xmlStreamReader, int xmlFormatVersion) {
      super(xmlStreamReader);

      if (xmlFormatVersion < 2) {
        columns.add(SchemaUtils.idColumn());
        columns.add(SchemaUtils.versionColumn());
      }

      // Buffer the meta data
      readTag(XmlDataSetNode.METADATA_NODE);
      tableName = xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.NAME_ATTRIBUTE);

      try {
        for (String nextTag = readNextTagInsideParent(XmlDataSetNode.METADATA_NODE); nextTag != null; nextTag = readNextTagInsideParent(XmlDataSetNode.METADATA_NODE)) {

          if (XmlDataSetNode.COLUMN_NODE.equals(nextTag)) {
            Column column = new PullProcessorColumn();
            columns.add(column);
          }

          if (XmlDataSetNode.INDEX_NODE.equals(nextTag)) {
            indexes.add(new PullProcessorIndex());
          }
        }

      } catch (RuntimeException e) {
        throw new RuntimeException("Error parsing metadata for table [" + tableName + "]", e);
      }

    }


    /**
     * @see org.alfasoftware.morf.metadata.Table#columns()
     */
    @Override
    public List<Column> columns() {
      return columns;
    }


    /**
     * @see org.alfasoftware.morf.metadata.Table#getName()
     */
    @Override
    public String getName() {
      return tableName;
    }


    /**
     * @see org.alfasoftware.morf.metadata.Table#indexes()
     */
    @Override
    public List<Index> indexes() {
      return indexes;
    }

    /**
     * Implementation of {@link Column} that reads data from an XML Pull
     * processor.
     *
     * @author Copyright (c) Alfa Financial Software 2010
     */
    private final class PullProcessorColumn implements Column {

      /**
       * Holds the column name.
       */
      private final String   columnName;
      private final Supplier<String> upperCaseColumnName;

      /**
       * Holds the column data type.
       */
      private final DataType dataType;

      /**
       * Holds the width, may be null if n/a
       */
      private final Integer  width;

      /**
       * Holds the scale, may be null if n/a
       */
      private final Integer  scale;

      private final String   defaultValue;

      /**
       * Holds the nullable property, may be null if n/a
       */
      private Boolean        nullable;

      private Boolean        primaryKey;

      private boolean        autonumbered;

      /**
       * Autonumber start value.  May be null if n/a
       */
      private Integer        autonumberStart;


      /**
       * Creates the column and buffers its meta data.
       */
      public PullProcessorColumn() {
        super();
        columnName = xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.NAME_ATTRIBUTE).intern();
        upperCaseColumnName = Suppliers.memoize(() -> columnName.toUpperCase().intern());
        dataType = DataType.valueOf(xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.TYPE_ATTRIBUTE));
        defaultValue = StringUtils.defaultString(xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.DEFAULT_ATTRIBUTE));

        try {
          // not all datatypes need a width
          if (dataType.hasWidth()) {
            // The use of null indicates that although a scale should exist none
            // was provided.
            String widthString = xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.WIDTH_ATTRIBUTE);
            width = StringUtils.isEmpty(widthString) ? null : Integer.valueOf(widthString);
          } else {
            width = 0;
          }

          // not all datatypes need a scale
          if (dataType.hasScale()) {
            // The use of null indicates that although a scale should exist none
            // was provided.
            String scaleString = xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.SCALE_ATTRIBUTE);
            scale = StringUtils.isEmpty(scaleString) ? null : Integer.valueOf(scaleString);
          } else {
            scale = 0;
          }

          String nullableString = xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.NULLABLE_ATTRIBUTE);
          nullable = StringUtils.isEmpty(nullableString) ? null : Boolean.valueOf(nullableString);

          String primaryKeyString = xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.PRIMARYKEY_ATTRIBUTE);
          primaryKey = StringUtils.isEmpty(primaryKeyString) ? null : Boolean.valueOf(primaryKeyString);

          String autoNumString = xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.AUTONUMBER_ATTRIBUTE);
          autonumbered = StringUtils.isNotEmpty(autoNumString);
          autonumberStart = autonumbered ? Integer.valueOf(autoNumString) : null;

        } catch (NumberFormatException nfe) {
          throw new RuntimeException("Error parsing metadata for column [" + columnName + "]", nfe);
        }

      }


      /**
       * @see org.alfasoftware.morf.metadata.Column#getName()
       */
      @Override
      public String getName() {
        return columnName;
      }


      /**
       * @see org.alfasoftware.morf.metadata.Column#getUpperCaseName()
       */
      @Override
      public String getUpperCaseName() {
        return upperCaseColumnName.get();
      }


      /**
       * @see org.alfasoftware.morf.metadata.Column#getScale()
       */
      @Override
      public int getScale() {
        if (scale == null) {
          return 0;
        }
        return scale;
      }


      /**
       * @see org.alfasoftware.morf.metadata.Column#getType()
       */
      @Override
      public DataType getType() {
        return dataType;
      }


      /**
       * @see org.alfasoftware.morf.metadata.Column#getWidth()
       */
      @Override
      public int getWidth() {
        if (width == null) {
          return 0;
        }
        return width;
      }


      /**
       * @see org.alfasoftware.morf.metadata.Column#getDefaultValue()
       */
      @Override
      public String getDefaultValue() {
        return defaultValue;
      }


      /**
       * @see org.alfasoftware.morf.metadata.Column#isNullable()
       */
      @Override
      public boolean isNullable() {
        if (nullable == null) {
          return false;
        }
        return nullable;
      }


      /**
       * @see org.alfasoftware.morf.metadata.Column#isPrimaryKey()
       */
      @Override
      public boolean isPrimaryKey() {
        return primaryKey != null && primaryKey;
      }


      /**
       * @see org.alfasoftware.morf.metadata.Column#isAutoNumbered()
       */
      @Override
      public boolean isAutoNumbered() {
        return autonumbered;
      }


      /**
       * @see org.alfasoftware.morf.metadata.Column#getAutoNumberStart()
       */
      @Override
      public int getAutoNumberStart() {
        return autonumberStart == null ? 0 : autonumberStart;
      }


      @Override
      public String toString() {
        return this.toStringHelper();
      }
    }

    /**
     * Implementation of {@link Index} that can read from the pull processor.
     *
     * @author Copyright (c) Alfa Financial Software 2010
     */
    private class PullProcessorIndex implements Index {

      /**
       */
      private final String       indexName;
      /**
       */
      private final boolean      isUnique;
      /**
       */
      private final List<String> columnNames = new LinkedList<>();


      /**
       * Creates the index and reads the meta-data
       */
      public PullProcessorIndex() {
        super();
        indexName = xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.NAME_ATTRIBUTE);
        isUnique = Boolean.parseBoolean(xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.UNIQUE_ATTRIBUTE));

        String columnsNamesCombined = xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.COLUMNS_ATTRIBUTE);

        for (String columnName : StringUtils.split(columnsNamesCombined, ",")) {
          columnNames.add(columnName.trim());
        }
      }


      /**
       * @see org.alfasoftware.morf.metadata.Index#getName()
       */
      @Override
      public String getName() {
        return indexName;
      }


      /**
       * @see org.alfasoftware.morf.metadata.Index#isUnique()
       */
      @Override
      public boolean isUnique() {
        return isUnique;
      }


      /**
       * @see org.alfasoftware.morf.metadata.Index#columnNames()
       */
      @Override
      public List<String> columnNames() {
        return columnNames;
      }


      @Override
      public String toString() {
        return this.toStringHelper();
      }
    }


    /**
     * {@inheritDoc}
     *
     * @see org.alfasoftware.morf.metadata.Table#isTemporary()
     */
    @Override
    public boolean isTemporary() {
      return false;
    }

  }

  /**
   * Provides on demand XML reading as a record iterator.
   *
   * @author Copyright (c) Alfa Financial Software 2010
   */
  private static class PullProcessorRecordIterator extends XmlPullProcessor implements Iterator<Record> {

    /**
     * Stores the column names we need to provide values for.
     */
    private final Map<String, String> columnNamesAndUpperCase = new HashMap<>();

    /**
     * Holds the current tag so we know if there is a record to read.
     */
    private String currentTagName;


    /**
     * @param xmlStreamReader Input stream containing the source XML data.
     */
    public PullProcessorRecordIterator(XMLStreamReader xmlStreamReader) {
      super(xmlStreamReader);

      // Store the column names and get to the first record
      readTag(XmlDataSetNode.TABLE_NODE);

      // read the meta data
      Table table;
      String version = xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.VERSION_ATTRIBUTE);
      if (StringUtils.isNotEmpty(version)) {
        table = new PullProcessorTableMetaData(xmlStreamReader, Integer.parseInt(version));
      } else {
        table = new PullProcessorTableMetaData(xmlStreamReader, 1);
      }

      for (Column column : table.columns()) {
        columnNamesAndUpperCase.put(column.getName(), column.getUpperCaseName());
      }

      readTag(XmlDataSetNode.DATA_NODE);
      currentTagName = readNextTagInsideParent(XmlDataSetNode.DATA_NODE);
    }


    /**
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
      return XmlDataSetNode.RECORD_NODE.equals(currentTagName);
    }


    /**
     * @see java.util.Iterator#next()
     */
    @Override
    public Record next() {
      if (hasNext()) {
        // Buffer this record
        RecordBuilder result = DataSetUtils.record();
        for (Entry<String, String> columnNameAndUpperCase : columnNamesAndUpperCase.entrySet()) {
          result.setString(columnNameAndUpperCase.getValue(),
            Escaping.unescapeCharacters(xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, columnNameAndUpperCase.getKey()))
          );
        }

        // Is there another
        currentTagName = readNextTagInsideParent(XmlDataSetNode.DATA_NODE);

        return result;
      } else {
        throw new NoSuchElementException("No more records");
      }
    }


    /**
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
      throw new UnsupportedOperationException("Cannot remove item from a record iterator");
    }
  }

}