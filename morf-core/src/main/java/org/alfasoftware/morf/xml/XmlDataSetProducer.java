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

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.Authenticator;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.PasswordAuthentication;
import java.net.URL;
import java.net.URLDecoder;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.jar.JarInputStream;
import java.util.zip.ZipEntry;

import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
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
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.metadata.View;
import org.alfasoftware.morf.xml.XmlStreamProvider.XmlInputStreamProvider;
import org.apache.commons.lang.StringUtils;

import com.google.common.io.ByteStreams;
import com.google.common.io.CharSource;
import com.google.common.io.Closeables;
import com.google.common.io.Files;

/**
 * Reads XML and provides it as a data set. Uses XML pull-processing to get the
 * XML efficiently.
 */
public class XmlDataSetProducer implements DataSetProducer {

  /**
   * Source of streams from which to read XMl.
   */
  private final XmlInputStreamProvider xmlStreamProvider;


   /**
    * Array of temporary files that should be deleted in the close() method.
    */
  private final List<File> tempFiles = new ArrayList<>();


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

    File file = getFile(source, sourceUsername, sourcePassword);
    if (file.isDirectory()) {
      this.xmlStreamProvider = new DirectoryDataSet(file);
    } else {
      this.xmlStreamProvider = new ArchiveDataSetReader(file);
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
    for (File file : tempFiles) {
      if (!file.delete()) {
        throw new RuntimeException("Could not delete file [" + file.getPath() + "]");
      }
    }
    xmlStreamProvider.close();
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetProducer#records(java.lang.String)
   */
  @Override
  public Iterable<Record> records(final String tableName) {

    return new Iterable<Record>() {
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
   * Used to keep track of expanded resources in the temporary directory.
   * Keys are URLs as strings.
   */
  private static Map<String, File> expandedResources = new HashMap<>();


  /**
   * @param inputStream The inputstream to read from
   * @return A new pull parser
   */
  private static XMLStreamReader openPullParser(InputStream inputStream) {
    try {
      return XMLInputFactory.newFactory().createXMLStreamReader(inputStream, "UTF-8");
    } catch (XMLStreamException|FactoryConfigurationError e) {
      throw new RuntimeException(e);
    }
  }


  /**
   * Get a {@link File} for the resource at <var>url</var>. If the URL points to
   * within a ZIP or JAR file, the entry is expanded into a temporary directory
   * and a file reference to that returned.
   *
   * @param url Target to extract.
   * @param urlUsername the username for the url.
   * @param urlPassword the password for the url.
   * @return Location on disk at which file operations can be performed on
   *         <var>url</var>
   */
  private File getFile(URL url, String urlUsername, String urlPassword) {
    if (url.getProtocol().equals("file")) {
      try {
        return new File(URLDecoder.decode(url.getPath(), "utf-8"));
      } catch (UnsupportedEncodingException e) {
        throw new IllegalStateException("UTF-8 is not supported encoding!", e);
      }

    } else if (url.getProtocol().equals("jar")) {
      synchronized (expandedResources) {
        File result = expandedResources.get(url.toString());
        if (result != null) {
          return result;
        }

        // Either need a temporary file, or a temporary directory
        try {
          result = File.createTempFile(this.getClass().getSimpleName() + "-"
              + StringUtils.abbreviate(url.toString(), 32, 96).replaceAll("\\W+", "_") + "-", ".database");
          result.deleteOnExit();
          expandedResources.put(url.toString(), result);

          JarURLConnection jar = (JarURLConnection) url.openConnection();
          if (jar.getJarEntry().isDirectory()) {
            if (!result.delete() || !result.mkdirs()) {
              throw new IllegalStateException("Unable to transform [" + result + "] into a temporary directory");
            }

            try (JarInputStream input = new JarInputStream(jar.getJarFileURL().openStream())) {
            String prefix = jar.getJarEntry().getName();
            ZipEntry entry = null;
            while ((entry = input.getNextEntry()) != null) { // NOPMD
              if (entry.getName().startsWith(prefix) && !entry.isDirectory()) {
                File target = new File(result, entry.getName().substring(prefix.length()));
                if (!target.getParentFile().exists() && !target.getParentFile().mkdirs()) {
                  throw new RuntimeException("Could not make directories [" + target.getParentFile() + "]");
                }
                  try (OutputStream output = new BufferedOutputStream(new FileOutputStream(target))) {
                  ByteStreams.copy(input, output);
                }
              }
            }
            }

          } else {
            try (InputStream input = url.openStream();
                 OutputStream output = new BufferedOutputStream(new FileOutputStream(result))) {
              ByteStreams.copy(input, output);
            }
          }
        } catch (IOException e) {
          throw new RuntimeException("Unable to access [" + url + "] when targetting temporary file [" + result + "]", e);
        }

        return result;
      }

    } else if (url.getProtocol().equals("https") || url.getProtocol().equals("http")) {
      // This is an incomplete implementation for handling http(s) urls. It is only setup to work for url start positions in a directory style setup.

      if (url.getPath().endsWith(".zip")) {
        throw new RuntimeException("Currently unable to download zip files over http URL. Please Download the file and point the app to your local copy.");
      }

      // Create a temp file which will hold the xml files in the start position
      File directoryFile;
      try {
        directoryFile = File.createTempFile("urlDirectory", ".tmp");
      } catch (IOException e) {
        throw new RuntimeException("Unable to create temp url directory file", e);
      }

      // populate file from url
      downloadFileFromHttpUrl(url, urlUsername, urlPassword, directoryFile);

      // We will now have the directory file with all the links of the xml files so get a list of all the xml files
      ArrayList<String> xmlFiles = new ArrayList<>();
      CharSource source = Files.asCharSource(directoryFile, Charset.forName("UTF-8"));
      try (BufferedReader bufRead = source.openBufferedStream()) {
        String line = bufRead.readLine();
        while (line != null)
        {
          if (line.contains("file name=")) {
            int start = line.indexOf('"') + 1;
            int end = line.indexOf('"', start);
            String file = line.substring(start, end);
            if (file.endsWith(".xml")) xmlFiles.add(file);
          }
          line = bufRead.readLine();
        }
      } catch (IOException e) {
        throw new RuntimeException("Exception reading file [" + directoryFile+ "]", e);
      }

      // Download the xml files and place them in a folder
      File startPosition;
      try {
        startPosition = File.createTempFile("TempStartPosition", ".database");
      } catch (IOException e) {
        throw new RuntimeException("Unable to create start position directory", e);
      }

      if (!startPosition.delete() || !startPosition.mkdirs()) {
        throw new IllegalStateException("Unable to transform [" + startPosition + "] into a temporary directory");
      }

      for (String xml : xmlFiles) {
        File target;
        try {
          target = File.createTempFile(xml.substring(0,xml.indexOf('.')), ".xml", startPosition);
          tempFiles.add(target);
        } catch (IOException e) {
          throw new RuntimeException("Unable to create temp file for: " + xml, e);
        }
        if (!target.getParentFile().mkdirs()) {
          throw new RuntimeException("Unable to create directories for: " + target.getParentFile());
        }
        try {
          downloadFileFromHttpUrl(new URL(url.toString() + xml), urlUsername, urlPassword, target);
        } catch (MalformedURLException e) {
          throw new RuntimeException("Unable to create URL: " + url.toString() + xml, e);
        }
      }

      if (!directoryFile.delete()) {
        throw new RuntimeException("Unable to delete [" + directoryFile + "]");
      }

      // add start position directory last so that when we attempt to delete it in the close() method the files it holds will already have been deleted.
      tempFiles.add(startPosition);

      return startPosition;
    } else {
      throw new UnsupportedOperationException("Unsupported URL protocol on [" + url + "]");
    }
  }


  /**
   * Method to download file from http url
   *
   * @param url the url to download from
   * @param urlUsername the username for the url.
   * @param urlPassword the password for the url.
   * @param file the file to populate from the download
   */
  private void downloadFileFromHttpUrl(URL url, final String urlUsername, final String urlPassword, File file) {
    // Set authentication for the url
    Authenticator.setDefault (new Authenticator() {
      @Override
      protected PasswordAuthentication getPasswordAuthentication() {
          return new PasswordAuthentication (urlUsername, urlPassword.toCharArray());
      }
    });

    // Override the sun certificate trust manager so we ignore all certificate ssl handshake warnings
    TrustManager[] trustAllCerts = new TrustManager[]{
      new X509TrustManager() {
          @Override
          public java.security.cert.X509Certificate[] getAcceptedIssuers() {
              return new java.security.cert.X509Certificate[0];
          }
          @Override
          public void checkClientTrusted(java.security.cert.X509Certificate[] certs, String authType) {
            // Ignore everything
          }
          @Override
          public void checkServerTrusted(java.security.cert.X509Certificate[] certs, String authType) {
            // Ignore everything
          }
      }
    };

    // Activate the new trust manager
    try {
        SSLContext sc = SSLContext.getInstance("SSL");
        sc.init(null, trustAllCerts, new java.security.SecureRandom());
        HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    } catch (Exception e) {
      throw new RuntimeException("Unable to activate the trust manager.", e);
    }

    // Open a readable byte channel for the url
    try (ReadableByteChannel rbc = Channels.newChannel(url.openStream())) {

    // Create a file output stream to transfer the data from the url to the temp file
      try (FileOutputStream fos = new FileOutputStream(file)) {

    // transfer the data
      fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

      } catch (FileNotFoundException e) {
        throw new RuntimeException("Unable to create file output stream", e);
    } catch (IOException e) {
      throw new RuntimeException("Unable to transfer from url to temp file", e);
    }

    } catch (IOException e) {
      throw new RuntimeException("Unable to create readable byte channel", e);
    }

    // Remove username and password from being the default authentication
    Authenticator.setDefault(null);

    // Remove the overridden trust manager
    try {
      SSLContext sc = SSLContext.getInstance("SSL");
      sc.init(null, null, new java.security.SecureRandom());
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());
    } catch (Exception e) {
      throw new RuntimeException("Unable to reset the HttpsURLConnection defaultSSLSocketFactory.", e);
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
     * @param xmlPullParser pull parser that provides the xml data
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
        columnName = xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, XmlDataSetNode.NAME_ATTRIBUTE);
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

    private static final CharSequence NULL_CHARACTER = new String(new char[] { 0 /* null */ });

    /**
     * Stores the column names we need to provide values for.
     */
    private final Set<String> columnNames = new HashSet<>();

    /**
     * Holds the current tag so we know if there is a record to read.
     */
    private String currentTagName;


    /**
     * @param xmlPullParser Input stream containing the source XML data.
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
        columnNames.add(column.getName());
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
        for (String columnName : columnNames) {
          String attributeValue = xmlStreamReader.getAttributeValue(XmlDataSetNode.URI, columnName);

          // deal with escaping...
          String columnValue = attributeValue
            .replace("\\\\", "\\")
            .replace("\\0", NULL_CHARACTER);

          result.setString(columnName.toUpperCase(), columnValue);
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