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

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.xml.serializer.Method;
import org.apache.xml.serializer.OutputPropertiesFactory;
import org.apache.xml.serializer.Serializer;
import org.apache.xml.serializer.SerializerFactory;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

import org.alfasoftware.morf.dataset.DataSetConsumer;
import org.alfasoftware.morf.dataset.Record;
import org.alfasoftware.morf.metadata.Column;
import org.alfasoftware.morf.metadata.DataType;
import org.alfasoftware.morf.metadata.Index;
import org.alfasoftware.morf.metadata.Table;
import org.alfasoftware.morf.xml.XmlStreamProvider.XmlOutputStreamProvider;

/**
 * Serialises data sets to XML.
 *
 * <p>The output from this class can be sent directly to the file system by using the
 * constructor {@link #XmlDataSetConsumer(File)}. Alternatively the output from this
 * class can be sent to one or many streams by calling the constructor
 * accepting the {@link XmlOutputStreamProvider}.
 * </p>
 *
 * @author Copyright (c) Alfa Financial Software 2009
 */
public class XmlDataSetConsumer implements DataSetConsumer {

  /**
   * Attributes implementation for use when no attributes are present on a node.
   */
  private static final Attributes EMPTY_ATTRIBUTES = new AttributesImpl();

  /**
   * Controls the behaviour of the consumer when running against a directory.
   */
  public enum ClearDestinationBehaviour {
    /**
     * Clear the destination out before extracting (the default)
     */
    CLEAR,

    /**
     * Overwrite the destination
     */
    OVERWRITE
  }

  /**
   * Source of content handlers to send data to.
   */
  private final XmlOutputStreamProvider xmlStreamProvider;

  /**
   * What to do about clearing the destination.
   */
  private final ClearDestinationBehaviour clearDestinationBehaviour;

  /**
   * Creates a data set consumer that will pipe the data set to the file system location
   * specified by <var>file</var>.
   *
   * <p>The serialised output can be written to a single archive or multiple data files:</p>
   * <ul>
   * <li>If <var>file</var> identifies a directory then each table in the data set is
   * serialised to a separate XML file within that directory.</li>
   * <li>If <var>file</var> identifies a file name then the file will be created or replaced with
   * a zip archive containing one XML file per table in the data set.</li>
   * </ul>
   *
   * @param file The file system location to receive the data set.
   */
  public XmlDataSetConsumer(File file) {
    this(file, ClearDestinationBehaviour.CLEAR);
  }


  /**
   * Creates a data set consumer that will pipe the data set to the file system location
   * specified by <var>file</var>.
   *
   * <p>The serialised output can be written to a single archive or multiple data files:</p>
   * <ul>
   * <li>If <var>file</var> identifies a directory then each table in the data set is
   * serialised to a separate XML file within that directory.</li>
   * <li>If <var>file</var> identifies a file name then the file will be created or replaced with
   * a zip archive containing one XML file per table in the data set.</li>
   * </ul>
   *
   * @param file The file system location to receive the data set.
   * @param clearDestinationBehaviour Whether to clear the destination directory or not.
   */
  public XmlDataSetConsumer(File file, ClearDestinationBehaviour clearDestinationBehaviour) {
    super();
    if (file.isDirectory()) {
      this.xmlStreamProvider = new DirectoryDataSet(file);
    } else {
      this.xmlStreamProvider = new ArchiveDataSetWriter(file);
    }
    this.clearDestinationBehaviour = clearDestinationBehaviour;
  }


  /**
   * Creates a data set consumer that will pipe the data set to the streams
   * returned from <var>xmlStreamProvider</var>.
   *
   * @param xmlOutputStreamProvider Provides streams to receive the XML content
   * for the data in the data set.
   */
  public XmlDataSetConsumer(XmlOutputStreamProvider xmlOutputStreamProvider) {
    this(xmlOutputStreamProvider, ClearDestinationBehaviour.CLEAR);
  }


  /**
   * Creates a data set consumer that will pipe the data set to the streams
   * returned from <var>xmlStreamProvider</var>.
   *
   * @param xmlOutputStreamProvider Provides streams to receive the XML content
   * for the data in the data set.
   * @param clearDestinationBehaviour The behaviour of the consumer when running against a directory.
   */
  public XmlDataSetConsumer(XmlOutputStreamProvider xmlOutputStreamProvider, ClearDestinationBehaviour clearDestinationBehaviour) {
    super();
    this.xmlStreamProvider = xmlOutputStreamProvider;
    this.clearDestinationBehaviour = clearDestinationBehaviour;
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#open()
   */
  @Override
  public void open() {
    xmlStreamProvider.open();

    if (clearDestinationBehaviour.equals(ClearDestinationBehaviour.CLEAR)) {
      // we're outputting, so clear the destination of any previous runs
      xmlStreamProvider.clearDestination();
    }
  }


  /**
   * Fired when a dataset has ended.
   *
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#close(org.alfasoftware.morf.dataset.DataSetConsumer.CloseState)
   */
  @Override
  public void close(CloseState closeState) {
    xmlStreamProvider.close();
  }


  /**
   * @see org.alfasoftware.morf.dataset.DataSetConsumer#table(org.alfasoftware.morf.metadata.Table, java.lang.Iterable)
   */
  @Override
  public void table(final Table table, final Iterable<Record> records) {
    if (table == null) {
      throw new IllegalArgumentException("table is null");
    }

    // Get a content handler for this table
    try {
      OutputStream outputStream = xmlStreamProvider.openOutputStreamForTable(table.getName());
      try {
        ContentHandler contentHandler = createContentHandler(outputStream);

        contentHandler.startDocument();
        AttributesImpl tableAttributes = new AttributesImpl();
        tableAttributes.addAttribute(XmlDataSetNode.URI, XmlDataSetNode.VERSION_ATTRIBUTE, XmlDataSetNode.VERSION_ATTRIBUTE, XmlDataSetNode.STRING_TYPE, "2");

        contentHandler.startElement(XmlDataSetNode.URI, XmlDataSetNode.TABLE_NODE, XmlDataSetNode.TABLE_NODE, tableAttributes);
        outputTableMetaData(table, contentHandler);
        contentHandler.startElement(XmlDataSetNode.URI, XmlDataSetNode.DATA_NODE, XmlDataSetNode.DATA_NODE, EMPTY_ATTRIBUTES);

        for (Record record : records) {
          AttributesImpl rowValueAttributes = new AttributesImpl();
          for (Column column : table.columns()) {
            String value = getValue(record, column, table.getName());
            if (value != null) {
              rowValueAttributes.addAttribute(XmlDataSetNode.URI, column.getName(), column.getName(), XmlDataSetNode.STRING_TYPE, value);
            }
          }

          emptyElement(contentHandler, XmlDataSetNode.RECORD_NODE, rowValueAttributes);
        }

        contentHandler.endElement(XmlDataSetNode.URI, XmlDataSetNode.DATA_NODE, XmlDataSetNode.DATA_NODE);
        contentHandler.endElement(XmlDataSetNode.URI, XmlDataSetNode.TABLE_NODE, XmlDataSetNode.TABLE_NODE);
        contentHandler.endDocument();
      } finally {
        outputStream.close();
      }
    } catch (Exception e) {
      throw new RuntimeException("Error consuming table [" + table.getName() + "]", e);
    }
  }


  /**
   * Get the value of the column from the provided record. Can be overridden in subclasses if required.
   *
   * @param record the record to extract a value from
   * @param column the column to extract a value from
   * @param table the name of the table being processed
   * @return the value of column from record
   */
  protected String getValue(Record record, Column column, @SuppressWarnings("unused") String table) {
    return record.getValue(column.getName());
  }


  /**
   * @param outputStream The output
   * @return A content handler
   * @throws IOException When there's an XML error
   */
  private ContentHandler createContentHandler(OutputStream outputStream) throws IOException {
    Properties outputProperties = OutputPropertiesFactory.getDefaultMethodProperties(Method.XML);
    outputProperties.setProperty("indent", "yes");
    outputProperties.setProperty(OutputPropertiesFactory.S_KEY_INDENT_AMOUNT, "2");
    outputProperties.setProperty(OutputPropertiesFactory.S_KEY_LINE_SEPARATOR, "\n");
    Serializer serializer = SerializerFactory.getSerializer(outputProperties);
    serializer.setOutputStream(outputStream);
    return serializer.asContentHandler();
  }


  /**
   * Serialise the meta data for a table.
   *
   * @param table The meta data to serialise.
   * @param contentHandler Content handler to receive the meta data xml.
   * @throws SAXException Propagates from SAX API calls.
   */
  private void outputTableMetaData(Table table, ContentHandler contentHandler) throws SAXException {
    AttributesImpl tableAttributes = new AttributesImpl();
    tableAttributes.addAttribute(XmlDataSetNode.URI, XmlDataSetNode.NAME_ATTRIBUTE, XmlDataSetNode.NAME_ATTRIBUTE,
      XmlDataSetNode.STRING_TYPE, table.getName());
    contentHandler.startElement(XmlDataSetNode.URI, XmlDataSetNode.METADATA_NODE, XmlDataSetNode.METADATA_NODE, tableAttributes);

    for (Column column : table.columns()) {
      emptyElement(contentHandler, XmlDataSetNode.COLUMN_NODE, buildColumnAttributes(column));
    }

    // we need to sort the indexes by name to ensure consistency, since indexes don't have an explicit "sequence" in databases.
    List<Index> indexes = new ArrayList<Index>(table.indexes());
    Collections.sort(indexes, new Comparator<Index>() {
      @Override
      public int compare(Index o1, Index o2) {
        return o1.getName().compareTo(o2.getName());
      }
    });

    for (Index index : indexes) {
      emptyElement(contentHandler, XmlDataSetNode.INDEX_NODE, buildIndexAttributes(index));
    }

    contentHandler.endElement(XmlDataSetNode.URI, XmlDataSetNode.METADATA_NODE, XmlDataSetNode.METADATA_NODE);
  }

  /**
   * Build the attributes for a database index
   *
   * @param index The index
   * @return The attributes
   */
  private Attributes buildIndexAttributes(Index index) {
    AttributesImpl indexAttributes = new AttributesImpl();

    indexAttributes.addAttribute(XmlDataSetNode.URI, XmlDataSetNode.NAME_ATTRIBUTE, XmlDataSetNode.NAME_ATTRIBUTE,
      XmlDataSetNode.STRING_TYPE, index.getName());
    indexAttributes.addAttribute(XmlDataSetNode.URI, XmlDataSetNode.UNIQUE_ATTRIBUTE, XmlDataSetNode.UNIQUE_ATTRIBUTE,
      XmlDataSetNode.STRING_TYPE, Boolean.toString(index.isUnique()));

    String columnNames = StringUtils.join(index.columnNames(), ",");
    indexAttributes.addAttribute(XmlDataSetNode.URI, XmlDataSetNode.COLUMNS_ATTRIBUTE, XmlDataSetNode.COLUMNS_ATTRIBUTE,
      XmlDataSetNode.STRING_TYPE, columnNames);

    return indexAttributes;
  }


  /**
   * @param column The column for which to build attributes
   * @return The attributes
   */
  private AttributesImpl buildColumnAttributes(Column column) {
    AttributesImpl columnAttributes = new AttributesImpl();

    DataType columnDateType = column.getType();

    columnAttributes.addAttribute(XmlDataSetNode.URI, XmlDataSetNode.NAME_ATTRIBUTE, XmlDataSetNode.NAME_ATTRIBUTE, XmlDataSetNode.STRING_TYPE, column.getName());
    columnAttributes.addAttribute(XmlDataSetNode.URI, XmlDataSetNode.TYPE_ATTRIBUTE, XmlDataSetNode.TYPE_ATTRIBUTE, XmlDataSetNode.STRING_TYPE, columnDateType.name());

    // the width attribute may not be required by this data type
    if (columnDateType.hasWidth() && column.getWidth() > 0) {
      columnAttributes.addAttribute(XmlDataSetNode.URI, XmlDataSetNode.WIDTH_ATTRIBUTE, XmlDataSetNode.WIDTH_ATTRIBUTE, XmlDataSetNode.STRING_TYPE, Integer.toString(column.getWidth()));
    }

    // the scale attribute may not be required by this data type
    if (columnDateType.hasScale() && column.getScale() > 0) {
      columnAttributes.addAttribute(XmlDataSetNode.URI, XmlDataSetNode.SCALE_ATTRIBUTE, XmlDataSetNode.SCALE_ATTRIBUTE, XmlDataSetNode.STRING_TYPE, Integer.toString(column.getScale()));
    }

    if (StringUtils.isNotEmpty(column.getDefaultValue())) {
      columnAttributes.addAttribute(XmlDataSetNode.URI, XmlDataSetNode.DEFAULT_ATTRIBUTE, XmlDataSetNode.DEFAULT_ATTRIBUTE, XmlDataSetNode.STRING_TYPE, column.getDefaultValue());
    }

    if (column.isNullable()) {
      columnAttributes.addAttribute(XmlDataSetNode.URI, XmlDataSetNode.NULLABLE_ATTRIBUTE, XmlDataSetNode.NULLABLE_ATTRIBUTE, XmlDataSetNode.STRING_TYPE, Boolean.toString(column.isNullable()));
    }

    if (column.isPrimaryKey()) {
      columnAttributes.addAttribute(XmlDataSetNode.URI, XmlDataSetNode.PRIMARYKEY_ATTRIBUTE, XmlDataSetNode.PRIMARYKEY_ATTRIBUTE, XmlDataSetNode.STRING_TYPE, Boolean.toString(column.isPrimaryKey()));
    }

    if (column.isAutoNumbered()) {
      columnAttributes.addAttribute(XmlDataSetNode.URI, XmlDataSetNode.AUTONUMBER_ATTRIBUTE, XmlDataSetNode.AUTONUMBER_ATTRIBUTE, XmlDataSetNode.STRING_TYPE, Integer.toString(column.getAutoNumberStart()));
    }

    return columnAttributes;
  }

  /**
   * Output an empty (self-closing) element:  <foo/>
   *
   * @param contentHandler The content handler
   * @param name The element name
   * @param attributes The attributes
   * @throws SAXException When there's a writer error
   */
  private void emptyElement(ContentHandler contentHandler, String name, Attributes attributes) throws SAXException {
    contentHandler.startElement(XmlDataSetNode.URI, name, name, attributes);
    contentHandler.endElement(XmlDataSetNode.URI, name, name);
  }
}
