package org.alfasoftware.morf.directory;

import org.alfasoftware.morf.dataset.DataSetConsumer;

import java.io.File;
import java.util.function.Function;

public abstract class DirectoryDataSetConsumer implements DataSetConsumer {

    /**
     * Source of content handlers to send data to.
     */
    protected final DirectoryStreamProvider.DirectoryOutputStreamProvider directoryOutputStreamProvider;

    /**
     * What to do about clearing the destination.
     */
    protected final ClearDestinationBehaviour clearDestinationBehaviour;

    /**
     * Creates a data set consumer that will pipe the data set to the file system location
     * specified by <var>file</var>.
     *
     * <p>The serialised output can be written to a single archive or multiple data files:</p>
     * <ul>
     * <li>If <var>file</var> identifies a directory then each table in the data set is
     * serialised to a separate file within that directory.</li>
     * <li>If <var>file</var> identifies a file name then the file will be created or replaced with
     * a zip archive containing one file per table in the data set.</li>
     * </ul>
     *
     * @param file The file system location to receive the data set.
     * @param clearDestinationBehaviour Whether to clear the destination directory or not.
     */
    public DirectoryDataSetConsumer(File file,
                                    DirectoryDataSetConsumer.ClearDestinationBehaviour clearDestinationBehaviour,
                                    Function<File, DirectoryStreamProvider.DirectoryOutputStreamProvider> archiveDataSetWriterFunction) {
        super();
        if (file.isDirectory()) {
            this.directoryOutputStreamProvider = new DirectoryDataSet(file);
        } else {
            this.directoryOutputStreamProvider = archiveDataSetWriterFunction.apply(file);
        }
        this.clearDestinationBehaviour = clearDestinationBehaviour;
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
     */
    public DirectoryDataSetConsumer(File file, Function<File, DirectoryStreamProvider.DirectoryOutputStreamProvider> archiveDataSetWriterFunction) {
        this(file, DirectoryDataSetConsumer.ClearDestinationBehaviour.CLEAR, archiveDataSetWriterFunction);
    }

    public DirectoryDataSetConsumer(DirectoryStreamProvider.DirectoryOutputStreamProvider directoryOutputStreamProvider, ClearDestinationBehaviour clearDestinationBehaviour) {
        this.directoryOutputStreamProvider = directoryOutputStreamProvider;
        this.clearDestinationBehaviour = clearDestinationBehaviour;
    }


    /**
     * @see org.alfasoftware.morf.dataset.DataSetConsumer#open()
     */
    @Override
    public void open() {
        directoryOutputStreamProvider.open();

        if (clearDestinationBehaviour.equals(DirectoryDataSetConsumer.ClearDestinationBehaviour.CLEAR)) {
            // we're outputting, so clear the destination of any previous runs
            directoryOutputStreamProvider.clearDestination();
        }
    }

    /**
     * Fired when a dataset has ended.
     *
     * @see org.alfasoftware.morf.dataset.DataSetConsumer#close(org.alfasoftware.morf.dataset.DataSetConsumer.CloseState)
     */
    @Override
    public void close(CloseState closeState) {
        directoryOutputStreamProvider.close();
    }


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
}
