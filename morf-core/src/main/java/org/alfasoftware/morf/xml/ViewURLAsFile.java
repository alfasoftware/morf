package org.alfasoftware.morf.xml;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.Closeable;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.JarURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.net.URLDecoder;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.jar.JarInputStream;
import java.util.zip.ZipEntry;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.google.common.base.Charsets;
import com.google.common.io.ByteStreams;
import com.google.common.io.CharSource;
import com.google.common.io.Files;

/**
 * Given an URL, provides a view of it as a File/Folder. The purpose is to abstract the client from the specific protocol details.
 *
 * <p>Processing some type of URLs, like http or jar, may require the creation of temporary resources. Once usage is finished
 * clients should call #close on this object in order o clean up the
 * temporary files it generated.</p>
 */
class ViewURLAsFile implements Closeable {
  private static final Log log = LogFactory.getLog(ViewURLAsFile.class);

  /**
   * Used to keep track of expanded resources in the temporary directory.
   * Keys are URLs as strings.
   */
  private final Map<String, File> expandedResources = new HashMap<>();


  /**
   * Array of temporary files that should be deleted in the close() method.
   */
  private final List<File> tempFiles = new ArrayList<>();

  /**
   * Returns a view of the Resource located by <var>url</var> as a {@link File}.
   *
   * <p>If the URL points to
   * within a ZIP or JAR file, the entry is expanded into a temporary directory
   * and a file reference to that returned.</p>
   *
   * <p>If the URL point to a zip over http/https resource then it provides a downloaded version of it.</p>
   * <p>If the URL point to a directory over http/https resource then it provides a downloaded version of it.</p>
   *
   * @param url Target to extract.
   * @param urlUsername the username for the url.
   * @param urlPassword the password for the url.
   * @return Location on disk at which file operations can be performed on
   *         <var>url</var>
   */
  File getFile(final URL url, final String urlUsername, final String urlPassword) {
    if (url.getProtocol().equals("file")) {
      log.info(url.toString() + " is a File System resource. Providing it directily as a File.");
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
            log.info(url.toString() + " is a directory inside a Jar. Inflating it to a temporary folder in order to provide a file view of it.");
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
            log.info(url.toString() + " is a file inside a Jar. Extracting it to a temporary file to provide a view of it.");
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
      if (url.getPath().endsWith(".zip")) {
        log.info(url.toString() + " is a zip over http/https. Downloading it to a temporary file to provide a view of it.");
        File dataSet = createTempFile("dataset", ".tmp");
        downloadFileFromHttpUrl(url, urlUsername, urlPassword, dataSet);
        tempFiles.add(dataSet);
        log.info("Successfully downloaded zipped data set [" + url + "] to temp file [" + dataSet + "]");
        return dataSet;
      } else {
        log.info(url.toString() + " is a directory over http/https. Downloading the remote directory to provide a view of the xml files contained in there.");
        // -- This is an experimental attempt to traverse the file directory and source the relevant XML files. YMMV.
        //
        // Create a temp file which will hold the xml files in the data set
        File directoryFile = createTempFile("urlDirectory", ".tmp");

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
        File dataSet = createTempFile("dataset", ".database");
        if (!dataSet.delete() || !dataSet.mkdirs()) {
          throw new IllegalStateException("Unable to transform [" + dataSet + "] into a temporary directory");
        }

        for (String xml : xmlFiles) {
          File target = createTempFile(xml.substring(0,xml.indexOf('.')), ".xml", dataSet);
          tempFiles.add(target);
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

        // add data set directory last so that when we attempt to delete it in the close() method the files it holds will already have been deleted.
        tempFiles.add(dataSet);

        return dataSet;
      }
    } else {
      throw new UnsupportedOperationException("Unsupported URL protocol on [" + url + "]");
    }
  }


  /**
   * Closes all the temporary files opened.
   */
  @Override
  public void close() {
    for (File file : tempFiles) {
      try {
        java.nio.file.Files.delete(file.toPath());
      } catch (Exception e) {
        throw new RuntimeException("Could not delete file [" + file.getPath() + "]", e);
      }
    }
  }


  /**
   * Wrapper for {@link java.io.File#createTempFile(String, String)} that
   * wraps any exceptions in a {@link RuntimeException} and propagates it.
   */
  private File createTempFile(String prefix, String suffix) {
    return createTempFile(prefix, suffix, null);
  }


  /**
   * Wrapper for {@link java.io.File#createTempFile(String, String, File)} that
   * wraps any exceptions in a {@link RuntimeException} and propagates it.
   */
  private File createTempFile(String prefix, String suffix, File file) {
    try {
      return File.createTempFile(prefix, suffix, file);
    } catch (IOException e) {
      throw new RuntimeException("Unable to create temp file", e);
    }
  }


  /**
   * Downloads a file over HTTP/HTTPS.
   *
   * @param url The url to download from.
   * @param urlUsername The username for the url.
   * @param urlPassword The password for the url.
   * @param file The file to populate from the download.
   */
  private void downloadFileFromHttpUrl(final URL url, final String urlUsername, final String urlPassword, File file) {
    // -- Create connection to URL...
    //
    URLConnection urlConnection;
    try {
      urlConnection = url.openConnection();
    } catch (IOException e) {
      throw new RuntimeException("Error opening connection to URL [" + url + "]", e);
    }

    // -- Set up authentication if required...
    //
    if (urlUsername != null && urlPassword != null) {
      String userpass = urlUsername + ":" + urlPassword;
      String basicAuth = "Basic " + new String(Base64.getEncoder().encode(userpass.getBytes(StandardCharsets.UTF_8)), Charsets.US_ASCII);
      urlConnection.setRequestProperty("Authorization", basicAuth);
    }

    // -- Download the file...
    //
    try (
        ReadableByteChannel readableByteChannel = Channels.newChannel(urlConnection.getInputStream());
        FileOutputStream fileOutputStream = new FileOutputStream(file);
        FileChannel fileChannel = fileOutputStream.getChannel()
    ) {
      fileChannel.transferFrom(readableByteChannel, 0, Long.MAX_VALUE);
      log.debug("Successfully downloaded file [" + url + "] to temp file [" + file + "]");
    } catch (IOException e) {
      throw new RuntimeException("Error downloading data set from [" + url + "]", e);
    }
  }

}
