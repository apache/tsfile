/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tsfile.external.commons.io;

import org.apache.tsfile.external.commons.io.file.Counters;
import org.apache.tsfile.external.commons.io.file.PathUtils;
import org.apache.tsfile.external.commons.io.file.StandardDeleteOption;
import org.apache.tsfile.external.commons.io.filefilter.FileFileFilter;
import org.apache.tsfile.external.commons.io.filefilter.IOFileFilter;
import org.apache.tsfile.external.commons.io.filefilter.SuffixFileFilter;
import org.apache.tsfile.external.commons.io.function.IOConsumer;
import org.apache.tsfile.external.commons.io.function.Uncheck;

import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.nio.charset.Charset;
import java.nio.file.CopyOption;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class FileUtils {

  private static final String PROTOCOL_FILE = "file";

  public static void deleteDirectory(final File directory) throws IOException {
    Objects.requireNonNull(directory, "directory");
    if (!directory.exists()) {
      return;
    }
    if (!isSymlink(directory)) {
      cleanDirectory(directory);
    }
    delete(directory);
  }

  public static File delete(final File file) throws IOException {
    Objects.requireNonNull(file, PROTOCOL_FILE);
    Files.delete(file.toPath());
    return file;
  }

  public static boolean isSymlink(final File file) {
    return file != null && Files.isSymbolicLink(file.toPath());
  }

  private static void requireDirectoryExists(final File directory, final String name)
      throws FileNotFoundException {
    Objects.requireNonNull(directory, name);
    if (!directory.isDirectory()) {
      if (directory.exists()) {
        throw new IllegalArgumentException(
            "Parameter '" + name + "' is not a directory: '" + directory + "'");
      }
      throw new FileNotFoundException("Directory '" + directory + "' does not exist.");
    }
  }

  private static File[] listFiles(final File directory, final FileFilter fileFilter)
      throws IOException {
    requireDirectoryExists(directory, "directory");
    final File[] files =
        fileFilter == null ? directory.listFiles() : directory.listFiles(fileFilter);
    if (files == null) {
      // null if the directory does not denote a directory, or if an I/O error occurs.
      throw new IOException("Unknown I/O error listing contents of directory: " + directory);
    }
    return files;
  }

  /**
   * Creates all directories for a File object, including any necessary but non-existent parent
   * directories. If the {@code directory} already exists or is null, nothing happens.
   *
   * <p>Calls {@link File#mkdirs()} and throws an {@link IOException} on failure.
   *
   * @param directory the receiver for {@code mkdirs()}. If the {@code directory} already exists or
   *     is null, nothing happens.
   * @throws IOException if the directory was not created along with all its parent directories.
   * @throws IOException if the given file object is not a directory.
   * @throws SecurityException See {@link File#mkdirs()}.
   * @see File#mkdirs()
   */
  public static void forceMkdir(final File directory) throws IOException {
    mkdirs(directory);
  }

  /**
   * Creates all directories for a File object, including any necessary but non-existent parent
   * directories. If the parent directory already exists or is null, nothing happens.
   *
   * <p>Calls {@link File#mkdirs()} for the parent of @{code file}.
   *
   * @param file file with parents to create, must not be {@code null}.
   * @throws NullPointerException if the file is {@code null}.
   * @throws IOException if the directory was not created along with all its parent directories.
   * @throws SecurityException See {@link File#mkdirs()}.
   * @see File#mkdirs()
   * @since 2.5
   */
  public static void forceMkdirParent(File file) throws IOException {
    forceMkdir(getParentFile(Objects.requireNonNull(file, "file")));
  }

  public static void forceDelete(final File file) throws IOException {
    Objects.requireNonNull(file, PROTOCOL_FILE);

    final Counters.PathCounters deleteCounters;
    try {
      deleteCounters =
          PathUtils.delete(
              file.toPath(),
              PathUtils.EMPTY_LINK_OPTION_ARRAY,
              StandardDeleteOption.OVERRIDE_READ_ONLY);
    } catch (final IOException ex) {
      throw new IOException("Cannot delete file: " + file, ex);
    }
    if (deleteCounters.getFileCounter().get() < 1
        && deleteCounters.getDirectoryCounter().get() < 1) {
      // didn't find a file to delete.
      throw new FileNotFoundException("File does not exist: " + file);
    }
  }

  public static void cleanDirectory(final File directory) throws IOException {
    IOConsumer.forAll(FileUtils::forceDelete, listFiles(directory, null));
  }

  public static void moveFile(
      final File srcFile, final File destFile, final CopyOption... copyOptions) throws IOException {
    validateMoveParameters(srcFile, destFile);
    checkFileExists(srcFile, "srcFile");
    requireAbsent(destFile, "destFile");
    final boolean rename = srcFile.renameTo(destFile);
    if (!rename) {
      // Don't interfere with file date on move, handled by StandardCopyOption.COPY_ATTRIBUTES
      copyFile(srcFile, destFile, false, copyOptions);
      if (!srcFile.delete()) {
        deleteQuietly(destFile);
        throw new IOException(
            "Failed to delete original file '" + srcFile + "' after copy to '" + destFile + "'");
      }
    }
  }

  private static void checkFileExists(final File file, final String name)
      throws FileNotFoundException {
    Objects.requireNonNull(file, name);
    if (!file.isFile()) {
      if (file.exists()) {
        throw new IllegalArgumentException("Parameter '" + name + "' is not a file: " + file);
      }
      if (!Files.isSymbolicLink(file.toPath())) {
        throw new FileNotFoundException("Source '" + file + "' does not exist");
      }
    }
  }

  /**
   * Requires that the given {@link File} is a file.
   *
   * @param file The {@link File} to check.
   * @param name The parameter name to use in the exception message.
   * @return the given file.
   * @throws NullPointerException if the given {@link File} is {@code null}.
   * @throws IllegalArgumentException if the given {@link File} does not exist or is not a file.
   */
  private static File requireFile(final File file, final String name) {
    Objects.requireNonNull(file, name);
    if (!file.isFile()) {
      throw new IllegalArgumentException("Parameter '" + name + "' is not a file: " + file);
    }
    return file;
  }

  private static void requireAbsent(final File file, final String name) throws FileExistsException {
    if (file.exists()) {
      throw new FileExistsException(
          String.format("File element in parameter '%s' already exists: '%s'", name, file));
    }
  }

  public static void copyFile(final File srcFile, final File destFile) throws IOException {
    copyFile(srcFile, destFile, StandardCopyOption.REPLACE_EXISTING);
  }

  public static void copyFile(
      final File srcFile, final File destFile, final CopyOption... copyOptions) throws IOException {
    copyFile(srcFile, destFile, true, copyOptions);
  }

  public static void copyFile(
      final File srcFile,
      final File destFile,
      final boolean preserveFileDate,
      final CopyOption... copyOptions)
      throws IOException {
    Objects.requireNonNull(destFile, "destination");
    checkFileExists(srcFile, "srcFile");
    requireCanonicalPathsNotEquals(srcFile, destFile);
    createParentDirectories(destFile);
    if (destFile.exists()) {
      checkFileExists(destFile, "destFile");
    }

    final Path srcPath = srcFile.toPath();

    Files.copy(srcPath, destFile.toPath(), copyOptions);

    // On Windows, the last modified time is copied by default.
    if (preserveFileDate && !Files.isSymbolicLink(srcPath) && !setTimes(srcFile, destFile)) {
      throw new IOException("Cannot set the file time.");
    }
  }

  private static void requireCanonicalPathsNotEquals(final File file1, final File file2)
      throws IOException {
    final String canonicalPath = file1.getCanonicalPath();
    if (canonicalPath.equals(file2.getCanonicalPath())) {
      throw new IllegalArgumentException(
          String.format(
              "File canonical paths are equal: '%s' (file1='%s', file2='%s')",
              canonicalPath, file1, file2));
    }
  }

  public static File createParentDirectories(final File file) throws IOException {
    return mkdirs(getParentFile(file));
  }

  private static File mkdirs(final File directory) throws IOException {
    if (directory != null && !directory.mkdirs() && !directory.isDirectory()) {
      throw new IOException("Cannot create directory '" + directory + "'.");
    }
    return directory;
  }

  private static File getParentFile(final File file) {
    return file == null ? null : file.getParentFile();
  }

  private static boolean setTimes(final File sourceFile, final File targetFile) {
    Objects.requireNonNull(sourceFile, "sourceFile");
    Objects.requireNonNull(targetFile, "targetFile");
    try {
      // Set creation, modified, last accessed to match source file
      final BasicFileAttributes srcAttr =
          Files.readAttributes(sourceFile.toPath(), BasicFileAttributes.class);
      final BasicFileAttributeView destAttrView =
          Files.getFileAttributeView(targetFile.toPath(), BasicFileAttributeView.class);
      // null guards are not needed; BasicFileAttributes.setTimes(...) is null safe
      destAttrView.setTimes(
          srcAttr.lastModifiedTime(), srcAttr.lastAccessTime(), srcAttr.creationTime());
      return true;
    } catch (final IOException ignored) {
      // Fallback: Only set modified time to match source file
      return targetFile.setLastModified(sourceFile.lastModified());
    }
  }

  public static boolean deleteQuietly(final File file) {
    if (file == null) {
      return false;
    }
    try {
      if (file.isDirectory()) {
        cleanDirectory(file);
      }
    } catch (final Exception ignored) {
      // ignore
    }

    try {
      return file.delete();
    } catch (final Exception ignored) {
      return false;
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IoTDB
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * The empty String {@code ""}.
   *
   * @since 2.0
   */
  public static final String EMPTY = "";

  /**
   * Writes a CharSequence to a file creating the file if it does not exist.
   *
   * @param file the file to write
   * @param data the content to write to the file
   * @param charset the charset to use, {@code null} means platform default
   * @param append if {@code true}, then the data will be added to the end of the file rather than
   *     overwriting
   * @throws IOException in case of an I/O error
   * @since 2.3
   */
  public static void write(
      final File file, final CharSequence data, final Charset charset, final boolean append)
      throws IOException {
    writeStringToFile(file, Objects.toString(data, null), charset, append);
  }

  /**
   * Writes a String to a file creating the file if it does not exist.
   *
   * @param file the file to write
   * @param data the content to write to the file
   * @param charset the charset to use, {@code null} means platform default
   * @param append if {@code true}, then the String will be added to the end of the file rather than
   *     overwriting
   * @throws IOException in case of an I/O error
   * @since 2.3
   */
  public static void writeStringToFile(
      final File file, final String data, final Charset charset, final boolean append)
      throws IOException {
    try (OutputStream out = newOutputStream(file, append)) {
      IOUtils.write(data, out, charset);
    }
  }

  /**
   * Creates a new OutputStream by opening or creating a file, returning an output stream that may
   * be used to write bytes to the file.
   *
   * @param append Whether or not to append.
   * @param file the File.
   * @return a new OutputStream.
   * @throws IOException if an I/O error occurs.
   * @see PathUtils#newOutputStream(Path, boolean)
   * @since 2.12.0
   */
  public static OutputStream newOutputStream(final File file, final boolean append)
      throws IOException {
    return PathUtils.newOutputStream(Objects.requireNonNull(file, "file").toPath(), append);
  }

  /**
   * Moves a file preserving attributes.
   *
   * <p>Shorthand for {@code moveFile(srcFile, destFile, StandardCopyOption.COPY_ATTRIBUTES)}.
   *
   * <p>When the destination file is on another file system, do a "copy and delete".
   *
   * @param srcFile the file to be moved.
   * @param destFile the destination file.
   * @throws NullPointerException if any of the given {@link File}s are {@code null}.
   * @throws FileExistsException if the destination file exists.
   * @throws FileNotFoundException if the source file does not exist.
   * @throws IOException if source or destination is invalid.
   * @throws IOException if an error occurs.
   * @since 1.4
   */
  public static void moveFile(final File srcFile, final File destFile) throws IOException {
    moveFile(srcFile, destFile, StandardCopyOption.COPY_ATTRIBUTES);
  }

  /**
   * Copies bytes from the URL {@code source} to a file {@code destination}. The directories up to
   * {@code destination} will be created if they don't already exist. {@code destination} will be
   * overwritten if it already exists.
   *
   * <p>Warning: this method does not set a connection or read timeout and thus might block forever.
   * Use {@link #copyURLToFile(URL, File, int, int)} with reasonable timeouts to prevent this.
   *
   * @param source the {@link URL} to copy bytes from, must not be {@code null}
   * @param destination the non-directory {@link File} to write bytes to (possibly overwriting),
   *     must not be {@code null}
   * @throws IOException if {@code source} URL cannot be opened
   * @throws IOException if {@code destination} is a directory
   * @throws IOException if {@code destination} cannot be written
   * @throws IOException if {@code destination} needs creating but can't be
   * @throws IOException if an IO error occurs during copying
   */
  public static void copyURLToFile(final URL source, final File destination) throws IOException {
    final Path path = destination.toPath();
    PathUtils.createParentDirectories(path);
    PathUtils.copy(source::openStream, path, StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Moves a file to a directory.
   *
   * <p>If {@code createDestDir} is true, creates all destination parent directories, including any
   * necessary but non-existent parent directories.
   *
   * @param srcFile the file to be moved.
   * @param destDir the destination file.
   * @param createDestDir If {@code true} create the destination directory, otherwise if {@code
   *     false} throw an IOException.
   * @throws NullPointerException if any of the given {@link File}s are {@code null}.
   * @throws FileExistsException if the destination file exists.
   * @throws FileNotFoundException if the source file does not exist.
   * @throws IOException if source or destination is invalid.
   * @throws IOException if the directory was not created along with all its parent directories, if
   *     enabled.
   * @throws IOException if an error occurs or setting the last-modified time didn't succeed.
   * @throws SecurityException See {@link File#mkdirs()}.
   * @since 1.4
   */
  public static void moveFileToDirectory(
      final File srcFile, final File destDir, final boolean createDestDir) throws IOException {
    validateMoveParameters(srcFile, destDir);
    if (!destDir.exists() && createDestDir) {
      mkdirs(destDir);
    }
    requireExistsChecked(destDir, "destDir");
    requireDirectory(destDir, "destDir");
    moveFile(srcFile, new File(destDir, srcFile.getName()));
  }

  /**
   * Validates the given arguments.
   *
   * <ul>
   *   <li>Throws {@link NullPointerException} if {@code source} is null
   *   <li>Throws {@link NullPointerException} if {@code destination} is null
   *   <li>Throws {@link FileNotFoundException} if {@code source} does not exist
   * </ul>
   *
   * @param source the file or directory to be moved.
   * @param destination the destination file or directory.
   * @throws NullPointerException if any of the given {@link File}s are {@code null}.
   * @throws FileNotFoundException if the source file does not exist.
   */
  private static void validateMoveParameters(final File source, final File destination)
      throws FileNotFoundException {
    Objects.requireNonNull(source, "source");
    Objects.requireNonNull(destination, "destination");
    if (!source.exists()) {
      throw new FileNotFoundException("Source '" + source + "' does not exist");
    }
  }

  /**
   * Requires that the given {@link File} exists and throws an {@link FileNotFoundException} if it
   * doesn't.
   *
   * @param file The {@link File} to check.
   * @param fileParamName The parameter name to use in the exception message in case of {@code null}
   *     input.
   * @return the given file.
   * @throws NullPointerException if the given {@link File} is {@code null}.
   * @throws FileNotFoundException if the given {@link File} does not exist.
   */
  private static File requireExistsChecked(final File file, final String fileParamName)
      throws FileNotFoundException {
    Objects.requireNonNull(file, fileParamName);
    if (!file.exists()) {
      throw new FileNotFoundException(
          "File system element for parameter '"
              + fileParamName
              + "' does not exist: '"
              + file
              + "'");
    }
    return file;
  }

  /**
   * Requires that the given {@link File} is a directory.
   *
   * @param directory The {@link File} to check.
   * @param name The parameter name to use in the exception message in case of null input or if the
   *     file is not a directory.
   * @return the given directory.
   * @throws NullPointerException if the given {@link File} is {@code null}.
   * @throws IllegalArgumentException if the given {@link File} does not exist or is not a
   *     directory.
   */
  private static File requireDirectory(final File directory, final String name) {
    Objects.requireNonNull(directory, name);
    if (!directory.isDirectory()) {
      throw new IllegalArgumentException(
          "Parameter '" + name + "' is not a directory: '" + directory + "'");
    }
    return directory;
  }

  /**
   * Finds files within a given directory (and optionally its subdirectories) which match an array
   * of extensions.
   *
   * @param directory the directory to search in
   * @param extensions an array of extensions, ex. {"java","xml"}. If this parameter is {@code
   *     null}, all files are returned.
   * @param recursive if true all subdirectories are searched as well
   * @return a collection of java.io.File with the matching files
   */
  public static Collection<File> listFiles(
      final File directory, final String[] extensions, final boolean recursive) {
    return Uncheck.apply(d -> toList(streamFiles(d, recursive, extensions)), directory);
  }

  private static List<File> toList(final Stream<File> stream) {
    return stream.collect(Collectors.toList());
  }

  /**
   * Streams over the files in a given directory (and optionally its subdirectories) which match an
   * array of extensions.
   *
   * @param directory the directory to search in
   * @param recursive if true all subdirectories are searched as well
   * @param extensions an array of extensions, ex. {"java","xml"}. If this parameter is {@code
   *     null}, all files are returned.
   * @return an iterator of java.io.File with the matching files
   * @since 2.9.0
   */
  public static Stream<File> streamFiles(
      final File directory, final boolean recursive, final String... extensions)
      throws IOException {
    // @formatter:off
    final IOFileFilter filter =
        extensions == null
            ? FileFileFilter.INSTANCE
            : FileFileFilter.INSTANCE.and(new SuffixFileFilter(toSuffixes(extensions)));
    // @formatter:on
    return PathUtils.walk(
            directory.toPath(), filter, toMaxDepth(recursive), false, FileVisitOption.FOLLOW_LINKS)
        .map(Path::toFile);
  }

  /**
   * Converts an array of file extensions to suffixes.
   *
   * @param extensions an array of extensions. Format: {"java", "xml"}
   * @return an array of suffixes. Format: {".java", ".xml"}
   * @throws NullPointerException if the parameter is null
   */
  private static String[] toSuffixes(final String... extensions) {
    return Stream.of(Objects.requireNonNull(extensions, "extensions"))
        .map(e -> "." + e)
        .toArray(String[]::new);
  }

  /**
   * Converts whether or not to recurse into a recursion max depth.
   *
   * @param recursive whether or not to recurse
   * @return the recursion depth
   */
  private static int toMaxDepth(final boolean recursive) {
    return recursive ? Integer.MAX_VALUE : 1;
  }

  /**
   * Converts each of an array of {@link File} to a {@link URL}.
   *
   * <p>Returns an array of the same size as the input.
   *
   * @param files the files to convert, must not be {@code null}
   * @return an array of URLs matching the input
   * @throws IOException if a file cannot be converted
   * @throws NullPointerException if the parameter is null
   */
  public static URL[] toURLs(final File... files) throws IOException {
    Objects.requireNonNull(files, "files");
    final URL[] urls = new URL[files.length];
    for (int i = 0; i < urls.length; i++) {
      urls[i] = files[i].toURI().toURL();
    }
    return urls;
  }

  public static void moveDirectory(final File srcDir, final File destDir) throws IOException {
    validateMoveParameters(srcDir, destDir);
    requireDirectory(srcDir, "srcDir");
    requireAbsent(destDir, "destDir");
    if (!srcDir.renameTo(destDir)) {
      if (destDir.getCanonicalPath().startsWith(srcDir.getCanonicalPath() + File.separator)) {
        throw new IOException(
            "Cannot move directory: " + srcDir + " to a subdirectory of itself: " + destDir);
      }
      copyDirectory(srcDir, destDir);
      deleteDirectory(srcDir);
      if (srcDir.exists()) {
        throw new IOException(
            "Failed to delete original directory '" + srcDir + "' after copy to '" + destDir + "'");
      }
    }
  }

  /**
   * Copies a whole directory to a new location preserving the file dates.
   *
   * <p>This method copies the specified directory and all its child directories and files to the
   * specified destination. The destination is the new location and name of the directory.
   *
   * <p>The destination directory is created if it does not exist. If the destination directory did
   * exist, then this method merges the source with the destination, with the source taking
   * precedence.
   *
   * <p><strong>Note:</strong> Setting {@code preserveFileDate} to {@code true} tries to preserve
   * the file's last modified date/times using {@link BasicFileAttributeView#setTimes(FileTime,
   * FileTime, FileTime)}, however it is not guaranteed that the operation will succeed. If the
   * modification operation fails it will fallback to {@link File#setLastModified(long)} and if that
   * fails, the methods throws IOException.
   *
   * @param srcDir an existing directory to copy, must not be {@code null}.
   * @param destDir the new directory, must not be {@code null}.
   * @throws NullPointerException if any of the given {@link File}s are {@code null}.
   * @throws IllegalArgumentException if the source or destination is invalid.
   * @throws FileNotFoundException if the source does not exist.
   * @throws IOException if an error occurs or setting the last-modified time didn't succeed.
   * @since 1.1
   */
  public static void copyDirectory(final File srcDir, final File destDir) throws IOException {
    copyDirectory(srcDir, destDir, true);
  }

  /**
   * Copies a whole directory to a new location.
   *
   * <p>This method copies the contents of the specified source directory to within the specified
   * destination directory.
   *
   * <p>The destination directory is created if it does not exist. If the destination directory did
   * exist, then this method merges the source with the destination, with the source taking
   * precedence.
   *
   * <p><strong>Note:</strong> Setting {@code preserveFileDate} to {@code true} tries to preserve
   * the files' last modified date/times using {@link File#setLastModified(long)}, however it is not
   * guaranteed that those operations will succeed. If the modification operation fails, the methods
   * throws IOException.
   *
   * @param srcDir an existing directory to copy, must not be {@code null}.
   * @param destDir the new directory, must not be {@code null}.
   * @param preserveFileDate true if the file date of the copy should be the same as the original.
   * @throws NullPointerException if any of the given {@link File}s are {@code null}.
   * @throws IllegalArgumentException if the source or destination is invalid.
   * @throws FileNotFoundException if the source does not exist.
   * @throws IOException if an error occurs or setting the last-modified time didn't succeed.
   * @since 1.1
   */
  public static void copyDirectory(
      final File srcDir, final File destDir, final boolean preserveFileDate) throws IOException {
    copyDirectory(srcDir, destDir, null, preserveFileDate);
  }

  /**
   * Copies a filtered directory to a new location.
   *
   * <p>This method copies the contents of the specified source directory to within the specified
   * destination directory.
   *
   * <p>The destination directory is created if it does not exist. If the destination directory did
   * exist, then this method merges the source with the destination, with the source taking
   * precedence.
   *
   * <p><strong>Note:</strong> Setting {@code preserveFileDate} to {@code true} tries to preserve
   * the file's last modified date/times using {@link BasicFileAttributeView#setTimes(FileTime,
   * FileTime, FileTime)}, however it is not guaranteed that the operation will succeed. If the
   * modification operation fails it will fallback to {@link File#setLastModified(long)} and if that
   * fails, the methods throws IOException. <b>Example: Copy directories only</b>
   *
   * <pre>
   * // only copy the directory structure
   * FileUtils.copyDirectory(srcDir, destDir, DirectoryFileFilter.DIRECTORY, false);
   * </pre>
   *
   * <b>Example: Copy directories and txt files</b>
   *
   * <pre>
   * // Create a filter for ".txt" files
   * IOFileFilter txtSuffixFilter = FileFilterUtils.suffixFileFilter(".txt");
   * IOFileFilter txtFiles = FileFilterUtils.andFileFilter(FileFileFilter.FILE, txtSuffixFilter);
   *
   * // Create a filter for either directories or ".txt" files
   * FileFilter filter = FileFilterUtils.orFileFilter(DirectoryFileFilter.DIRECTORY, txtFiles);
   *
   * // Copy using the filter
   * FileUtils.copyDirectory(srcDir, destDir, filter, false);
   * </pre>
   *
   * @param srcDir an existing directory to copy, must not be {@code null}.
   * @param destDir the new directory, must not be {@code null}.
   * @param filter the filter to apply, null means copy all directories and files.
   * @param preserveFileDate true if the file date of the copy should be the same as the original.
   * @throws NullPointerException if any of the given {@link File}s are {@code null}.
   * @throws IllegalArgumentException if the source or destination is invalid.
   * @throws FileNotFoundException if the source does not exist.
   * @throws IOException if an error occurs or setting the last-modified time didn't succeed.
   * @since 1.4
   */
  public static void copyDirectory(
      final File srcDir,
      final File destDir,
      final FileFilter filter,
      final boolean preserveFileDate)
      throws IOException {
    copyDirectory(srcDir, destDir, filter, preserveFileDate, StandardCopyOption.REPLACE_EXISTING);
  }

  /**
   * Copies a filtered directory to a new location.
   *
   * <p>This method copies the contents of the specified source directory to within the specified
   * destination directory.
   *
   * <p>The destination directory is created if it does not exist. If the destination directory did
   * exist, then this method merges the source with the destination, with the source taking
   * precedence.
   *
   * <p><strong>Note:</strong> Setting {@code preserveFileDate} to {@code true} tries to preserve
   * the file's last modified date/times using {@link BasicFileAttributeView#setTimes(FileTime,
   * FileTime, FileTime)}, however it is not guaranteed that the operation will succeed. If the
   * modification operation fails it will fallback to {@link File#setLastModified(long)} and if that
   * fails, the methods throws IOException. <b>Example: Copy directories only</b>
   *
   * <pre>
   * // only copy the directory structure
   * FileUtils.copyDirectory(srcDir, destDir, DirectoryFileFilter.DIRECTORY, false);
   * </pre>
   *
   * <b>Example: Copy directories and txt files</b>
   *
   * <pre>
   * // Create a filter for ".txt" files
   * IOFileFilter txtSuffixFilter = FileFilterUtils.suffixFileFilter(".txt");
   * IOFileFilter txtFiles = FileFilterUtils.andFileFilter(FileFileFilter.FILE, txtSuffixFilter);
   *
   * // Create a filter for either directories or ".txt" files
   * FileFilter filter = FileFilterUtils.orFileFilter(DirectoryFileFilter.DIRECTORY, txtFiles);
   *
   * // Copy using the filter
   * FileUtils.copyDirectory(srcDir, destDir, filter, false);
   * </pre>
   *
   * @param srcDir an existing directory to copy, must not be {@code null}
   * @param destDir the new directory, must not be {@code null}
   * @param fileFilter the filter to apply, null means copy all directories and files
   * @param preserveFileDate true if the file date of the copy should be the same as the original
   * @param copyOptions options specifying how the copy should be done, for example {@link
   *     StandardCopyOption}.
   * @throws NullPointerException if any of the given {@link File}s are {@code null}.
   * @throws IllegalArgumentException if the source or destination is invalid.
   * @throws FileNotFoundException if the source does not exist.
   * @throws IOException if an error occurs or setting the last-modified time didn't succeed.
   * @since 2.8.0
   */
  public static void copyDirectory(
      final File srcDir,
      final File destDir,
      final FileFilter fileFilter,
      final boolean preserveFileDate,
      final CopyOption... copyOptions)
      throws IOException {
    requireFileCopy(srcDir, destDir);
    requireDirectory(srcDir, "srcDir");
    requireCanonicalPathsNotEquals(srcDir, destDir);

    // Cater for destination being directory within the source directory (see IO-141)
    List<String> exclusionList = null;
    final String srcDirCanonicalPath = srcDir.getCanonicalPath();
    final String destDirCanonicalPath = destDir.getCanonicalPath();
    if (destDirCanonicalPath.startsWith(srcDirCanonicalPath)) {
      final File[] srcFiles = listFiles(srcDir, fileFilter);
      if (srcFiles.length > 0) {
        exclusionList = new ArrayList<>(srcFiles.length);
        for (final File srcFile : srcFiles) {
          exclusionList.add(new File(destDir, srcFile.getName()).getCanonicalPath());
        }
      }
    }
    doCopyDirectory(srcDir, destDir, fileFilter, exclusionList, preserveFileDate, copyOptions);
  }

  /**
   * Requires parameter attributes for a file copy operation.
   *
   * @param source the source file
   * @param destination the destination
   * @throws NullPointerException if any of the given {@link File}s are {@code null}.
   * @throws FileNotFoundException if the source does not exist.
   */
  private static void requireFileCopy(final File source, final File destination)
      throws FileNotFoundException {
    requireExistsChecked(source, "source");
    Objects.requireNonNull(destination, "destination");
  }

  /**
   * Internal copy directory method. Creates all destination parent directories, including any
   * necessary but non-existent parent directories.
   *
   * @param srcDir the validated source directory, must not be {@code null}.
   * @param destDir the validated destination directory, must not be {@code null}.
   * @param fileFilter the filter to apply, null means copy all directories and files.
   * @param exclusionList List of files and directories to exclude from the copy, may be null.
   * @param preserveDirDate preserve the directories last modified dates.
   * @param copyOptions options specifying how the copy should be done, see {@link
   *     StandardCopyOption}.
   * @throws IOException if the directory was not created along with all its parent directories.
   * @throws SecurityException See {@link File#mkdirs()}.
   */
  private static void doCopyDirectory(
      final File srcDir,
      final File destDir,
      final FileFilter fileFilter,
      final List<String> exclusionList,
      final boolean preserveDirDate,
      final CopyOption... copyOptions)
      throws IOException {
    // recurse dirs, copy files.
    final File[] srcFiles = listFiles(srcDir, fileFilter);
    requireDirectoryIfExists(destDir, "destDir");
    mkdirs(destDir);
    requireCanWrite(destDir, "destDir");
    for (final File srcFile : srcFiles) {
      final File dstFile = new File(destDir, srcFile.getName());
      if (exclusionList == null || !exclusionList.contains(srcFile.getCanonicalPath())) {
        if (srcFile.isDirectory()) {
          doCopyDirectory(
              srcFile, dstFile, fileFilter, exclusionList, preserveDirDate, copyOptions);
        } else {
          copyFile(srcFile, dstFile, preserveDirDate, copyOptions);
        }
      }
    }
    // Do this last, as the above has probably affected directory metadata
    if (preserveDirDate) {
      setTimes(srcDir, destDir);
    }
  }

  /**
   * Throws an {@link IllegalArgumentException} if the file is not writable. This provides a more
   * precise exception message than a plain access denied.
   *
   * @param file The file to test.
   * @param name The parameter name to use in the exception message.
   * @throws NullPointerException if the given {@link File} is {@code null}.
   * @throws IllegalArgumentException if the file is not writable.
   */
  private static void requireCanWrite(final File file, final String name) {
    Objects.requireNonNull(file, "file");
    if (!file.canWrite()) {
      throw new IllegalArgumentException(
          "File parameter '" + name + " is not writable: '" + file + "'");
    }
  }

  /**
   * Requires that the given {@link File} is a directory if it exists.
   *
   * @param directory The {@link File} to check.
   * @param name The parameter name to use in the exception message in case of null input.
   * @return the given directory.
   * @throws NullPointerException if the given {@link File} is {@code null}.
   * @throws IllegalArgumentException if the given {@link File} exists but is not a directory.
   */
  private static File requireDirectoryIfExists(final File directory, final String name) {
    Objects.requireNonNull(directory, name);
    if (directory.exists()) {
      requireDirectory(directory, name);
    }
    return directory;
  }

  /**
   * Tests whether the specified {@link File} is a directory or not. Implemented as a null-safe
   * delegate to {@link Files#isDirectory(Path path, LinkOption... options)}.
   *
   * @param file the path to the file.
   * @param options options indicating how symbolic links are handled
   * @return {@code true} if the file is a directory; {@code false} if the path is null, the file
   *     does not exist, is not a directory, or it cannot be determined if the file is a directory
   *     or not.
   * @throws SecurityException In the case of the default provider, and a security manager is
   *     installed, the {@link SecurityManager#checkRead(String) checkRead} method is invoked to
   *     check read access to the directory.
   * @since 2.9.0
   */
  public static boolean isDirectory(final File file, final LinkOption... options) {
    return file != null && Files.isDirectory(file.toPath(), options);
  }

  /**
   * Reads the contents of a file into a String. The file is always closed.
   *
   * @param file the file to read, must not be {@code null}
   * @param charsetName the name of the requested charset, {@code null} means platform default
   * @return the file contents, never {@code null}
   * @throws NullPointerException if file is {@code null}.
   * @throws FileNotFoundException if the file does not exist, is a directory rather than a regular
   *     file, or for some other reason cannot be opened for reading.
   * @throws IOException if an I/O error occurs.
   * @since 2.3
   */
  public static String readFileToString(final File file, final Charset charsetName)
      throws IOException {
    return IOUtils.toString(
        () -> Files.newInputStream(file.toPath()), Charsets.toCharset(charsetName));
  }
}
