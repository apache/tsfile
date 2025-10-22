/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.external.commons.io.file;

import org.apache.tsfile.external.commons.io.function.IOFunction;
import org.apache.tsfile.external.commons.io.function.IOSupplier;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.FileVisitor;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.DosFileAttributeView;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFileAttributes;
import java.nio.file.attribute.PosixFilePermission;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Stream;

public class PathUtils {
  public static final LinkOption[] NOFOLLOW_LINK_OPTION_ARRAY = {LinkOption.NOFOLLOW_LINKS};

  /** Empty {@link OpenOption} array. */
  public static final OpenOption[] EMPTY_OPEN_OPTION_ARRAY = {};

  public static final LinkOption[] EMPTY_LINK_OPTION_ARRAY = {};
  public static final DeleteOption[] EMPTY_DELETE_OPTION_ARRAY = {};

  public static Counters.PathCounters delete(
      final Path path, final LinkOption[] linkOptions, final DeleteOption... deleteOptions)
      throws IOException {
    // File deletion through Files deletes links, not targets, so use LinkOption.NOFOLLOW_LINKS.
    return Files.isDirectory(path, linkOptions)
        ? deleteDirectory(path, linkOptions, deleteOptions)
        : deleteFile(path, linkOptions, deleteOptions);
  }

  public static LinkOption[] noFollowLinkOptionArray() {
    return NOFOLLOW_LINK_OPTION_ARRAY.clone();
  }

  public static Counters.PathCounters deleteFile(
      final Path file, final LinkOption[] linkOptions, final DeleteOption... deleteOptions)
      throws NoSuchFileException, IOException {
    //
    // TODO Needs clean up
    //
    if (Files.isDirectory(file, linkOptions)) {
      throw new NoSuchFileException(file.toString());
    }
    final Counters.PathCounters pathCounts = Counters.longPathCounters();
    boolean exists = exists(file, linkOptions);
    long size = exists && !Files.isSymbolicLink(file) ? Files.size(file) : 0;
    try {
      if (Files.deleteIfExists(file)) {
        pathCounts.getFileCounter().increment();
        pathCounts.getByteCounter().add(size);
        return pathCounts;
      }
    } catch (final AccessDeniedException ignored) {
      // Ignore and try again below.
    }
    final Path parent = getParent(file);
    PosixFileAttributes posixFileAttributes = null;
    try {
      if (overrideReadOnly(deleteOptions)) {
        posixFileAttributes = readPosixFileAttributes(parent, linkOptions);
        setReadOnly(file, false, linkOptions);
      }
      // Read size _after_ having read/execute access on POSIX.
      exists = exists(file, linkOptions);
      size = exists && !Files.isSymbolicLink(file) ? Files.size(file) : 0;
      if (Files.deleteIfExists(file)) {
        pathCounts.getFileCounter().increment();
        pathCounts.getByteCounter().add(size);
      }
    } finally {
      if (posixFileAttributes != null) {
        Files.setPosixFilePermissions(parent, posixFileAttributes.permissions());
      }
    }
    return pathCounts;
  }

  public static Path setReadOnly(
      final Path path, final boolean readOnly, final LinkOption... linkOptions) throws IOException {
    try {
      // Windows is simplest
      if (setDosReadOnly(path, readOnly, linkOptions)) {
        return path;
      }
    } catch (final IOException ignored) {
      // Retry with POSIX below.
    }
    final Path parent = getParent(path);
    if (!isPosix(
        parent, linkOptions)) { // Test parent because we may not the permissions to test the file.
      throw new IOException(
          String.format(
              "DOS or POSIX file operations not available for '%s', linkOptions %s",
              path, Arrays.toString(linkOptions)));
    }
    // POSIX
    if (readOnly) {
      // RO
      // File, then parent dir (if any).
      setPosixReadOnlyFile(path, readOnly, linkOptions);
      setPosixDeletePermissions(parent, false, linkOptions);
    } else {
      // RE
      // Parent dir (if any), then file.
      setPosixDeletePermissions(parent, true, linkOptions);
    }
    return path;
  }

  private static boolean setPosixDeletePermissions(
      final Path parent, final boolean enableDeleteChildren, final LinkOption... linkOptions)
      throws IOException {
    // To delete a file in POSIX, you need write and execute permissions on its parent directory.
    // @formatter:off
    return setPosixPermissions(
        parent,
        enableDeleteChildren,
        Arrays.asList(
            PosixFilePermission.OWNER_WRITE,
            // PosixFilePermission.GROUP_WRITE,
            // PosixFilePermission.OTHERS_WRITE,
            PosixFilePermission.OWNER_EXECUTE
            // PosixFilePermission.GROUP_EXECUTE,
            // PosixFilePermission.OTHERS_EXECUTE
            ),
        linkOptions);
    // @formatter:on
  }

  private static boolean setPosixPermissions(
      final Path path,
      final boolean addPermissions,
      final List<PosixFilePermission> updatePermissions,
      final LinkOption... linkOptions)
      throws IOException {
    if (path != null) {
      final Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path, linkOptions);
      final Set<PosixFilePermission> newPermissions = new HashSet<>(permissions);
      if (addPermissions) {
        newPermissions.addAll(updatePermissions);
      } else {
        newPermissions.removeAll(updatePermissions);
      }
      if (!newPermissions.equals(permissions)) {
        Files.setPosixFilePermissions(path, newPermissions);
      }
      return true;
    }
    return false;
  }

  private static void setPosixReadOnlyFile(
      final Path path, final boolean readOnly, final LinkOption... linkOptions) throws IOException {
    // Not Windows 10
    final Set<PosixFilePermission> permissions = Files.getPosixFilePermissions(path, linkOptions);
    // @formatter:off
    final List<PosixFilePermission> readPermissions =
        Arrays.asList(
            PosixFilePermission.OWNER_READ
            // PosixFilePermission.GROUP_READ,
            // PosixFilePermission.OTHERS_READ
            );
    final List<PosixFilePermission> writePermissions =
        Arrays.asList(
            PosixFilePermission.OWNER_WRITE
            // PosixFilePermission.GROUP_WRITE,
            // PosixFilePermission.OTHERS_WRITE
            );
    // @formatter:on
    if (readOnly) {
      // RO: We can read, we cannot write.
      permissions.addAll(readPermissions);
      permissions.removeAll(writePermissions);
    } else {
      // Not RO: We can read, we can write.
      permissions.addAll(readPermissions);
      permissions.addAll(writePermissions);
    }
    Files.setPosixFilePermissions(path, permissions);
  }

  public static boolean isPosix(final Path test, final LinkOption... options) {
    return exists(test, options) && readPosixFileAttributes(test, options) != null;
  }

  private static boolean exists(final Path path, final LinkOption... options) {
    return path != null && (options != null ? Files.exists(path, options) : Files.exists(path));
  }

  public static PosixFileAttributes readPosixFileAttributes(
      final Path path, final LinkOption... options) {
    return readAttributes(path, PosixFileAttributes.class, options);
  }

  private static boolean setDosReadOnly(
      final Path path, final boolean readOnly, final LinkOption... linkOptions) throws IOException {
    final DosFileAttributeView dosFileAttributeView = getDosFileAttributeView(path, linkOptions);
    if (dosFileAttributeView != null) {
      dosFileAttributeView.setReadOnly(readOnly);
      return true;
    }
    return false;
  }

  public static <A extends BasicFileAttributes> A readAttributes(
      final Path path, final Class<A> type, final LinkOption... options) {
    try {
      return path == null ? null : Files.readAttributes(path, type, options);
    } catch (final UnsupportedOperationException | IOException e) {
      // For example, on Windows.
      return null;
    }
  }

  public static DosFileAttributeView getDosFileAttributeView(
      final Path path, final LinkOption... options) {
    return Files.getFileAttributeView(path, DosFileAttributeView.class, options);
  }

  public static Counters.PathCounters deleteDirectory(
      final Path directory, final LinkOption[] linkOptions, final DeleteOption... deleteOptions)
      throws IOException {
    return visitFileTree(
            new DeletingPathVisitor(Counters.longPathCounters(), linkOptions, deleteOptions),
            directory)
        .getPathCounters();
  }

  public static <T extends FileVisitor<? super Path>> T visitFileTree(
      final T visitor, final Path directory) throws IOException {
    Files.walkFileTree(directory, visitor);
    return visitor;
  }

  private static Path getParent(final Path path) {
    return path == null ? null : path.getParent();
  }

  private static boolean overrideReadOnly(final DeleteOption... deleteOptions) {
    if (deleteOptions == null) {
      return false;
    }
    return Stream.of(deleteOptions).anyMatch(e -> e == StandardDeleteOption.OVERRIDE_READ_ONLY);
  }

  public static String getFileNameString(final Path path) {
    return getFileName(path, Path::toString);
  }

  public static <R> R getFileName(final Path path, final Function<Path, R> function) {
    final Path fileName = path != null ? path.getFileName() : null;
    return fileName != null ? function.apply(fileName) : null;
  }

  public static boolean isEmptyDirectory(final Path directory) throws IOException {
    try (DirectoryStream<Path> directoryStream = Files.newDirectoryStream(directory)) {
      return !directoryStream.iterator().hasNext();
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IoTDB
  /////////////////////////////////////////////////////////////////////////////////////////////////

  /**
   * A LinkOption used to follow link in this class, the inverse of {@link
   * LinkOption#NOFOLLOW_LINKS}.
   *
   * @since 2.12.0
   */
  static final LinkOption NULL_LINK_OPTION = null;

  private static final OpenOption[] OPEN_OPTIONS_TRUNCATE = {
    StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING
  };

  private static final OpenOption[] OPEN_OPTIONS_APPEND = {
    StandardOpenOption.CREATE, StandardOpenOption.APPEND
  };

  /**
   * Deletes a directory including subdirectories.
   *
   * @param directory directory to delete.
   * @return The visitor used to delete the given directory.
   * @throws IOException if an I/O error is thrown by a visitor method.
   */
  public static Counters.PathCounters deleteDirectory(final Path directory) throws IOException {
    return deleteDirectory(directory, EMPTY_DELETE_OPTION_ARRAY);
  }

  /**
   * Deletes a directory including subdirectories.
   *
   * @param directory directory to delete.
   * @param deleteOptions How to handle deletion.
   * @return The visitor used to delete the given directory.
   * @throws IOException if an I/O error is thrown by a visitor method.
   * @since 2.8.0
   */
  public static Counters.PathCounters deleteDirectory(
      final Path directory, final DeleteOption... deleteOptions) throws IOException {
    final LinkOption[] linkOptions = PathUtils.noFollowLinkOptionArray();
    // POSIX ops will noop on non-POSIX.
    return withPosixFileAttributes(
        getParent(directory),
        linkOptions,
        overrideReadOnly(deleteOptions),
        pfa ->
            visitFileTree(
                    new DeletingPathVisitor(
                        Counters.longPathCounters(), linkOptions, deleteOptions),
                    directory)
                .getPathCounters());
  }

  private static <R> R withPosixFileAttributes(
      final Path path,
      final LinkOption[] linkOptions,
      final boolean overrideReadOnly,
      final IOFunction<PosixFileAttributes, R> function)
      throws IOException {
    final PosixFileAttributes posixFileAttributes =
        overrideReadOnly ? readPosixFileAttributes(path, linkOptions) : null;
    try {
      return function.apply(posixFileAttributes);
    } finally {
      if (posixFileAttributes != null && path != null && Files.exists(path, linkOptions)) {
        Files.setPosixFilePermissions(path, posixFileAttributes.permissions());
      }
    }
  }

  /**
   * Creates a new OutputStream by opening or creating a file, returning an output stream that may
   * be used to write bytes to the file.
   *
   * @param path the Path.
   * @param append Whether or not to append.
   * @return a new OutputStream.
   * @throws IOException if an I/O error occurs.
   * @see Files#newOutputStream(Path, OpenOption...)
   * @since 2.12.0
   */
  public static OutputStream newOutputStream(final Path path, final boolean append)
      throws IOException {
    return newOutputStream(
        path, EMPTY_LINK_OPTION_ARRAY, append ? OPEN_OPTIONS_APPEND : OPEN_OPTIONS_TRUNCATE);
  }

  static OutputStream newOutputStream(
      final Path path, final LinkOption[] linkOptions, final OpenOption... openOptions)
      throws IOException {
    if (!exists(path, linkOptions)) {
      createParentDirectories(
          path, linkOptions != null && linkOptions.length > 0 ? linkOptions[0] : NULL_LINK_OPTION);
    }
    final List<OpenOption> list =
        new ArrayList<>(Arrays.asList(openOptions != null ? openOptions : EMPTY_OPEN_OPTION_ARRAY));
    list.addAll(Arrays.asList(linkOptions != null ? linkOptions : EMPTY_LINK_OPTION_ARRAY));
    return Files.newOutputStream(path, list.toArray(EMPTY_OPEN_OPTION_ARRAY));
  }

  /**
   * Creates the parent directories for the given {@code path}.
   *
   * <p>If the parent directory already exists, then return it.
   *
   * <p>
   *
   * @param path The path to a file (or directory).
   * @param attrs An optional list of file attributes to set atomically when creating the
   *     directories.
   * @return The Path for the {@code path}'s parent directory or null if the given path has no
   *     parent.
   * @throws IOException if an I/O error occurs.
   * @since 2.9.0
   */
  public static Path createParentDirectories(final Path path, final FileAttribute<?>... attrs)
      throws IOException {
    return createParentDirectories(path, LinkOption.NOFOLLOW_LINKS, attrs);
  }

  /**
   * Creates the parent directories for the given {@code path}.
   *
   * <p>If the parent directory already exists, then return it.
   *
   * <p>
   *
   * @param path The path to a file (or directory).
   * @param linkOption A {@link LinkOption} or null.
   * @param attrs An optional list of file attributes to set atomically when creating the
   *     directories.
   * @return The Path for the {@code path}'s parent directory or null if the given path has no
   *     parent.
   * @throws IOException if an I/O error occurs.
   * @since 2.12.0
   */
  public static Path createParentDirectories(
      final Path path, final LinkOption linkOption, final FileAttribute<?>... attrs)
      throws IOException {
    Path parent = getParent(path);
    parent = linkOption == LinkOption.NOFOLLOW_LINKS ? parent : readIfSymbolicLink(parent);
    if (parent == null) {
      return null;
    }
    final boolean exists =
        linkOption == null ? Files.exists(parent) : Files.exists(parent, linkOption);
    return exists ? parent : Files.createDirectories(parent, attrs);
  }

  private static Path readIfSymbolicLink(final Path path) throws IOException {
    return path != null ? Files.isSymbolicLink(path) ? Files.readSymbolicLink(path) : path : null;
  }

  /**
   * Copies the InputStream from the supplier with {@link Files#copy(InputStream, Path,
   * CopyOption...)}.
   *
   * @param in Supplies the InputStream.
   * @param target See {@link Files#copy(InputStream, Path, CopyOption...)}.
   * @param copyOptions See {@link Files#copy(InputStream, Path, CopyOption...)}.
   * @return See {@link Files#copy(InputStream, Path, CopyOption...)}
   * @throws IOException See {@link Files#copy(InputStream, Path, CopyOption...)}
   * @since 2.12.0
   */
  public static long copy(
      final IOSupplier<InputStream> in, final Path target, final CopyOption... copyOptions)
      throws IOException {
    try (InputStream inputStream = in.get()) {
      return Files.copy(inputStream, target, copyOptions);
    }
  }

  /**
   * Returns a stream of filtered paths.
   *
   * @param start the start path
   * @param pathFilter the path filter
   * @param maxDepth the maximum depth of directories to walk.
   * @param readAttributes whether to call the filters with file attributes (false passes null).
   * @param options the options to configure the walk.
   * @return a filtered stream of paths.
   * @throws IOException if an I/O error is thrown when accessing the starting file.
   * @since 2.9.0
   */
  public static Stream<Path> walk(
      final Path start,
      final PathFilter pathFilter,
      final int maxDepth,
      final boolean readAttributes,
      final FileVisitOption... options)
      throws IOException {
    return Files.walk(start, maxDepth, options)
        .filter(
            path ->
                pathFilter.accept(
                        path, readAttributes ? readBasicFileAttributesUnchecked(path) : null)
                    == FileVisitResult.CONTINUE);
  }

  /**
   * Reads the BasicFileAttributes from the given path. Returns null instead of throwing {@link
   * UnsupportedOperationException}.
   *
   * @param path the path to read.
   * @return the path attributes.
   * @throws UncheckedIOException if an I/O error occurs
   * @since 2.9.0
   * @deprecated Use {@link #readBasicFileAttributes(Path, LinkOption...)}.
   */
  @Deprecated
  public static BasicFileAttributes readBasicFileAttributesUnchecked(final Path path) {
    return readBasicFileAttributes(path, EMPTY_LINK_OPTION_ARRAY);
  }

  /**
   * Reads the BasicFileAttributes from the given path. Returns null instead of throwing {@link
   * UnsupportedOperationException}.
   *
   * @param path the path to read.
   * @param options options indicating how to handle symbolic links.
   * @return the path attributes.
   * @since 2.12.0
   */
  public static BasicFileAttributes readBasicFileAttributes(
      final Path path, final LinkOption... options) {
    return readAttributes(path, BasicFileAttributes.class, options);
  }
}
