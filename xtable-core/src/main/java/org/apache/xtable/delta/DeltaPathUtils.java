/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
 
package org.apache.xtable.delta;

import java.io.UnsupportedEncodingException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import org.apache.hadoop.fs.Path;

import org.apache.xtable.paths.PathUtils;

/**
 * Shared path resolution for Delta log actions. Used by both Delta Standalone and Delta Kernel
 * implementations.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class DeltaPathUtils {

  /**
   * Resolves the path of an AddFile or RemoveFile action against the table base path.
   *
   * <p>Delta allows the path to be relative to the table root or absolute (see PROTOCOL.md). An
   * absolute path (one with a URI scheme, e.g. s3://, hdfs://, file://, or starting with the
   * separator) must be used as-is: concatenating it onto the table base path would point at a
   * non-existent location and silently drop records from the converted table.
   *
   * @param tableBasePath the table base path
   * @param dataFilePath the path recorded in the Delta log (relative or absolute)
   * @return the full absolute path to the file
   */
  public static String getFullPathToFile(String tableBasePath, String dataFilePath) {
    String path = decode(dataFilePath);
    if (isAbsolutePath(path)) {
      return path;
    }
    return tableBasePath.endsWith(Path.SEPARATOR)
        ? tableBasePath + path
        : tableBasePath + Path.SEPARATOR + path;
  }

  /**
   * Returns the path to record in an AddFile or RemoveFile action for a data file.
   *
   * <p>A file under the table base path is recorded relative to it, since relative paths seem more
   * commonly supported by query engines. A file outside the base path, for example one written to a
   * separate data location of an Iceberg source, is recorded with its absolute path, since a
   * relative path cannot point to it.
   *
   * @param tableBasePath the table base path
   * @param dataFilePath the path to the data file, relative or absolute
   * @return the path to record in the Delta log
   */
  public static String getPathForDeltaLog(String tableBasePath, String dataFilePath) {
    return isUnderBasePath(tableBasePath, dataFilePath)
        ? encodeRelative(PathUtils.getRelativePath(dataFilePath, tableBasePath))
        : new Path(dataFilePath).toUri().toString();
  }

  // Paths in the Delta log are URI-encoded (PROTOCOL.md, "Add File and Remove File"). Without the
  // encoding a file under an escaped partition directory, e.g. s=a%2Fb from a value "a/b", is
  // decoded by Delta readers to a directory that does not exist.
  private static String encodeRelative(String relativePath) {
    try {
      return new URI(null, null, relativePath, null).toString();
    } catch (URISyntaxException e) {
      throw new IllegalArgumentException("Cannot encode data file path " + relativePath, e);
    }
  }

  // Decodes the %XX escapes of a Delta log path. "+" is literal in a URI path, so it is kept. A
  // path that is not a valid URI was not encoded by its writer and is used as-is.
  private static String decode(String deltaLogPath) {
    if (deltaLogPath.indexOf('%') < 0) {
      return deltaLogPath;
    }
    try {
      new URI(deltaLogPath);
    } catch (URISyntaxException e) {
      return deltaLogPath;
    }
    try {
      return URLDecoder.decode(deltaLogPath.replace("+", "%2B"), StandardCharsets.UTF_8.name());
    } catch (UnsupportedEncodingException e) {
      throw new IllegalStateException(e);
    }
  }

  private static boolean isUnderBasePath(String tableBasePath, String dataFilePath) {
    if (!dataFilePath.contains(":")) {
      return true;
    }
    URI file = new Path(dataFilePath).toUri();
    // compare without the scheme to handle differences like s3 vs s3a
    URI base = new Path(tableBasePath).toUri();
    return Objects.equals(base.getAuthority(), file.getAuthority())
        && file.getPath().startsWith(base.getPath() + Path.SEPARATOR);
  }

  private static boolean isAbsolutePath(String dataFilePath) {
    return dataFilePath.startsWith(Path.SEPARATOR) || dataFilePath.contains(":");
  }
}
