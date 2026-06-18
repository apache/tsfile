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

package org.apache.tsfile.spark;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.i18n.Messages;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public final class TsFileTableSchemaInferer {

  private TsFileTableSchemaInferer() {}

  public static InferenceResult infer(TsFileTableOptions options) {
    List<String> files = discoverTsFiles(options.path());
    if (files.isEmpty()) {
      throw new TsFileSparkException(
          Messages.format("error.spark.no_tsfile_files", options.path()));
    }

    String selectedTable = options.table();
    boolean inferTable = selectedTable == null;
    TableSchema selectedSchema = null;
    for (String file : files) {
      Map<String, TableSchema> schemaMap = readTableSchemas(file);
      if (inferTable) {
        if (schemaMap.size() != 1) {
          throw new TsFileSparkException(
              Messages.format("error.spark.multiple_tables_requires_table", file));
        }
        if (selectedTable == null) {
          selectedTable = schemaMap.keySet().iterator().next();
        }
      }
      TableSchema current = schemaMap.get(selectedTable);
      if (current == null) {
        throw new TsFileSparkException(
            Messages.format("error.spark.table_not_found_in_file", file, selectedTable));
      }
      if (selectedSchema == null) {
        selectedSchema = current;
      } else {
        validateCompatible(selectedSchema, current, file);
      }
    }

    return new InferenceResult(
        files,
        TsFileTableSchema.fromTableSchema(
            selectedSchema, options.timeColumn(), options.timestampAs()));
  }

  private static Map<String, TableSchema> readTableSchemas(String file) {
    try (TsFileSequenceReader reader = new TsFileSequenceReader(file)) {
      return reader.getTableSchemaMap();
    } catch (IOException e) {
      throw new TsFileSparkException(
          Messages.format("error.spark.read_table_metadata_failed", file), e);
    }
  }

  private static void validateCompatible(
      TableSchema expected, TableSchema actual, String actualFile) {
    if (!expected.getTableName().equals(actual.getTableName())) {
      throw new TsFileSparkException(
          Messages.format(
              "error.spark.incompatible_table_name",
              actualFile,
              expected.getTableName(),
              actual.getTableName()));
    }
    if (expected.getColumnSchemas().size() != actual.getColumnSchemas().size()) {
      throw new TsFileSparkException(
          Messages.format("error.spark.incompatible_column_count", actualFile));
    }
    for (int i = 0; i < expected.getColumnSchemas().size(); i++) {
      IMeasurementSchema expectedColumn = expected.getColumnSchemas().get(i);
      IMeasurementSchema actualColumn = actual.getColumnSchemas().get(i);
      ColumnCategory expectedCategory = expected.getColumnTypes().get(i);
      ColumnCategory actualCategory = actual.getColumnTypes().get(i);
      if (!expectedColumn.getMeasurementName().equals(actualColumn.getMeasurementName())
          || expectedColumn.getType() != actualColumn.getType()
          || expectedCategory != actualCategory) {
        throw new TsFileSparkException(
            Messages.format(
                "error.spark.incompatible_table_schema_column",
                actualFile,
                i,
                expectedColumn.getMeasurementName(),
                expectedColumn.getType(),
                expectedCategory,
                actualColumn.getMeasurementName(),
                actualColumn.getType(),
                actualCategory));
      }
    }
  }

  private static List<String> discoverTsFiles(String inputPath) {
    try {
      Configuration conf = new Configuration();
      Path path = new Path(inputPath);
      FileSystem fs = path.getFileSystem(conf);
      List<FileStatus> statuses = new ArrayList<>();
      if (containsGlob(inputPath)) {
        FileStatus[] globStatuses = fs.globStatus(path);
        if (globStatuses != null) {
          Collections.addAll(statuses, globStatuses);
        }
      } else if (fs.exists(path)) {
        statuses.add(fs.getFileStatus(path));
      } else {
        throw new TsFileSparkException(Messages.format("error.spark.path_not_exist", inputPath));
      }
      List<String> files = new ArrayList<>();
      for (FileStatus status : statuses) {
        collectTsFiles(fs, status, files);
      }
      return files.stream().distinct().sorted().collect(Collectors.toList());
    } catch (IOException e) {
      throw new TsFileSparkException(
          Messages.format("error.spark.discover_path_failed", inputPath), e);
    }
  }

  private static void collectTsFiles(FileSystem fs, FileStatus status, List<String> files)
      throws IOException {
    if (status.isDirectory()) {
      FileStatus[] children = fs.listStatus(status.getPath());
      List<FileStatus> sortedChildren = new ArrayList<>();
      Collections.addAll(sortedChildren, children);
      sortedChildren.sort(Comparator.comparing(child -> child.getPath().toString()));
      for (FileStatus child : sortedChildren) {
        if (isHidden(child.getPath())) {
          continue;
        }
        if (child.isFile() && child.getPath().getName().endsWith(".tsfile")) {
          files.add(toLocalFile(child.getPath()));
        }
      }
    } else if (status.isFile()) {
      if (!status.getPath().getName().endsWith(".tsfile")) {
        throw new TsFileSparkException(
            Messages.format("error.spark.input_not_tsfile", status.getPath()));
      }
      files.add(toLocalFile(status.getPath()));
    }
  }

  private static boolean containsGlob(String path) {
    return path.indexOf('*') >= 0 || path.indexOf('?') >= 0 || path.indexOf('[') >= 0;
  }

  private static boolean isHidden(Path path) {
    String name = path.getName();
    return name.startsWith("_") || name.startsWith(".");
  }

  private static String toLocalFile(Path path) {
    URI uri = path.toUri();
    String scheme = uri.getScheme();
    if (scheme == null) {
      return path.toString();
    }
    if ("file".equalsIgnoreCase(scheme)) {
      return Paths.get(uri).toString();
    }
    throw new TsFileSparkException(Messages.format("error.spark.local_paths_only", path));
  }

  public static class InferenceResult {
    private final List<String> files;
    private final TsFileTableSchema tableSchema;

    public InferenceResult(List<String> files, TsFileTableSchema tableSchema) {
      this.files = Collections.unmodifiableList(new ArrayList<>(files));
      this.tableSchema = tableSchema;
    }

    public List<String> files() {
      return files;
    }

    public TsFileTableSchema tableSchema() {
      return tableSchema;
    }
  }
}
