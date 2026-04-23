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

package org.apache.tsfile.tools;

import org.apache.tsfile.external.commons.io.FilenameUtils;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TsFileTool {

  private static final Logger LOGGER = LoggerFactory.getLogger(TsFileTool.class);

  private static int THREAD_COUNT = 8;
  private static long CHUNK_SIZE_BYTE = 256L * 1024 * 1024;
  private static String outputDirectoryStr = "";
  private static String inputDirectoryStr = "";
  private static String failedDirectoryStr = "failed";
  private static String schemaPathStr = "";
  private static String tableNameStr = null;
  private static String timePrecisionStr = null;
  private static String separatorStr = null;
  private static String formatStr = null;

  private static ImportSchema importSchema = null;

  public static void main(String[] args) {
    if (System.getenv("TSFILE_HOME") != null) {
      System.setProperty("TSFILE_HOME", System.getenv("TSFILE_HOME"));
    }
    parseCommandLineParams(args);
    if (!validateParams()) {
      return;
    }
    createDir();

    boolean isSchemaMode = schemaPathStr != null && !schemaPathStr.isEmpty();
    if (isSchemaMode) {
      try {
        importSchema = ImportSchemaParser.parse(schemaPathStr);
      } catch (Exception e) {
        LOGGER.error("Failed to parse schema file: " + schemaPathStr, e);
        System.exit(1);
      }
    }

    File inputDirectory = new File(inputDirectoryStr);
    ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

    try {
      processDirectory(inputDirectory, executor);
    } finally {
      executor.shutdown();
      try {
        executor.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
      } catch (InterruptedException e) {
        LOGGER.error("Failed to await termination", e);
      }
      LOGGER.info("The " + inputDirectoryStr + " directory or file has completed execution");
    }
  }

  private static void processDirectory(File directory, ExecutorService executor) {
    if (directory.isFile()) {
      processFile(directory, executor);
    } else {
      File[] files = directory.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isDirectory()) {
            processDirectory(file, executor);
          } else if (file.isFile() && isAcceptedFormat(file.getName())) {
            processFile(file, executor);
          }
        }
      }
    }
  }

  private static boolean isAcceptedFormat(String fileName) {
    String lower = fileName.toLowerCase();
    String fmt = resolveFormat(fileName);
    if (formatStr != null) {
      return fmt.equals(formatStr);
    }
    return lower.endsWith(".csv")
        || lower.endsWith(".parquet")
        || lower.endsWith(".arrow")
        || lower.endsWith(".ipc")
        || lower.endsWith(".feather");
  }

  private static String resolveFormat(String fileName) {
    if (formatStr != null) {
      return formatStr;
    }
    String lower = fileName.toLowerCase();
    if (lower.endsWith(".parquet")) {
      return "parquet";
    }
    if (lower.endsWith(".arrow") || lower.endsWith(".ipc") || lower.endsWith(".feather")) {
      return "arrow";
    }
    return "csv";
  }

  private static void processFile(File inputFile, ExecutorService executor) {
    String baseName = FilenameUtils.getBaseName(inputFile.getName());
    String inputFileAbsolutePath = new File(inputDirectoryStr).getAbsolutePath();
    String sourceFileName = inputFile.getName();
    String relativePath =
        inputFile.getAbsolutePath().replace(inputFileAbsolutePath, "").replace(sourceFileName, "");
    String outputDir = outputDirectoryStr + relativePath;
    String format = resolveFormat(inputFile.getName());

    executor.submit(
        () -> {
          try {
            if (importSchema != null) {
              processSchemaMode(inputFile, baseName, outputDir, format);
            } else {
              processAutoMode(inputFile, baseName, outputDir, format);
            }
          } catch (Exception e) {
            LOGGER.error("Failed to process file: " + inputFile.getAbsolutePath(), e);
            cpFile(inputFile.getAbsolutePath(), failedDirectoryStr);
          }
        });
  }

  private static void processSchemaMode(
      File inputFile, String baseName, String outputDir, String format) {
    try (SourceReader reader = createSchemaReader(inputFile, format)) {
      ImportExecutor importExecutor = new ImportExecutor(importSchema);
      boolean success = importExecutor.execute(reader, outputDir, baseName, failedDirectoryStr);
      if (success) {
        LOGGER.info(baseName + ".tsfile successfully generated");
      } else {
        cpFile(inputFile.getAbsolutePath(), failedDirectoryStr);
      }
    } catch (Exception e) {
      LOGGER.error("Failed to process file: " + inputFile.getAbsolutePath(), e);
      cpFile(inputFile.getAbsolutePath(), failedDirectoryStr);
    }
  }

  private static void processAutoMode(
      File inputFile, String baseName, String outputDir, String format) {
    try (SourceReader reader = createAutoReader(inputFile, format)) {
      ImportSchema autoSchema = reader.inferSchema();
      ImportExecutor importExecutor = new ImportExecutor(autoSchema);
      boolean success = importExecutor.execute(reader, outputDir, baseName, failedDirectoryStr);
      if (success) {
        LOGGER.info(baseName + ".tsfile successfully generated");
      } else {
        cpFile(inputFile.getAbsolutePath(), failedDirectoryStr);
      }
    } catch (Exception e) {
      LOGGER.error("Failed to process file: " + inputFile.getAbsolutePath(), e);
      cpFile(inputFile.getAbsolutePath(), failedDirectoryStr);
    }
  }

  private static SourceReader createSchemaReader(File inputFile, String format) {
    if ("parquet".equals(format)) {
      return new ParquetSourceReader(inputFile, importSchema);
    }
    if ("arrow".equals(format)) {
      return new ArrowSourceReader(inputFile, importSchema);
    }
    return new CsvSourceReader(inputFile, importSchema, CHUNK_SIZE_BYTE);
  }

  private static SourceReader createAutoReader(File inputFile, String format) {
    if ("parquet".equals(format)) {
      ParquetSourceReader reader = new ParquetSourceReader(inputFile);
      if (tableNameStr != null) {
        reader.setOverrideTableName(tableNameStr);
      }
      if (timePrecisionStr != null) {
        reader.setOverrideTimePrecision(timePrecisionStr);
      }
      return reader;
    }
    if ("arrow".equals(format)) {
      ArrowSourceReader reader = new ArrowSourceReader(inputFile);
      if (tableNameStr != null) {
        reader.setOverrideTableName(tableNameStr);
      }
      if (timePrecisionStr != null) {
        reader.setOverrideTimePrecision(timePrecisionStr);
      }
      return reader;
    }
    String sep = separatorStr != null ? separatorStr : ",";
    CsvSourceReader reader = new CsvSourceReader(inputFile, sep, CHUNK_SIZE_BYTE);
    if (tableNameStr != null) {
      reader.setOverrideTableName(tableNameStr);
    }
    if (timePrecisionStr != null) {
      reader.setOverrideTimePrecision(timePrecisionStr);
    }
    return reader;
  }

  private static void cpFile(String sourceFilePath, String targetDirectoryPath) {
    try {
      String inputFileAbsolutePath = new File(inputDirectoryStr).getAbsolutePath();
      String sourceFileName = new File(sourceFilePath).getName();
      String relativeDir =
          targetDirectoryPath
              + sourceFilePath.replace(inputFileAbsolutePath, "").replace(sourceFileName, "");
      Files.createDirectories(Paths.get(relativeDir));
      Path sourcePath = Paths.get(sourceFilePath);
      Path targetPath = Paths.get(relativeDir, sourcePath.getFileName().toString());
      Files.copy(sourcePath, targetPath, StandardCopyOption.REPLACE_EXISTING);
    } catch (IOException e) {
      LOGGER.error("Failed to copy file: " + sourceFilePath, e);
    }
  }

  private static void printHelp(Options options) {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp("csv2tsfile.sh/csv2tsfile.bat", options);
  }

  private static void parseCommandLineParams(String[] args) {
    THREAD_COUNT = 8;
    CHUNK_SIZE_BYTE = 256L * 1024 * 1024;
    outputDirectoryStr = "";
    inputDirectoryStr = "";
    failedDirectoryStr = "failed";
    schemaPathStr = "";
    tableNameStr = null;
    timePrecisionStr = null;
    separatorStr = null;
    formatStr = null;
    importSchema = null;

    Options options = new Options();
    options.addOption("s", "source", true, "Input directory or file");
    options.addOption("t", "target", true, "Output directory");
    options.addOption("fd", "fail_dir", true, "Failed file directory");
    options.addOption("b", "block_size", true, "Block size (default 256M)");
    options.addOption("tn", "thread_num", true, "Thread count (default 8)");
    options.addOption("schema", "schema", true, "Schema file path (omit for auto mode)");
    options.addOption(null, "table_name", true, "Table name override (auto mode)");
    options.addOption(null, "time_precision", true, "Time precision: ms, us, ns, s (auto mode)");
    options.addOption(null, "separator", true, "CSV separator: , / tab / ; (auto mode, default ,)");
    options.addOption(
        null,
        "format",
        true,
        "Source format: csv / parquet / arrow (default: auto-detect by extension)");
    options.addOption("h", "help", false, "Show help");

    try {
      CommandLineParser parser = new DefaultParser();
      CommandLine cmd = parser.parse(options, args);

      if (cmd.hasOption("h")) {
        printHelp(options);
        System.exit(0);
      }
      if (cmd.hasOption("s")) {
        inputDirectoryStr = cmd.getOptionValue("s");
      }
      if (cmd.hasOption("t")) {
        outputDirectoryStr = cmd.getOptionValue("t");
      }
      if (cmd.hasOption("fd")) {
        failedDirectoryStr = cmd.getOptionValue("fd");
      }
      if (cmd.hasOption("b")) {
        CHUNK_SIZE_BYTE = parseBlockSize(cmd.getOptionValue("b"));
      }
      if (cmd.hasOption("tn")) {
        THREAD_COUNT = Integer.parseInt(cmd.getOptionValue("tn"));
      }
      if (cmd.hasOption("schema")) {
        schemaPathStr = cmd.getOptionValue("schema");
      }
      if (cmd.hasOption("table_name")) {
        tableNameStr = cmd.getOptionValue("table_name");
      }
      if (cmd.hasOption("time_precision")) {
        timePrecisionStr = cmd.getOptionValue("time_precision");
      }
      if (cmd.hasOption("separator")) {
        String sep = cmd.getOptionValue("separator");
        if ("tab".equalsIgnoreCase(sep)) {
          separatorStr = "\t";
        } else {
          separatorStr = sep;
        }
      }
      if (cmd.hasOption("format")) {
        formatStr = cmd.getOptionValue("format").toLowerCase();
      }
      if (failedDirectoryStr == null || failedDirectoryStr.isEmpty()) {
        failedDirectoryStr = "failed";
      }
    } catch (ParseException e) {
      LOGGER.error("Error parsing command line options", e);
    }
  }

  static long parseBlockSize(String blockSizeValue) {
    blockSizeValue = blockSizeValue.toUpperCase();
    if (blockSizeValue.endsWith("K")) {
      return Long.parseLong(blockSizeValue.substring(0, blockSizeValue.length() - 1)) * 1024;
    } else if (blockSizeValue.endsWith("M")) {
      return Long.parseLong(blockSizeValue.substring(0, blockSizeValue.length() - 1)) * 1024 * 1024;
    } else if (blockSizeValue.endsWith("G")) {
      return Long.parseLong(blockSizeValue.substring(0, blockSizeValue.length() - 1))
          * 1024
          * 1024
          * 1024;
    } else if (blockSizeValue.endsWith("T") || blockSizeValue.endsWith("B")) {
      throw new IllegalArgumentException("block_size only supports units of K, M, G, or numbers");
    }
    return Long.parseLong(blockSizeValue);
  }

  private static void createDir() {
    File targetDir = new File(outputDirectoryStr);
    if (!targetDir.exists()) {
      targetDir.mkdirs();
    }
    if (failedDirectoryStr != null) {
      File failDirFile = new File(failedDirectoryStr);
      if (!failDirFile.exists()) {
        failDirFile.mkdirs();
      }
    }
  }

  private static boolean validateParams() {
    if (inputDirectoryStr == null || inputDirectoryStr.isEmpty()) {
      LOGGER.error("Missing required parameters. --source/-s is required");
      return false;
    }
    if (outputDirectoryStr == null || outputDirectoryStr.isEmpty()) {
      LOGGER.error("Missing required parameters. --target/-t is required");
      return false;
    }
    File sourceDir = new File(inputDirectoryStr);
    if (!sourceDir.exists()) {
      LOGGER.error(sourceDir + " directory or file does not exist.");
      return false;
    }
    if (schemaPathStr != null && !schemaPathStr.isEmpty()) {
      File schemaFile = new File(schemaPathStr);
      if (!schemaFile.exists()) {
        LOGGER.error(schemaPathStr + " schema file does not exist.");
        return false;
      }
    }
    if (THREAD_COUNT <= 0) {
      LOGGER.error("Invalid thread number. Thread number must be greater than 0.");
      return false;
    }
    return true;
  }
}
