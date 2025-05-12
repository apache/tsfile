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

package org.apache.tsfile;

import oshi.SystemInfo;
import oshi.hardware.GlobalMemory;
import oshi.software.os.OSProcess;
import oshi.software.os.OperatingSystem;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public class MemoryMonitor {
  private static final String CSV_HEADER = "iter,memory_usage(kb)\n";
  private static final String CSV_FILE = "/tmp/memory_usage_java.csv";
  private BufferedWriter writer;
  private int iter = 0;

  public MemoryMonitor() throws IOException {
    writer =
        Files.newBufferedWriter(
            Paths.get(CSV_FILE), StandardOpenOption.CREATE, StandardOpenOption.APPEND);
    writer.write(CSV_HEADER);
  }

  public void recordMemoryUsage() throws IOException {
    long memory = get_memory_usage();
    String line = String.format("%d,%d\n", iter++, memory);
    writer.write(line);
    writer.flush();
  }

  public long get_memory_usage() {
    SystemInfo si = new SystemInfo();
    OperatingSystem os = si.getOperatingSystem();
    GlobalMemory memory = si.getHardware().getMemory();
    OSProcess currentProcess = os.getProcess(os.getProcessId());
    long residentSetSize = currentProcess.getResidentSetSize();
    return residentSetSize / 1024;
  }

  public void close() {
    try {
      if (writer != null) {
        writer.close();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}
