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

package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.encoding.encoder.FleaEncoder;

import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

public class FleaDecoderTest {
  @Test
  public void test() throws Exception {
    long[] original = DescendingBitPackingDecoderTest.getTestData();
    DescendingBitPackingDecoderTest.compressDecompressAndAssert(
        original, new FleaEncoder(), new FleaDecoder());
  }

  @Test
  public void endToEndTest() throws Exception {
    long[] original = DescendingBitPackingDecoderTest.getEndToEndTestData();
    DescendingBitPackingDecoderTest.endToEndCompressDecompressAndAssert(original, "FLEA");
  }

  @Test
  public void fileTest() throws Exception {
    List<String> files = new ArrayList<>();
    String[] dataFolderPathList = {
      "../../../encoding-periodic-ng/data", "../../../encoding-periodic-ng/data_no_period"
    };

    for (String dataFolderPath : dataFolderPathList) {
      Path folder = Paths.get(dataFolderPath);
      try (Stream<Path> paths = Files.walk(folder)) {
        paths.filter(Files::isRegularFile).forEach(filePath -> files.add(filePath.toString()));
      } catch (IOException e) {
        e.printStackTrace();
      }
    }
    for (String file : files) {
      System.out.println("file: " + file);
      Path path = Paths.get(file);
      long[] original = Files.lines(path).skip(1).mapToLong(Long::parseLong).toArray();
      DescendingBitPackingDecoderTest.endToEndCompressDecompressAndAssert(original, "FLEA");
      System.out.println("OK");
    }
  }
}
