/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.tsfile.encoding.table;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileReader;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.Path;
import org.apache.tsfile.read.expression.QueryExpression;
import org.apache.tsfile.read.query.dataset.QueryDataSet;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collections;

/** Standard TsFile BLOB container for independently decodable complete-record blocks. */
public final class ClusterTableTsFile {
  public static final String DEVICE = "root.cluster.table";
  public static final String PAYLOAD = "payload";

  private ClusterTableTsFile() {}

  @FunctionalInterface
  public interface BlockConsumer {
    void accept(ClusterTable table) throws IOException;
  }

  public static final class Writer implements Closeable {
    private final TsFileWriter writer;
    private final ClusterTableEncoder encoder;
    private final IMeasurementSchema schema;
    private long blockId;
    private boolean failed;

    public Writer(File file, ClusterTableOptions options) throws IOException {
      if (file.exists()) throw new IOException("Refusing to overwrite existing TsFile: " + file);
      encoder = new ClusterTableEncoder(options);
      schema =
          new MeasurementSchema(
              PAYLOAD, TSDataType.BLOB, TSEncoding.PLAIN, CompressionType.UNCOMPRESSED);
      writer = new TsFileWriter(file);
      try {
        writer.registerTimeseries(DEVICE, schema);
      } catch (WriteProcessException error) {
        writer.close();
        throw new IOException(error);
      }
    }

    /** Caller may pass a whole numeric table; bounded pages each carry complete metadata. */
    public void append(ClusterTable table) throws IOException {
      if (failed) throw new IOException("Writer failed; use a new output file");
      encoder.fit(table);
      int pageSize = ClusterTableEncoder.pageSize(table);
      for (int start = 0; start < table.rowCount(); start += pageSize) {
        ClusterTable page = table.slice(start, Math.min(table.rowCount(), start + pageSize));
        byte[] bytes = encoder.encode(page).bytes();
        Tablet tablet = new Tablet(DEVICE, Collections.singletonList(schema), 1);
        tablet.addTimestamp(0, blockId);
        tablet.addValue(0, PAYLOAD, bytes);
        tablet.setRowSize(1);
        try {
          writer.writeTree(tablet);
          blockId++;
        } catch (IOException | WriteProcessException error) {
          failed = true;
          throw new IOException("Cannot write cluster table block", error);
        }
      }
    }

    public void flush() throws IOException {
      writer.flush();
    }

    @Override
    public void close() throws IOException {
      writer.close();
    }
  }

  public static void read(File file, BlockConsumer consumer) throws IOException {
    try (TsFileReader reader = new TsFileReader(new TsFileSequenceReader(file.getPath()))) {
      QueryDataSet result =
          reader.query(
              QueryExpression.create(
                  Collections.singletonList(new Path(DEVICE, PAYLOAD, true)), null));
      while (result.hasNext()) {
        byte[] bytes = result.next().getFields().get(0).getBinaryV().getValues();
        consumer.accept(ClusterTableCodec.decode(bytes));
      }
    }
  }
}
