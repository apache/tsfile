package org.apache.tsfile.tools;

import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.controller.CachedChunkLoaderImpl;
import org.apache.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.tsfile.read.query.executor.TableQueryExecutor;
import org.apache.tsfile.read.reader.block.TsBlockReader;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TsfiletoolsTest {
  private final String testDir = "target" + File.separator + "csvTest";
  private final String csvFile = testDir + File.separator + "data.csv";
  private final String schemaFile = testDir + File.separator + "schemaFile.txt";

  float[] tmpResult2 = new float[20];
  float[] tmpResult3 = new float[20];
  float[] tmpResult5 = new float[20];

  @Before
  public void setUp() {
    new File(testDir).mkdirs();
    genCsvFile(20);
    genSchemaFile();
  }

  public void genSchemaFile() {
    try (BufferedWriter writer = new BufferedWriter(new FileWriter(schemaFile))) {
      writer.write("table_name=root.db1");
      writer.newLine();
      writer.write("time_precision=ms");
      writer.newLine();
      writer.write("has_header=true");
      writer.newLine();
      writer.write("separator=,");
      writer.newLine();
      writer.write("null_format=\\N");
      writer.newLine();
      writer.newLine();
      writer.write("time_column=time");
      writer.newLine();
      writer.write("csv_columns");
      writer.newLine();
      writer.write("time INT64,");
      writer.newLine();
      writer.write("tmp2 FLOAT,");
      writer.newLine();
      writer.write("tmp3 FLOAT,");
      writer.newLine();
      writer.write("SKIP,");
      writer.newLine();
      writer.write("tmp5 FLOAT");
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void genCsvFile(int rows) {

    try (BufferedWriter writer = new BufferedWriter(new FileWriter(csvFile))) {
      writer.write("time,tmp2,tmp3,tmp4,tmp5");
      writer.newLine();
      Random random = new Random();
      long timestamp = System.currentTimeMillis();

      for (int i = 0; i < rows; i++) {
        timestamp = timestamp + i;
        float tmp2 = random.nextFloat();
        float tmp3 = random.nextFloat();
        float tmp4 = random.nextFloat();
        float tmp5 = random.nextFloat();
        tmpResult2[i] = tmp2;
        tmpResult3[i] = tmp3;
        tmpResult5[i] = tmp5;
        writer.write(timestamp + "," + tmp2 + "," + tmp3 + "," + tmp4 + "," + tmp5);
        writer.newLine();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDown() throws Exception {
    FileUtils.deleteDirectory(new File(testDir));
  }

  @Test
  public void testCsvToTsfile() throws Exception {
    String scFilePath = new File(schemaFile).getAbsolutePath();
    String csvFilePath = new File(csvFile).getAbsolutePath();
    String targetPath = new File(testDir).getAbsolutePath();
    String dataTsfilePath = new File(targetPath + File.separator + "data.tsfile").getAbsolutePath();
    String[] args = new String[] {"-s" + csvFilePath, "-schema" + scFilePath, "-t" + targetPath};
    TsFileTool.main(args);
    List<String> columns = new ArrayList<>();
    columns.add("tmp2");
    columns.add("tmp3");
    columns.add("tmp5");
    try (TsFileSequenceReader sequenceReader = new TsFileSequenceReader(dataTsfilePath)) {
      TableQueryExecutor tableQueryExecutor =
          new TableQueryExecutor(
              new MetadataQuerierByFileImpl(sequenceReader),
              new CachedChunkLoaderImpl(sequenceReader),
              TableQueryExecutor.TableQueryOrdering.DEVICE);
      final TsBlockReader reader = tableQueryExecutor.query("root.db1", columns, null, null, null);
      assertTrue(reader.hasNext());
      int cnt = 0;
      while (reader.hasNext()) {
        final TsBlock result = reader.next();
        float[] floats_tmp2 = result.getColumn(0).getFloats();
        float[] floats_tmp3 = result.getColumn(1).getFloats();
        float[] floats_tmp5 = result.getColumn(2).getFloats();
        for (int i = 0; i < 20; i++) {
          assertEquals(tmpResult2[i], floats_tmp2[i], 0);
          assertEquals(tmpResult3[i], floats_tmp3[i], 0);
          assertEquals(tmpResult5[i], floats_tmp5[i], 0);
        }
        cnt += result.getPositionCount();
      }
      assertEquals(20, cnt);
    }
  }
}
