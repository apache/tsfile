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

package org.apache.tsfile.write.record;

import org.apache.tsfile.common.conf.TSFileConfig;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.BitMap;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.DateUtils;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.apache.tsfile.write.UnSupportedDataTypeException;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A tablet data of one device, the tablet contains multiple measurements of this device that share
 * the same time column.
 *
 * <p>for example: device root.sg1.d1
 *
 * <p>time, m1, m2, m3 1, 1, 2, 3 2, 1, 2, 3 3, 1, 2, 3
 *
 * <p>Notice: The tablet should not have empty cell, please use BitMap to denote null value
 */
public class Tablet {

  private static final int DEFAULT_SIZE = 1024;
  private static final String NOT_SUPPORT_DATATYPE = "Data type %s is not supported.";

  /** DeviceId if using tree-view interfaces or TableName when using table-view interfaces. */
  private String insertTargetName;

  /** The list of {@link MeasurementSchema}s for creating the {@link Tablet} */
  private List<IMeasurementSchema> schemas;
  /**
   * Marking the type of each column, namely ID or MEASUREMENT. Notice: the ID columns must be the
   * FIRST ones.
   */
  private List<ColumnType> columnTypes;

  /** Columns in [0, idColumnRange) are all ID columns. */
  private int idColumnRange;

  /** MeasurementId->indexOf({@link MeasurementSchema}) */
  private final Map<String, Integer> measurementIndex;

  /** Timestamps in this {@link Tablet} */
  public long[] timestamps;
  /** Each object is a primitive type array, which represents values of one measurement */
  public Object[] values;
  /** Each {@link BitMap} represents the existence of each value in the current column. */
  public BitMap[] bitMaps;
  /** The number of rows to include in this {@link Tablet} */
  public int rowSize;
  /** The maximum number of rows for this {@link Tablet} */
  private final int maxRowNumber;

  /**
   * Return a {@link Tablet} with default specified row number. This is the standard constructor
   * (all Tablet should be the same size).
   *
   * @param insertTargetName the name of the device specified to be written in
   * @param schemas the list of {@link MeasurementSchema}s for creating the tablet, only
   *     measurementId and type take effects
   */
  public Tablet(String insertTargetName, List<IMeasurementSchema> schemas) {
    this(insertTargetName, schemas, DEFAULT_SIZE);
  }

  public Tablet(String insertTargetName, List<IMeasurementSchema> schemas, int maxRowNumber) {
    this(
        insertTargetName,
        schemas,
        ColumnType.nCopy(ColumnType.MEASUREMENT, schemas.size()),
        maxRowNumber);
  }

  public Tablet(
      String insertTargetName, List<IMeasurementSchema> schemas, List<ColumnType> columnTypes) {
    this(insertTargetName, schemas, columnTypes, DEFAULT_SIZE);
  }

  /**
   * Return a {@link Tablet} with the specified number of rows (maxBatchSize). Only call this
   * constructor directly for testing purposes. {@link Tablet} should normally always be default
   * size.
   *
   * @param insertTargetName the name of the device specified to be written in
   * @param schemas the list of {@link MeasurementSchema}s for creating the row batch, only
   *     measurementId and type take effects
   * @param maxRowNumber the maximum number of rows for this tablet
   */
  public Tablet(
      String insertTargetName,
      List<IMeasurementSchema> schemas,
      List<ColumnType> columnTypes,
      int maxRowNumber) {
    this.insertTargetName = insertTargetName;
    this.schemas = new ArrayList<>(schemas);
    setColumnTypes(columnTypes);
    this.maxRowNumber = maxRowNumber;
    measurementIndex = new HashMap<>();
    constructMeasurementIndexMap();

    createColumns();

    reset();
  }

  /**
   * Return a {@link Tablet} with specified timestamps and values. Only call this constructor
   * directly for Trigger.
   *
   * @param insertTargetName the name of the device specified to be written in
   * @param schemas the list of {@link MeasurementSchema}s for creating the row batch, only
   *     measurementId and type take effects
   * @param timestamps given timestamps
   * @param values given values
   * @param bitMaps given {@link BitMap}s
   * @param maxRowNumber the maximum number of rows for this {@link Tablet}
   */
  public Tablet(
      String insertTargetName,
      List<IMeasurementSchema> schemas,
      long[] timestamps,
      Object[] values,
      BitMap[] bitMaps,
      int maxRowNumber) {
    this(
        insertTargetName,
        schemas,
        ColumnType.nCopy(ColumnType.MEASUREMENT, schemas.size()),
        timestamps,
        values,
        bitMaps,
        maxRowNumber);
  }

  public Tablet(
      String insertTargetName,
      List<IMeasurementSchema> schemas,
      List<ColumnType> columnTypes,
      long[] timestamps,
      Object[] values,
      BitMap[] bitMaps,
      int maxRowNumber) {
    this.insertTargetName = insertTargetName;
    this.schemas = schemas;
    setColumnTypes(columnTypes);
    this.timestamps = timestamps;
    this.values = values;
    this.bitMaps = bitMaps;
    this.maxRowNumber = maxRowNumber;
    // rowSize == maxRowNumber in this case
    this.rowSize = maxRowNumber;
    measurementIndex = new HashMap<>();
    constructMeasurementIndexMap();
  }

  private void constructMeasurementIndexMap() {
    int indexInSchema = 0;
    for (IMeasurementSchema schema : schemas) {
      measurementIndex.put(schema.getMeasurementId(), indexInSchema);
      indexInSchema++;
    }
  }

  public void setInsertTargetName(String insertTargetName) {
    this.insertTargetName = insertTargetName;
  }

  public void setSchemas(List<IMeasurementSchema> schemas) {
    this.schemas = schemas;
  }

  public void initBitMaps() {
    this.bitMaps = new BitMap[schemas.size()];
    for (int column = 0; column < schemas.size(); column++) {
      this.bitMaps[column] = new BitMap(getMaxRowNumber());
    }
  }

  public void addTimestamp(int rowIndex, long timestamp) {
    timestamps[rowIndex] = timestamp;
  }

  public void addValue(String measurementId, int rowIndex, Object value) {
    int indexOfSchema = measurementIndex.get(measurementId);
    IMeasurementSchema measurementSchema = schemas.get(indexOfSchema);
    addValueOfDataType(measurementSchema.getType(), rowIndex, indexOfSchema, value);
  }

  private void addValueOfDataType(
      TSDataType dataType, int rowIndex, int indexOfSchema, Object value) {

    if (value == null) {
      // Init the bitMap to mark null value
      if (bitMaps == null) {
        bitMaps = new BitMap[values.length];
      }
      if (bitMaps[indexOfSchema] == null) {
        bitMaps[indexOfSchema] = new BitMap(maxRowNumber);
      }
      // Mark the null value position
      bitMaps[indexOfSchema].mark(rowIndex);
    }
    switch (dataType) {
      case TEXT:
      case STRING:
        {
          if (columnTypes.get(indexOfSchema).equals(ColumnType.MEASUREMENT)) {
            Binary[] sensor = (Binary[]) values[indexOfSchema];
            if (value instanceof Binary) {
              sensor[rowIndex] = (Binary) value;
            } else {
              sensor[rowIndex] =
                  value != null
                      ? new Binary((String) value, TSFileConfig.STRING_CHARSET)
                      : Binary.EMPTY_VALUE;
            }
          } else {
            String[] stringValues = (String[]) values[indexOfSchema];
            stringValues[rowIndex] = value != null ? value.toString() : null;
          }
          break;
        }
      case BLOB:
        {
          Binary[] sensor = (Binary[]) values[indexOfSchema];
          sensor[rowIndex] = value != null ? (Binary) value : Binary.EMPTY_VALUE;
          break;
        }
      case FLOAT:
        {
          float[] sensor = (float[]) values[indexOfSchema];
          sensor[rowIndex] = value != null ? (float) value : Float.MIN_VALUE;
          break;
        }
      case INT32:
        {
          int[] sensor = (int[]) values[indexOfSchema];
          sensor[rowIndex] = value != null ? (int) value : Integer.MIN_VALUE;
          break;
        }
      case DATE:
        {
          LocalDate[] sensor = (LocalDate[]) values[indexOfSchema];
          sensor[rowIndex] = (LocalDate) value;
          break;
        }
      case INT64:
      case TIMESTAMP:
        {
          long[] sensor = (long[]) values[indexOfSchema];
          sensor[rowIndex] = value != null ? (long) value : Long.MIN_VALUE;
          break;
        }
      case DOUBLE:
        {
          double[] sensor = (double[]) values[indexOfSchema];
          sensor[rowIndex] = value != null ? (double) value : Double.MIN_VALUE;
          break;
        }
      case BOOLEAN:
        {
          boolean[] sensor = (boolean[]) values[indexOfSchema];
          sensor[rowIndex] = value != null && (boolean) value;
          break;
        }
      default:
        throw new UnSupportedDataTypeException(String.format(NOT_SUPPORT_DATATYPE, dataType));
    }
  }

  public List<IMeasurementSchema> getSchemas() {
    return schemas;
  }

  /** Return the maximum number of rows for this tablet */
  public int getMaxRowNumber() {
    return maxRowNumber;
  }

  /** Reset Tablet to the default state - set the rowSize to 0 and reset bitMaps */
  public void reset() {
    rowSize = 0;
    if (bitMaps != null) {
      for (BitMap bitMap : bitMaps) {
        if (bitMap != null) {
          bitMap.reset();
        }
      }
    }
  }

  private void createColumns() {
    // create timestamp column
    timestamps = new long[maxRowNumber];

    // calculate total value column size
    int valueColumnsSize = schemas.size();

    // value column
    values = new Object[valueColumnsSize];
    int columnIndex = 0;
    for (int i = 0; i < schemas.size(); i++) {
      IMeasurementSchema schema = schemas.get(i);
      ColumnType columnType = columnTypes.get(i);
      TSDataType dataType = schema.getType();
      values[columnIndex] = createValueColumnOfDataType(dataType, columnType);
      columnIndex++;
    }
  }

  private Object createValueColumnOfDataType(TSDataType dataType, ColumnType columnType) {

    Object valueColumn;
    switch (dataType) {
      case INT32:
        valueColumn = new int[maxRowNumber];
        break;
      case INT64:
      case TIMESTAMP:
        valueColumn = new long[maxRowNumber];
        break;
      case FLOAT:
        valueColumn = new float[maxRowNumber];
        break;
      case DOUBLE:
        valueColumn = new double[maxRowNumber];
        break;
      case BOOLEAN:
        valueColumn = new boolean[maxRowNumber];
        break;
      case TEXT:
      case STRING:
        if (columnType.equals(ColumnType.MEASUREMENT)) {
          valueColumn = new Binary[maxRowNumber];
        } else {
          valueColumn = new String[maxRowNumber];
        }
        break;
      case BLOB:
        valueColumn = new Binary[maxRowNumber];
        break;
      case DATE:
        valueColumn = new LocalDate[maxRowNumber];
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(NOT_SUPPORT_DATATYPE, dataType));
    }
    return valueColumn;
  }

  public int getTimeBytesSize() {
    return rowSize * 8;
  }

  /** @return Total bytes of values */
  public int getTotalValueOccupation() {
    int valueOccupation = 0;
    int columnIndex = 0;
    for (IMeasurementSchema schema : schemas) {
      valueOccupation += calOccupationOfOneColumn(schema.getType(), columnIndex);
      columnIndex++;
    }
    // Add bitmap size if the tablet has bitMaps
    if (bitMaps != null) {
      for (BitMap bitMap : bitMaps) {
        // Marker byte
        valueOccupation++;
        if (bitMap != null && !bitMap.isAllUnmarked()) {
          valueOccupation += rowSize / Byte.SIZE + 1;
        }
      }
    }
    return valueOccupation;
  }

  private int calOccupationOfOneColumn(TSDataType dataType, int columnIndex) {
    int valueOccupation = 0;
    switch (dataType) {
      case BOOLEAN:
        valueOccupation += rowSize;
        break;
      case INT32:
      case FLOAT:
      case DATE:
        valueOccupation += rowSize * 4;
        break;
      case INT64:
      case DOUBLE:
      case TIMESTAMP:
        valueOccupation += rowSize * 8;
        break;
      case TEXT:
      case BLOB:
      case STRING:
        valueOccupation += rowSize * 4;
        if (columnTypes == null || columnTypes.get(columnIndex) == ColumnType.MEASUREMENT) {
          Binary[] binaries = (Binary[]) values[columnIndex];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            valueOccupation += binaries[rowIndex].getLength();
          }
        } else {
          String[] strings = (String[]) values[columnIndex];
          for (int rowIndex = 0; rowIndex < rowSize; rowIndex++) {
            valueOccupation += strings[rowIndex].length() * Character.BYTES;
          }
        }
        break;
      default:
        throw new UnSupportedDataTypeException(String.format(NOT_SUPPORT_DATATYPE, dataType));
    }
    return valueOccupation;
  }

  /** Serialize {@link Tablet} */
  public ByteBuffer serialize() throws IOException {
    try (PublicBAOS byteArrayOutputStream = new PublicBAOS();
        DataOutputStream outputStream = new DataOutputStream(byteArrayOutputStream)) {
      serialize(outputStream);
      return ByteBuffer.wrap(byteArrayOutputStream.getBuf(), 0, byteArrayOutputStream.size());
    }
  }

  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(insertTargetName, stream);
    ReadWriteIOUtils.write(rowSize, stream);
    writeMeasurementSchemas(stream);
    writeTimes(stream);
    writeBitMaps(stream);
    writeValues(stream);
  }

  /** Serialize {@link MeasurementSchema}s */
  private void writeMeasurementSchemas(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(schemas != null), stream);
    if (schemas != null) {
      ReadWriteIOUtils.write(schemas.size(), stream);
      for (int i = 0; i < schemas.size(); i++) {
        IMeasurementSchema schema = schemas.get(i);
        ColumnType columnType = columnTypes.get(i);
        if (schema == null) {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(false), stream);
        } else {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(true), stream);
          schema.serializeTo(stream);
          ReadWriteIOUtils.write((byte) columnType.ordinal(), stream);
        }
      }
    }
  }

  private void writeTimes(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(timestamps != null), stream);
    if (timestamps != null) {
      for (int i = 0; i < rowSize; i++) {
        ReadWriteIOUtils.write(timestamps[i], stream);
      }
    }
  }

  /** Serialize {@link BitMap}s */
  private void writeBitMaps(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(bitMaps != null), stream);
    if (bitMaps != null) {
      int size = (schemas == null ? 0 : schemas.size());
      for (int i = 0; i < size; i++) {
        if (bitMaps[i] == null) {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(false), stream);
        } else {
          ReadWriteIOUtils.write(BytesUtils.boolToByte(true), stream);
          ReadWriteIOUtils.write(bitMaps[i].getSize(), stream);
          ReadWriteIOUtils.write(new Binary(bitMaps[i].getByteArray()), stream);
        }
      }
    }
  }

  /** Serialize values */
  private void writeValues(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(values != null), stream);
    if (values != null) {
      int size = (schemas == null ? 0 : schemas.size());
      for (int i = 0; i < size; i++) {
        serializeColumn(schemas.get(i).getType(), values[i], stream, columnTypes.get(i));
      }
    }
  }

  private void serializeColumn(
      TSDataType dataType, Object column, DataOutputStream stream, ColumnType columnType)
      throws IOException {
    ReadWriteIOUtils.write(BytesUtils.boolToByte(column != null), stream);

    if (column != null) {
      switch (dataType) {
        case INT32:
          int[] intValues = (int[]) column;
          for (int j = 0; j < rowSize; j++) {
            ReadWriteIOUtils.write(intValues[j], stream);
          }
          break;
        case DATE:
          LocalDate[] dateValues = (LocalDate[]) column;
          for (int j = 0; j < rowSize; j++) {
            ReadWriteIOUtils.write(DateUtils.parseDateExpressionToInt(dateValues[j]), stream);
          }
          break;
        case INT64:
        case TIMESTAMP:
          long[] longValues = (long[]) column;
          for (int j = 0; j < rowSize; j++) {
            ReadWriteIOUtils.write(longValues[j], stream);
          }
          break;
        case FLOAT:
          float[] floatValues = (float[]) column;
          for (int j = 0; j < rowSize; j++) {
            ReadWriteIOUtils.write(floatValues[j], stream);
          }
          break;
        case DOUBLE:
          double[] doubleValues = (double[]) column;
          for (int j = 0; j < rowSize; j++) {
            ReadWriteIOUtils.write(doubleValues[j], stream);
          }
          break;
        case BOOLEAN:
          boolean[] boolValues = (boolean[]) column;
          for (int j = 0; j < rowSize; j++) {
            ReadWriteIOUtils.write(BytesUtils.boolToByte(boolValues[j]), stream);
          }
          break;
        case TEXT:
        case STRING:
          if (columnType == ColumnType.MEASUREMENT) {
            Binary[] binaryValues = (Binary[]) column;
            for (int j = 0; j < rowSize; j++) {
              ReadWriteIOUtils.write(BytesUtils.boolToByte(binaryValues[j] != null), stream);
              if (binaryValues[j] != null) {
                ReadWriteIOUtils.write(binaryValues[j], stream);
              }
            }
          } else {
            String[] stringValues = (String[]) column;
            for (int j = 0; j < rowSize; j++) {
              ReadWriteIOUtils.write(BytesUtils.boolToByte(stringValues[j] != null), stream);
              if (stringValues[j] != null) {
                ReadWriteIOUtils.write(stringValues[j], stream);
              }
            }
          }
          break;
        case BLOB:
          Binary[] binaryValues = (Binary[]) column;
          for (int j = 0; j < rowSize; j++) {
            ReadWriteIOUtils.write(BytesUtils.boolToByte(binaryValues[j] != null), stream);
            if (binaryValues[j] != null) {
              ReadWriteIOUtils.write(binaryValues[j], stream);
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", dataType));
      }
    }
  }

  /** Deserialize Tablet */
  public static Tablet deserialize(ByteBuffer byteBuffer) {
    String deviceId = ReadWriteIOUtils.readString(byteBuffer);
    int rowSize = ReadWriteIOUtils.readInt(byteBuffer);

    // deserialize schemas
    int schemaSize = 0;
    List<IMeasurementSchema> schemas = new ArrayList<>();
    List<ColumnType> columnTypes = new ArrayList<>();
    boolean isSchemasNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isSchemasNotNull) {
      schemaSize = ReadWriteIOUtils.readInt(byteBuffer);
      for (int i = 0; i < schemaSize; i++) {
        boolean hasSchema = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
        if (hasSchema) {
          schemas.add(MeasurementSchema.deserializeFrom(byteBuffer));
          columnTypes.add(ColumnType.values()[byteBuffer.get()]);
        }
      }
    }

    // deserialize times
    long[] times = new long[rowSize];
    boolean isTimesNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isTimesNotNull) {
      for (int i = 0; i < rowSize; i++) {
        times[i] = ReadWriteIOUtils.readLong(byteBuffer);
      }
    }

    // deserialize bitmaps
    BitMap[] bitMaps = new BitMap[schemaSize];
    boolean isBitMapsNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isBitMapsNotNull) {
      bitMaps = readBitMapsFromBuffer(byteBuffer, schemaSize);
    }

    // deserialize values
    TSDataType[] dataTypes =
        schemas.stream().map(IMeasurementSchema::getType).toArray(TSDataType[]::new);
    Object[] values = new Object[schemaSize];
    boolean isValuesNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
    if (isValuesNotNull) {
      values = readTabletValuesFromBuffer(byteBuffer, dataTypes, columnTypes, schemaSize, rowSize);
    }

    Tablet tablet = new Tablet(deviceId, schemas, columnTypes, times, values, bitMaps, rowSize);
    tablet.constructMeasurementIndexMap();
    return tablet;
  }

  /** deserialize bitmaps */
  public static BitMap[] readBitMapsFromBuffer(ByteBuffer byteBuffer, int columns) {
    BitMap[] bitMaps = new BitMap[columns];
    for (int i = 0; i < columns; i++) {
      boolean hasBitMap = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
      if (hasBitMap) {
        final int size = ReadWriteIOUtils.readInt(byteBuffer);
        final Binary valueBinary = ReadWriteIOUtils.readBinary(byteBuffer);
        bitMaps[i] = new BitMap(size, valueBinary.getValues());
      }
    }
    return bitMaps;
  }

  /**
   * @param byteBuffer data values
   * @param columnTypes
   * @param columns column number
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static Object[] readTabletValuesFromBuffer(
      ByteBuffer byteBuffer,
      TSDataType[] types,
      List<ColumnType> columnTypes,
      int columns,
      int rowSize) {
    Object[] values = new Object[columns];
    for (int i = 0; i < columns; i++) {
      boolean isValueColumnsNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));

      if (isValueColumnsNotNull) {
        switch (types[i]) {
          case BOOLEAN:
            boolean[] boolValues = new boolean[rowSize];
            for (int index = 0; index < rowSize; index++) {
              boolValues[index] = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
            }
            values[i] = boolValues;
            break;
          case INT32:
            int[] intValues = new int[rowSize];
            for (int index = 0; index < rowSize; index++) {
              intValues[index] = ReadWriteIOUtils.readInt(byteBuffer);
            }
            values[i] = intValues;
            break;
          case DATE:
            LocalDate[] dateValues = new LocalDate[rowSize];
            for (int index = 0; index < rowSize; index++) {
              dateValues[index] =
                  DateUtils.parseIntToLocalDate(ReadWriteIOUtils.readInt(byteBuffer));
            }
            values[i] = dateValues;
            break;
          case INT64:
          case TIMESTAMP:
            long[] longValues = new long[rowSize];
            for (int index = 0; index < rowSize; index++) {
              longValues[index] = ReadWriteIOUtils.readLong(byteBuffer);
            }
            values[i] = longValues;
            break;
          case FLOAT:
            float[] floatValues = new float[rowSize];
            for (int index = 0; index < rowSize; index++) {
              floatValues[index] = ReadWriteIOUtils.readFloat(byteBuffer);
            }
            values[i] = floatValues;
            break;
          case DOUBLE:
            double[] doubleValues = new double[rowSize];
            for (int index = 0; index < rowSize; index++) {
              doubleValues[index] = ReadWriteIOUtils.readDouble(byteBuffer);
            }
            values[i] = doubleValues;
            break;
          case TEXT:
          case STRING:
            ColumnType columnType = columnTypes.get(i);
            if (columnType == ColumnType.MEASUREMENT) {
              Binary[] binaryValues = new Binary[rowSize];
              for (int index = 0; index < rowSize; index++) {
                boolean isNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
                if (isNotNull) {
                  binaryValues[index] = ReadWriteIOUtils.readBinary(byteBuffer);
                } else {
                  binaryValues[index] = Binary.EMPTY_VALUE;
                }
              }
              values[i] = binaryValues;
            } else {
              String[] binaryValues = new String[rowSize];
              for (int index = 0; index < rowSize; index++) {
                boolean isNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
                if (isNotNull) {
                  binaryValues[index] = ReadWriteIOUtils.readString(byteBuffer);
                } else {
                  binaryValues[index] = null;
                }
              }
              values[i] = binaryValues;
            }
            break;
          case BLOB:
            Binary[] binaryValues = new Binary[rowSize];
            for (int index = 0; index < rowSize; index++) {
              boolean isNotNull = BytesUtils.byteToBool(ReadWriteIOUtils.readByte(byteBuffer));
              if (isNotNull) {
                binaryValues[index] = ReadWriteIOUtils.readBinary(byteBuffer);
              } else {
                binaryValues[index] = Binary.EMPTY_VALUE;
              }
            }
            values[i] = binaryValues;
            break;
          default:
            throw new UnSupportedDataTypeException(
                String.format(
                    "data type %s is not supported when convert data at client", types[i]));
        }
      }
    }
    return values;
  }

  /**
   * Note that the function will judge 2 {@link Tablet}s to be equal when their contents are
   * logically the same. Namely, a {@link Tablet} with {@link BitMap} "null" may be equal to another
   * {@link Tablet} with 3 columns and
   * {@link BitMap "[null, null, null]", and a {@link Tablet} with rowSize 2 is judged identical to
   * other {@link Tablet}s regardless of any timeStamps with indexes larger than or equal to 2.
   *
   * @param o the tablet to compare
   * @return {@code true} if the tablets are logically equal
   */
  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || !getClass().equals(o.getClass())) {
      return false;
    }
    Tablet that = (Tablet) o;

    boolean flag =
        that.rowSize == rowSize
            && Objects.equals(that.insertTargetName, insertTargetName)
            && Objects.equals(that.schemas, schemas)
            && Objects.equals(that.columnTypes, columnTypes)
            && Objects.equals(that.measurementIndex, measurementIndex);
    if (!flag) {
      return false;
    }

    // assert timestamps and bitmaps
    int columns = (schemas == null ? 0 : schemas.size());
    if (!isTimestampsEqual(this.timestamps, that.timestamps, rowSize)
        || !isBitMapsEqual(this.bitMaps, that.bitMaps, columns)) {
      return false;
    }

    // assert values
    Object[] thatValues = that.values;
    if (thatValues == values) {
      return true;
    }
    if (thatValues == null || values == null) {
      return false;
    }
    if (thatValues.length != values.length) {
      return false;
    }
    for (int i = 0, n = values.length; i < n; i++) {
      if (thatValues[i] == values[i]) {
        continue;
      }
      if (thatValues[i] == null || values[i] == null) {
        return false;
      }
      if (!thatValues[i].getClass().equals(values[i].getClass())) {
        return false;
      }

      switch (schemas.get(i).getType()) {
        case INT32:
          int[] thisIntValues = (int[]) values[i];
          int[] thatIntValues = (int[]) thatValues[i];
          if (thisIntValues.length < rowSize || thatIntValues.length < rowSize) {
            return false;
          }
          for (int j = 0; j < rowSize; j++) {
            if (thisIntValues[j] != thatIntValues[j]) {
              return false;
            }
          }
          break;
        case DATE:
          LocalDate[] thisDateValues = (LocalDate[]) values[i];
          LocalDate[] thatDateValues = (LocalDate[]) thatValues[i];
          if (thisDateValues.length < rowSize || thatDateValues.length < rowSize) {
            return false;
          }
          for (int j = 0; j < rowSize; j++) {
            if (!thisDateValues[j].equals(thatDateValues[j])) {
              return false;
            }
          }
          break;
        case INT64:
        case TIMESTAMP:
          long[] thisLongValues = (long[]) values[i];
          long[] thatLongValues = (long[]) thatValues[i];
          if (thisLongValues.length < rowSize || thatLongValues.length < rowSize) {
            return false;
          }
          for (int j = 0; j < rowSize; j++) {
            if (thisLongValues[j] != thatLongValues[j]) {
              return false;
            }
          }
          break;
        case FLOAT:
          float[] thisFloatValues = (float[]) values[i];
          float[] thatFloatValues = (float[]) thatValues[i];
          if (thisFloatValues.length < rowSize || thatFloatValues.length < rowSize) {
            return false;
          }
          for (int j = 0; j < rowSize; j++) {
            if (thisFloatValues[j] != thatFloatValues[j]) {
              return false;
            }
          }
          break;
        case DOUBLE:
          double[] thisDoubleValues = (double[]) values[i];
          double[] thatDoubleValues = (double[]) thatValues[i];
          if (thisDoubleValues.length < rowSize || thatDoubleValues.length < rowSize) {
            return false;
          }
          for (int j = 0; j < rowSize; j++) {
            if (thisDoubleValues[j] != thatDoubleValues[j]) {
              return false;
            }
          }
          break;
        case BOOLEAN:
          boolean[] thisBooleanValues = (boolean[]) values[i];
          boolean[] thatBooleanValues = (boolean[]) thatValues[i];
          if (thisBooleanValues.length < rowSize || thatBooleanValues.length < rowSize) {
            return false;
          }
          for (int j = 0; j < rowSize; j++) {
            if (thisBooleanValues[j] != thatBooleanValues[j]) {
              return false;
            }
          }
          break;
        case TEXT:
        case STRING:
          ColumnType columnType = columnTypes.get(i);
          if (columnType == ColumnType.MEASUREMENT) {
            Binary[] thisBinaryValues = (Binary[]) values[i];
            Binary[] thatBinaryValues = (Binary[]) thatValues[i];
            if (thisBinaryValues.length < rowSize || thatBinaryValues.length < rowSize) {
              return false;
            }
            for (int j = 0; j < rowSize; j++) {
              if (!thisBinaryValues[j].equals(thatBinaryValues[j])) {
                return false;
              }
            }
          } else {
            String[] thisStringValues = (String[]) values[i];
            String[] thatStringValues = (String[]) thatValues[i];
            if (thisStringValues.length < rowSize || thatStringValues.length < rowSize) {
              return false;
            }
            for (int j = 0; j < rowSize; j++) {
              if (!thisStringValues[j].equals(thatStringValues[j])) {
                return false;
              }
            }
          }
          break;
        case BLOB:
          Binary[] thisBinaryValues = (Binary[]) values[i];
          Binary[] thatBinaryValues = (Binary[]) thatValues[i];
          if (thisBinaryValues.length < rowSize || thatBinaryValues.length < rowSize) {
            return false;
          }
          for (int j = 0; j < rowSize; j++) {
            if (!thisBinaryValues[j].equals(thatBinaryValues[j])) {
              return false;
            }
          }
          break;
        default:
          throw new UnSupportedDataTypeException(
              String.format("Data type %s is not supported.", schemas.get(i).getType()));
      }
    }

    return true;
  }

  private boolean isTimestampsEqual(long[] thisTimestamps, long[] thatTimestamps, int rowSize) {
    if (thisTimestamps == thatTimestamps) {
      return true;
    }
    if (thisTimestamps == null || thatTimestamps == null) {
      return false;
    }

    for (int i = 0; i < rowSize; i++) {
      if (thisTimestamps[i] != thatTimestamps[i]) {
        return false;
      }
    }
    return true;
  }

  private boolean isBitMapsEqual(BitMap[] thisBitMaps, BitMap[] thatBitMaps, int columns) {
    if (thisBitMaps == thatBitMaps) {
      return true;
    }
    if (thisBitMaps == null) {
      for (int i = 0; i < columns; i++) {
        if (thatBitMaps[i] != null && !thatBitMaps[i].isAllMarked()) {
          return false;
        }
      }
      return true;
    }
    if (thatBitMaps == null) {
      for (int i = 0; i < columns; i++) {
        if (thisBitMaps[i] != null && !thisBitMaps[i].isAllMarked()) {
          return false;
        }
      }
      return true;
    }

    for (int i = 0; i < columns; i++) {
      if (!thisBitMaps[i].equals(thatBitMaps[i])) {
        return false;
      }
    }
    return true;
  }

  public boolean isNull(int i, int j) {
    return bitMaps != null && bitMaps[j] != null && !bitMaps[j].isMarked(i);
  }

  /**
   * @param i row number
   * @param j column number
   * @return the string format of the i-th value in the j-th column.
   */
  public Object getValue(int i, int j) {
    if (isNull(i, j)) {
      return null;
    }
    switch (schemas.get(j).getType()) {
      case TEXT:
      case STRING:
        if (columnTypes.get(j).equals(ColumnType.MEASUREMENT)) {
          return ((Binary[]) values[j])[i];
        } else {
          return ((String[]) values[j])[i];
        }
      case INT32:
        return ((int[]) values[j])[i];
      case FLOAT:
        return ((float[]) values[j])[i];
      case DOUBLE:
        return ((double[]) values[j])[i];
      case BOOLEAN:
        return ((boolean[]) values[j])[i];
      case INT64:
        return ((long[]) values[j])[i];
      default:
        throw new IllegalArgumentException("Unsupported type: " + schemas.get(j).getType());
    }
  }

  /**
   * Only used when the tablet is used for table-view interfaces。
   *
   * @param i a row number.
   * @return the IDeviceID of the i-th row.
   */
  public IDeviceID getDeviceID(int i) {
    String[] idArray = new String[idColumnRange + 1];
    idArray[0] = insertTargetName;
    for (int j = 0; j < idColumnRange; j++) {
      final Object value = getValue(i, j);
      idArray[j + 1] = value != null ? value.toString() : null;
    }
    return new StringArrayDeviceID(idArray);
  }

  public int getIdColumnRange() {
    return idColumnRange;
  }

  public void setColumnTypes(List<ColumnType> columnTypes) {
    this.columnTypes = columnTypes;
    idColumnRange = 0;
    for (ColumnType columnType : columnTypes) {
      if (columnType.equals(ColumnType.MEASUREMENT)) {
        break;
      }
      idColumnRange++;
    }
  }

  public enum ColumnType {
    ID,
    MEASUREMENT,
    ATTRIBUTE;

    public static List<ColumnType> nCopy(ColumnType type, int n) {
      List<ColumnType> result = new ArrayList<>(n);
      for (int i = 0; i < n; i++) {
        result.add(type);
      }
      return result;
    }
  }

  /**
   * A tree-interface.
   *
   * @return the insertTargetName as the deviceId
   */
  public String getDeviceId() {
    return insertTargetName;
  }

  /**
   * A tree-interface.
   *
   * @param deviceId set the deviceId as the insertTargetName
   */
  public void setDeviceId(String deviceId) {
    this.insertTargetName = deviceId;
  }

  public String getTableName() {
    return insertTargetName;
  }

  /**
   * A table-interface.
   *
   * @param tableName set the tableName as the insertTargetName
   */
  public void setTableName(String tableName) {
    this.insertTargetName = tableName;
  }

  public List<ColumnType> getColumnTypes() {
    return columnTypes;
  }
}
