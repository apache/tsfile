<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at
    
        http://www.apache.org/licenses/LICENSE-2.0
    
    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->
# Interface Definitions - Java

## Write Interface

### ITsFileWriter

Used to write data to tsfile

```Java
interface ITsFileWriter extends AutoCloseable {
  /**
   * Write one tablet into TsFile.
   *
   * @param tablet input tablet.
   */
  void write(Tablet tablet);
  
  /**
   * Close writer and flush buffered data.
   */
  void close();
}
```

### TsFileWriterBuilder

Used to construct ITsFileWriter

```Java
class TsFileWriterBuilder {
  /**
   * Build writer instance from configured options.
   *
   * @return ITsFileWriter instance.
   */
  public ITsFileWriter build();
  
  /**
   * Set target file.
   *
   * @param file target file.
   * @return builder itself.
   */
  public TsFileWriterBuilder file(File file);
  
  /**
   * Set table schema.
   *
   * @param schema table schema.
   * @return builder itself.
   */
  public TsFileWriterBuilder tableSchema(TableSchema schema);
  
  /**
   * Set memory threshold for internal buffering.
   *
   * @param memoryThreshold threshold in bytes.
   * @return builder itself.
   */
  public TsFileWriterBuilder memoryThreshold(long memoryThreshold);
}
```

### TableSchema

Describe the data structure of the table schema

```Java
class TableSchema {
  /**
   * Construct table schema.
   *
   * @param tableName table name.
   * @param columnSchemaList column schema list.
   */
  public TableSchema(String tableName, List<ColumnSchema> columnSchemaList);
}

class ColumnSchema {
  /**
   * Construct one column schema.
   *
   * @param columnName column name.
   * @param dataType column data type.
   * @param columnCategory column category.
   */
  public ColumnSchema(String columnName, TSDataType dataType, ColumnCategory columnCategory);
  
  /**
   * Get column name.
   *
   * @return column name.
   */
  public String getColumnName();
  
  /**
   * Get column data type.
   *
   * @return TSDataType.
   */
  public TSDataType getDataType();
  
  /**
   * Get column category.
   *
   * @return column category.
   */
  public Tablet.ColumnCategory getColumnCategory();
}

class ColumnSchemaBuilder {
  /**
   * Build ColumnSchema object.
   *
   * @return ColumnSchema instance.
   */
  public ColumnSchema build();
  
  /**
   * Set column name.
   *
   * @param columnName column name.
   * @return builder itself.
   */
  public ColumnSchemaBuilder name(String columnName);
  
  /**
   * Set column data type.
   *
   * @param columnType data type.
   * @return builder itself.
   */
  public ColumnSchemaBuilder dataType(TSDataType columnType);
  
  /**
   * Set column category.
   *
   * @param columnCategory column category.
   * @return builder itself.
   */
  public ColumnSchemaBuilder category(ColumnCategory columnCategory);
  
  // Supported types
  enum TSDataType {    
    BOOLEAN,    
    INT32,
    INT64,    
    FLOAT,    
    DOUBLE,    
    TIMESTAMP,   
    TEXT,    
    DATE,    
    BLOB,   
    STRING;  
  }
  
  // Supported column categories
  enum ColumnCategory {    
    TAG,   
    FIELD 
  }
}
```

### Tablet

Write column memory structure

```Java
class Tablet {
  /**
   * Construct tablet with default row capacity.
   *
   * @param columnNameList column names.
   * @param dataTypeList column data types.
   */
  public Tablet(List<String> columnNameList, List<TSDataType> dataTypeList);
  /**
   * Construct tablet with explicit row capacity.
   *
   * @param columnNameList column names.
   * @param dataTypeList column data types.
   * @param maxRowNum max row count.
   */
  public Tablet(List<String> columnNameList, List<TSDataType> dataTypeList, int maxRowNum);
  
  /**
   * Set timestamp for one row.
   *
   * @param rowIndex target row index.
   * @param timestamp timestamp value.
   */
  void addTimestamp(int rowIndex, long timestamp);
  
  // Interface for adding values
  // Add values based on column names
  void addValue(int rowIndex, String columnName, int val);
  void addValue(int rowIndex, String columnName, long val);  
  void addValue(int rowIndex, String columnName, float val);  
  void addValue(int rowIndex, String columnName, double val);  
  void addValue(int rowIndex, String columnName, boolean val);  
  void addValue(int rowIndex, String columnName, String val);
  void addValue(int rowIndex, String columnName, byte[] val); 
  void addValue(int rowIndex, String columnName, LocalDate val); 
  // Add values based on index position
  void addValue(int rowIndex, int columnIndex, int val);
  void addValue(int rowIndex, int columnIndex, long val);  
  void addValue(int rowIndex, int columnIndex, float val);  
  void addValue(int rowIndex, int columnIndex, double val);  
  void addValue(int rowIndex, int columnIndex, boolean val); 
  void addValue(int rowIndex, int columnIndex, String val);
  void addValue(int rowIndex, int columnIndex, byte[] val); 
  void addValue(int rowIndex, int columnIndex, LocalDate val);
}
```

## Read Interface

### ITsFileReader

Used to query data in tsfile

```Java
interface ITsFileReader extends AutoCloseable {
  /**
   * Query table data in time range.
   *
   * @param tableName table name.
   * @param columnNames selected columns.
   * @param startTime start timestamp.
   * @param endTime end timestamp.
   * @return query result set.
   */
  ResultSet query(String tableName, List<String> columnNames, long startTime, long endTime);

  /**
   * Query table data in time range with tag filter.
   *
   * @param tableName table name.
   * @param columnNames selected columns.
   * @param startTime start timestamp.
   * @param endTime end timestamp.
   * @param tagFilter tag filter.
   * @return query result set.
   */
  ResultSet query(String tableName, List<String> columnNames, long startTime, long endTime, Filter tagFilter);
  
  /**
   * Get schema of one table.
   *
   * @param tableName table name.
   * @return optional table schema.
   */
  Optional<TableSchema> getTableSchemas(String tableName);
  
  /**
   * Get schemas of all tables.
   *
   * @return list of table schemas.
   */
  List<TableSchema> getAllTableSchema();
  
  /**
   * Close reader.
   */
  void close();
}
```

### TsFileReaderBuilder

Used to construct ITsFileReader

```Java
class TsFileReaderBuilder {
  /**
   * Build reader instance.
   *
   * @return ITsFileReader instance.
   */
  public ITsFileReader build();
  
  /**
   * Set target file.
   *
   * @param file target tsfile.
   * @return builder itself.
   */
  public TsFileReaderBuilder file(File file);
}
```

### ResultSet

The result set of the query

```Java
interface ResultSet extends AutoCloseable {  
  /**
   * Move to next row.
   *
   * @return true if next row exists.
   */
  boolean next();  
    
  // Get the value of the current row and a certain column
  int getInt(String columnName);  
  int getInt(int columnIndex);  
  long getLong(String columnName);  
  long getLong(int columnIndex);
  float getFloat(String columnName);
  float getFloat(int columnIndex); 
  double getDouble(String columnName);
  double getDouble(int columnIndex); 
  boolean getBoolean(String columnName);
  boolean getBoolean(int columnIndex);
  String getString(String columnName);
  String getString(int columnIndex);
  LocalDate getDate(String columnName);
  LocalDate getDate(int columnIndex); 
  byte[] getBinary(String columnName);
  byte[] getBinary(int columnIndex);
     
  /**
   * Check if current-row column is NULL.
   *
   * @param columnName column name.
   * @return true if null.
   */
  boolean isNull(String columnName);  
  /**
   * Check if current-row column is NULL.
   *
   * @param columnIndex 1-based column index.
   * @return true if null.
   */
  boolean isNull(int columnIndex);  
    
  /**
   * Close result set.
   */
  void close();
    
  /**
   * Get result-set metadata.
   *
   * @return ResultSetMetadata.
   */
  ResultSetMetadata getMetadata();
}
```

### ResultSetMetadata

Used to obtain metadata for the result set

```Java
interface ResultSetMetadata {  
  /**
   * Get column name by index.
   *
   * @param columnIndex 1-based column index.
   * @return column name.
   */
  String getColumnName(int columnIndex);
  
  /**
   * Get column type by index.
   *
   * @param columnIndex 1-based column index.
   * @return TSDataType.
   */
  TSDataType getColumnType(int columnIndex);
}
```
### Filter
#### TagFilterBuilder
Used to construct tag-based filters for querying data
```java
class TagFilterBuilder {
  // Constructor function
  public TagFilterBuilder(TableSchema tableSchema);
  
  // Equal to filter
  public Filter eq(String columnName, Object value);
  
  // Not equal to filter
  public Filter neq(String columnName, Object value);
  
  // Less than filter
  public Filter lt(String columnName, Object value);
  
  // Less than or equal to filter
  public Filter lteq(String columnName, Object value);
  
  // Greater than filter
  public Filter gt(String columnName, Object value);
  
  // Greater than or equal to filter
  public Filter gteq(String columnName, Object value);
  
  // Between and filter (inclusive)
  public Filter betweenAnd(String columnName, Object value1, Object value2);
  
  // Not between and filter
  public Filter notBetweenAnd(String columnName, Object value1, Object value2);
  
  // Regular expression filter
  public Filter regExp(String columnName, String pattern);
  
  // Not regular expression filter
  public Filter notRegExp(String columnName, String pattern);
  
  // LIKE pattern filter
  public Filter like(String columnName, String pattern);
  
  // NOT LIKE pattern filter
  public Filter notLike(String columnName, String pattern);
  
  // Logical AND filter
  public Filter and(Filter left, Filter right);
  
  // Logical OR filter
  public Filter or(Filter left, Filter right);
  
  // Logical NOT filter
  public Filter not(Filter value);
}
```