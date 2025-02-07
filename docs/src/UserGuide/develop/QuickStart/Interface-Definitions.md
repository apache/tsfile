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
# Interface Definitions

## Write Interface

### ITsFileWriter

Used to write data to tsfile

```Java
interface ITsFileWriter extends AutoCloseable {
  // Write data
  void write(Tablet tablet);
  
  // Close Write
  void close();
}
```

### TsFileWriterBuilder

Used to construct ITsFileWriter

```Java
class TsFileWriterBuilder {
  // Build ITsFileWriter object
  public ITsFileWriter build();
  
  // target file
  public TsFileWriterBuilder file(File file);
  
  // Used to construct table structures
  public TsFileWriterBuilder tableSchema(TableSchema schema);
  
  // Used to limit the memory size of objects
  public TsFileWriterBuilder memoryThreshold(long memoryThreshold);
}
```

### TableSchema

Describe the data structure of the table schema

```Java
class TableSchema {
  // Constructor function
  public TableSchema(String tableName, List<ColumnSchema> columnSchemaList);
}

class ColumnSchema {
  // Constructor function
  public ColumnSchema(String columnName, TSDataType dataType, ColumnCategory columnCategory);
  
  // Get column names
  public String getColumnName();
  
  // Get the data type of the column
  public TSDataType getDataType();
  
  // Get column category
  public Tablet.ColumnCategory getColumnCategory();
}

class ColumnSchemaBuilder {
  // Build ColumnSchema object
  public ColumnSchema build();
  
  // Column Name
  public ColumnSchemaBuilder name(String columnName);
  
  // The data type of the column
  public ColumnSchemaBuilder dataType(TSDataType columnType);
  
  // Column category
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
  // Constructor function
  public Tablet(List<String> columnNameList, List<TSDataType> dataTypeList);
  public Tablet(List<String> columnNameList, List<TSDataType> dataTypeList, int maxRowNum);
  
  // Interface for adding timestamps
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
  // Used to execute queries and return results
  ResultSet query(String tableName, List<String> columnNames, long startTime, long endTime);
  
  // Return the schema of the table named tableName in tsfile
  Optional<TableSchema> getTableSchemas(String tableName);
  
  // Retrieve schema information for all tables in the tsfile
  List<TableSchema> getAllTableSchema();
  
  // Close query
  void close();
}
```

### TsFileReaderBuilder

Used to construct ITsFileWriter

```Java
class TsFileReaderBuilder {
  // Build ITsFileReader object
  public ITsFileReader build();
  
  // target file
  public TsFileReaderBuilder file(File file);
}
```

### ResultSet

The result set of the query

```Java
interface ResultSet extends AutoCloseable {  
  // Move the cursor to the next row and return whether there is still data
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
     
  // Determine whether a column is NULL in the current row
  boolean isNull(String columnName);  
  boolean isNull(int columnIndex);  
    
  // Close the current structure set
  void close();
    
  // Obtain the header of the result set
  ResultSetMetadata getMetadata();
}
```

### ResultSetMetadata

Used to obtain metadata for the result set

```Java
interface ResultSetMetadata {  
  // Obtain the column name of the Nth column in the result set
  String getColumnName(int columnIndex);
  
  // Obtain the data type of the Nth column in the result set
  TSDataType getColumnType(int columnIndex);
}
```
