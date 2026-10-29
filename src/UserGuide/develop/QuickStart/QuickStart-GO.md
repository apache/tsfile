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
# Quick Start - Go

## Dependencies and Installation

The Go package `github.com/apache/tsfile/go/tsfile` is a cgo binding over the C++ implementation, so a C/C++
build of TsFile must be available before Go code can be built. There are two ways to get it:

- Install the published C/C++ packages or SDK. No C++ build is needed; this works on Linux, macOS, and Windows.
- Build the C++ library from source. Use this to change TsFile or on a platform without a published package.

The Go module itself is fetched from GitHub in both cases; it is not published to the module proxy yet.

### Dependencies

- Go >= 1.22 with `CGO_ENABLED=1`
- A C/C++ toolchain and CMake, needed only when building the native library from source
- JDK >= 17 and Maven, needed only when building the native library from source
- On Windows, MinGW-w64 is the supported cgo toolchain

### Option 1: Use the published C/C++ packages (Linux, macOS, Windows)

Install the runtime and development files as described on the
[download and installation page](../../../Download/README.md#cpp-packages), for example the SDK archive, the DEB/RPM
packages, the Homebrew formula, or the Windows ZIP. Headers are installed under an `include/tsfile` subdirectory,
so point cgo at that directory and at the library directory of the install prefix:

```bash
export TSFILE_HOME=/path/to/tsfile-sdk   # or: $(brew --prefix local/tsfile250/tsfile)
export CGO_CFLAGS="-I$TSFILE_HOME/include/tsfile"
export CGO_LDFLAGS="-L$TSFILE_HOME/lib -Wl,-rpath,$TSFILE_HOME/lib"
```

The `-rpath` entry lets the compiled program find `libtsfile` at run time; drop it when the shared library is
installed in a standard system directory. For a system installation under `/usr`, the compiler finds the library
without `-L`, but the headers still need `-I/usr/include/tsfile`. The environment variables are added to the
package's own flags, whose paths point at a source checkout under `cpp/target/build`; when you use the installed
packages those paths do not exist, and the linker prints a search-path warning that can be ignored.

On Windows, extract the ZIP and set the same variables in PowerShell. The ZIP ships `bin\tsfile.dll` and an import
library in `lib\tsfile.lib` that the MinGW linker accepts, so `-ltsfile` resolves through `-L` as usual:

```powershell
$env:TSFILE_HOME = "C:\path\to\tsfile-2.5.0-windows-x86_64"
$env:CGO_CFLAGS = "-I$env:TSFILE_HOME\include\tsfile"
$env:CGO_LDFLAGS = "-L$env:TSFILE_HOME\lib -ltsfile"
$env:Path = "$env:TSFILE_HOME\bin;$env:Path"
```

Windows has no run-time library search path, so the compiled program finds `tsfile.dll` through `PATH`; keep
`bin` on `PATH`, or copy `tsfile.dll` next to the executable. The DLL links the C runtime statically and depends
only on the system libraries, so no Visual C++ redistributable is required.

cgo reads these variables when it compiles the package, so set them in every shell where you build a program that
links against the installed library, or record them with `go env -w CGO_CFLAGS=... CGO_LDFLAGS=...`. They are not
needed when running an already built program.

### Option 2: Build the native library from source

Clone the source code and build the C++ library in the repository root:

```bash
git clone https://github.com/apache/tsfile.git
cd tsfile
./mvnw -P with-cpp -DskipTests package
```

This produces the headers under `cpp/target/build/include` and the library under `cpp/target/build/lib` that the
Go package links against by default.

On Windows, build with MinGW. MinGW-w64 is this project's default C++ generator on Windows, and it is the compiler
that cgo uses for the Go package, so `-Dcpp.toolchain=mingw` makes the build produce a DLL and an import library that
match the Go toolchain directly. The build writes `tsfile.dll` to `cpp\target\build\lib` instead of a package's
`bin` directory, so point `PATH` there (or copy the DLL next to the executable) in addition to the MinGW-w64 `bin`
directory that provides the compiler:

```powershell
.\mvnw.cmd -P with-cpp -Dcpp.toolchain=mingw -DskipTests package
$env:Path = "$PWD\cpp\target\build\lib;C:\mingw64\bin;$env:Path"
```

Run that `PATH` assignment in every new terminal before building or running Go programs.

### Use the module in your project

With the installed packages (option 1), fetch the module directly in your Go project:

```bash
go get github.com/apache/tsfile/go@develop
go mod tidy
```

The module has no tagged release yet, so `@develop` resolves to a pseudo-version of the branch head. Record that
pseudo-version in `go.mod`, or pin a specific commit, for reproducible builds.

With a source build (option 2), replace the module with the `go` directory of your checkout; the package then
finds the headers and the library built next to it:

```bash
go mod edit -replace github.com/apache/tsfile/go=/path/to/tsfile/go
go get github.com/apache/tsfile/go
```

To run the bundled example instead, from the `go` directory of the checkout:

```bash
cd go
go run ./examples/table_read_write
```

## Writing Process

### Construct Writer

```go
schema := tsfile.TableSchema{
	Table: "table1",
	Columns: []tsfile.ColumnSchema{
		{Name: "id1", DataType: tsfile.DataTypeString, Category: tsfile.ColumnCategoryTag},
		{Name: "id2", DataType: tsfile.DataTypeString, Category: tsfile.ColumnCategoryTag},
		{Name: "s1", DataType: tsfile.DataTypeInt32, Category: tsfile.ColumnCategoryField},
		{Name: "s2", DataType: tsfile.DataTypeBoolean, Category: tsfile.ColumnCategoryField},
	},
}

writer, err := tsfile.NewWriter("test.tsfile", schema, tsfile.WithMemoryThreshold(10*1024*1024))
if err != nil {
	return err
}
defer writer.Close()
```

`NewWriter` creates or truncates the file. `WithMemoryThreshold` is optional and defaults to 128 MB.

### Write Data

```go
tablet, err := tsfile.NewTablet([]tsfile.TabletColumn{
	{Name: "id1", DataType: tsfile.DataTypeString},
	{Name: "id2", DataType: tsfile.DataTypeString},
	{Name: "s1", DataType: tsfile.DataTypeInt32},
	{Name: "s2", DataType: tsfile.DataTypeBoolean},
}, 5)
if err != nil {
	return err
}
defer tablet.Close()

for row := 0; row < 5; row++ {
	if err := tablet.AddTimestamp(row, int64(row)); err != nil {
		return err
	}
	tablet.SetString(row, 0, "id1_filed_1")
	tablet.SetString(row, 1, "id2_filed_1")
	tablet.SetInt32(row, 2, int32(row))
	tablet.SetBool(row, 3, true)
}

if err := writer.WriteTableTablet(tablet); err != nil {
	return err
}
```

Tablet rows and columns are zero-based. Every setter validates the index and the column type, and returns an error
when the row or column is out of range or the value does not match the declared type; check those errors in the same
way as `AddTimestamp`.

Data can also be written from an Arrow record batch or table. The schema must contain a `time` column of type
`int64` or `timestamp[ns]` without a time zone, followed by exactly the columns of the table schema:

```go
arrowSchema := arrow.NewSchema([]arrow.Field{
	{Name: "time", Type: arrow.PrimitiveTypes.Int64},
	{Name: "id1", Type: arrow.BinaryTypes.String},
	{Name: "id2", Type: arrow.BinaryTypes.String},
	{Name: "s1", Type: arrow.PrimitiveTypes.Int32},
	{Name: "s2", Type: arrow.FixedWidthTypes.Boolean},
}, nil)
builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
defer builder.Release()

timeBuilder := builder.Field(0).(*array.Int64Builder)
id1Builder := builder.Field(1).(*array.StringBuilder)
id2Builder := builder.Field(2).(*array.StringBuilder)
s1Builder := builder.Field(3).(*array.Int32Builder)
s2Builder := builder.Field(4).(*array.BooleanBuilder)
for row := 5; row < 10; row++ {
	timeBuilder.Append(int64(row))
	id1Builder.Append("id1_filed_1")
	id2Builder.Append("id2_filed_1")
	s1Builder.Append(int32(row))
	s2Builder.Append(true)
}

record := builder.NewRecord()
defer record.Release()
if err := writer.WriteArrowBatch(record); err != nil {
	return err
}
```

### Close File

```go
writer.Close()
```

### Sample Code

The sample code of using these interfaces is in
<https://github.com/apache/tsfile/blob/develop/go/examples/table_read_write/main.go>

## Query Process

### Construct Reader

```go
reader, err := tsfile.NewReader("test.tsfile")
if err != nil {
	return err
}
defer reader.Close()
```

### Construct Query Request

```go
result, err := reader.Query(
	"table1",
	[]string{"id1", "id2", "s1", "s2"},
	tsfile.WithTimeRange(2, 8),
	tsfile.WithTagFilter(tsfile.And(tsfile.Eq("id1", "id1_filed_1"), tsfile.Eq("id2", "id2_filed_1"))),
	tsfile.WithLimit(100),
)
if err != nil {
	return err
}
defer result.Close()
```

Time range, tag filter, offset, limit, and batch size are all optional and are pushed down to the native reader.

### Query Data

```go
metadata := result.Metadata()
for _, column := range metadata {
	fmt.Printf("%s(%d) ", column.Name, column.DataType)
}
fmt.Println()

for {
	hasNext, err := result.Next()
	if err != nil {
		return err
	}
	if !hasNext {
		break
	}
	for column := 1; column <= len(metadata); column++ {
		isNull, err := result.IsNull(column)
		if err != nil {
			return err
		}
		if isNull {
			fmt.Print("null ")
			continue
		}
		switch metadata[column-1].DataType {
		case tsfile.DataTypeBoolean:
			value, err := result.Bool(column)
			if err != nil {
				return err
			}
			fmt.Print(value, " ")
		case tsfile.DataTypeInt32, tsfile.DataTypeDate:
			value, err := result.Int32(column)
			if err != nil {
				return err
			}
			fmt.Print(value, " ")
		case tsfile.DataTypeInt64, tsfile.DataTypeTimestamp:
			value, err := result.Int64(column)
			if err != nil {
				return err
			}
			fmt.Print(value, " ")
		case tsfile.DataTypeFloat:
			value, err := result.Float32(column)
			if err != nil {
				return err
			}
			fmt.Print(value, " ")
		case tsfile.DataTypeDouble:
			value, err := result.Float64(column)
			if err != nil {
				return err
			}
			fmt.Print(value, " ")
		case tsfile.DataTypeString, tsfile.DataTypeText:
			value, err := result.String(column)
			if err != nil {
				return err
			}
			fmt.Print(value, " ")
		}
	}
	fmt.Println()
}
```

Result-set columns are one-based; column 1 is the timestamp and the selected columns follow in query order. The
typed getters (`Bool`, `Int32`, `Int64`, `Float32`, `Float64`, `String`, `Bytes`) reject a column whose data type
does not match, so dispatch on the metadata as shown above.

### Query Data as Arrow Batches

When `WithBatchSize` is positive the query runs in batch mode and rows are returned as Arrow record batches:

```go
result, err := reader.Query("table1", []string{"id1", "id2", "s1", "s2"}, tsfile.WithBatchSize(1024))
if err != nil {
	return err
}
defer result.Close()

for {
	record, err := result.ReadArrowRecordBatch()
	if errors.Is(err, io.EOF) {
		break
	}
	if err != nil {
		return err
	}
	fmt.Println(record.NumRows())
	record.Release()
}
```

### Close File

```go
result.Close()
reader.Close()
```

Closing the reader also closes every result set created from it.

### Sample Code

The sample code of using these interfaces is in
<https://github.com/apache/tsfile/blob/develop/go/examples/table_read_write/main.go>

> Note: The above read/write examples are all based on the table model interface. For details about the interface
definition, please refer to [Go Interface Definition](./InterfaceDefinition/InterfaceDefinition-Go.md). If you need
information regarding the tree model, please contact us.
