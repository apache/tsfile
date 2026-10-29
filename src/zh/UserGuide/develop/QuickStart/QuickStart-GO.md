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
# 快速上手 - Go

## 依赖与安装

Go 包 `github.com/apache/tsfile/go/tsfile` 是基于 C++ 实现的 cgo 绑定，因此必须先有一份 C/C++ 构建产物，Go 代码才能编译。
获取方式有两种：

- 安装我们发布的 C/C++ 包或 SDK，无需自行构建 C++，适用于 Linux 和 macOS。
- 从源码构建 C++ 库，适用于所有平台；Windows 上只支持这一种方式，Go 绑定是在 MinGW 下针对源码构建验证的。

两种方式下 Go 模块都从 GitHub 获取，目前尚未发布到模块代理。

### 依赖

- Go >= 1.22，且 `CGO_ENABLED=1`
- C/C++ 编译工具链和 CMake，仅在从源码构建原生库时需要
- JDK >= 17 和 Maven，仅在从源码构建原生库时需要
- Windows 上受支持的 cgo 工具链为 MinGW-w64

### 方式一：使用已发布的 C/C++ 包（Linux / macOS）

按[下载与安装页面](../../../Download/README.md#cpp-packages)安装运行库和开发包，可以是 SDK 压缩包、DEB/RPM 包或 Homebrew Formula。
头文件安装在 `include/tsfile` 子目录下，因此需要把 cgo 的头文件路径指向该目录，库路径指向安装前缀的库目录：

```bash
export TSFILE_HOME=/path/to/tsfile-sdk   # 或：$(brew --prefix local/tsfile250/tsfile)
export CGO_CFLAGS="-I$TSFILE_HOME/include/tsfile"
export CGO_LDFLAGS="-L$TSFILE_HOME/lib -Wl,-rpath,$TSFILE_HOME/lib"
```

其中 `-rpath` 让编译出的程序在运行时能找到 `libtsfile`；如果共享库安装在系统标准目录下可以去掉。
对于安装在 `/usr` 下的系统包，链接器无需 `-L` 就能找到库，但头文件仍需要 `-I/usr/include/tsfile`。
这些环境变量会追加到包内自带的编译参数之后，而包内参数指向源码检出的 `cpp/target/build` 目录；使用已安装的包时
该目录并不存在，链接器会打印一条搜索路径警告，可以忽略。

cgo 只在编译该包时读取这些变量，因此需要在每个构建程序的终端里设置，或者用
`go env -w CGO_CFLAGS=... CGO_LDFLAGS=...` 记录下来。运行已经编译好的程序时不需要它们。

### 方式二：从源码构建原生库

克隆源码，并在仓库根目录构建 C++ 库：

```bash
git clone https://github.com/apache/tsfile.git
cd tsfile
./mvnw -P with-cpp -DskipTests package
```

构建完成后会在 `cpp/target/build/include` 生成头文件、在 `cpp/target/build/lib` 生成库，Go 包默认就链接这里。

Windows 上需要使用 MinGW 构建，并把构建输出目录和 MinGW 运行库目录加入 `PATH`。Windows 的链接器不会记录库搜索路径，
缺少这两项时 Go 程序启动会因找不到 `tsfile.dll` 而失败：

```powershell
.\mvnw.cmd -P with-cpp -Dcpp.toolchain=mingw -DskipTests package
$env:Path = "$PWD\cpp\target\build\lib;C:\mingw64\bin;$env:Path"
```

每打开一个新的终端，都要在运行 Go 程序前执行上面的 `PATH` 设置，或者把 `cpp\target\build\lib\tsfile.dll`
复制到编译出的可执行文件旁边。`C:\mingw64\bin` 指 MinGW-w64 的安装目录，它提供 MinGW 构建所依赖的 GCC 运行库 DLL。

### 在项目中使用该模块

使用已安装的包（方式一）时，可以直接在你的 Go 项目中拉取模块：

```bash
go get github.com/apache/tsfile/go@develop
go mod tidy
```

模块目前还没有发布 tag，`@develop` 会解析成分支头部的伪版本号。为了构建可复现，请把该伪版本号记录在 `go.mod` 中，
或者固定到某个具体 commit。

使用源码构建（方式二）时，用 `replace` 指向检出目录下的 `go` 目录，Go 包就会使用它旁边构建好的头文件和库：

```bash
go mod edit -replace github.com/apache/tsfile/go=/path/to/tsfile/go
go get github.com/apache/tsfile/go
```

如果想直接运行自带的示例，在检出目录的 `go` 目录下执行：

```bash
cd go
go run ./examples/table_read_write
```

## 写入流程

### 构造 Writer

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

`NewWriter` 会创建或截断文件。`WithMemoryThreshold` 是可选参数，默认为 128 MB。

### 写入数据

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

Tablet 的行和列下标都从 0 开始。每个 setter 都会校验下标和列类型，当下标越界或写入值与声明类型不符时返回错误，应与
`AddTimestamp` 一样检查这些错误。

也可以通过 Arrow record batch 或 table 写入数据。其 schema 必须包含 `int64` 或 `timestamp[ns]`（不带时区）类型的
`time` 列，其后紧跟表 schema 中的全部列：

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

### 关闭文件

```go
writer.Close()
```

### 示例代码

使用这些接口的示例代码可以在以下链接中找到
<https://github.com/apache/tsfile/blob/develop/go/examples/table_read_write/main.go>

## 查询流程

### 构造 Reader

```go
reader, err := tsfile.NewReader("test.tsfile")
if err != nil {
	return err
}
defer reader.Close()
```

### 构造 Query Request

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

时间范围、Tag 过滤、offset、limit 和批次大小都是可选参数，均会下推到原生读取器。

### 查询数据

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

结果集的列下标从 1 开始：第 1 列是时间戳，其后按查询顺序排列所选列。类型化 getter（`Bool`、`Int32`、`Int64`、
`Float32`、`Float64`、`String`、`Bytes`）在列类型不匹配时返回错误，因此需要像上例一样按元数据分发。

### 以 Arrow 批次查询数据

当 `WithBatchSize` 为正数时，查询运行在批次模式下，数据以 Arrow record batch 的形式返回：

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

### 关闭文件

```go
result.Close()
reader.Close()
```

关闭 reader 时也会关闭由它创建的所有结果集。

### 示例代码

使用这些接口的示例代码可以在以下链接中找到
<https://github.com/apache/tsfile/blob/develop/go/examples/table_read_write/main.go>

> 注意：以上读写示例均基于表模型接口，接口定义介绍可见[Go 接口定义](./InterfaceDefinition/InterfaceDefinition-Go.md)。
> 若需了解树模型相关内容，请联系我们。
