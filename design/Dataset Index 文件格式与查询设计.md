# Dataset Index 文件格式与查询设计

**核心查询链路。** `table name + optional schema_fingerprint → table_id → device name → device_id → measurement name → column_id → series_id → SeriesFileSpan → SeriesLocator → DeviceFileSpan → TsFileReader`。Runtime 不硬编码任何业务 section 的绝对 offset；所有业务地址都来自 SectionDirectory。

## Header 与 SectionDirectory

|Header 字段|offset|大小|类型 / 本例值|语义|
|---|---|---|---|---|
|`magic`|`0x00–0x08`|8 B|`byte[8]`|`TSIDX\0\0\0`，逐字节比较，不按整数解释|
|`version_major`|`0x08–0x0A`|2 B|`u16 = 1`|不兼容的主版本直接拒绝|
|`version_minor`|`0x0A–0x0C`|2 B|`u16 = 0`|同一主版本内的兼容扩展|
|`header_size`|`0x0C–0x10`|4 B|`u32 = 64`|当前 v1 Header 编码长度；当前 parser 要求为 64 B|
|`directory_offset`|`0x10–0x18`|8 B|`u64 = 0x40`|SectionDirectory 的实际位置|
|`section_count`|`0x18–0x1C`|4 B|`u32 = 14`|当前文件实际包含的 DirectoryEntry 数量，不作为永久上限|
|`directory_entry_size`|`0x1C–0x20`|4 B|`u32 = 32`|当前 v1 DirectoryEntry 编码长度；当前 parser 要求为 32 B|
|`file_length`|`0x20–0x28`|8 B|`u64`|必须等于实际索引文件长度|
|`header_crc32c`|`0x28–0x2C`|4 B|`u32`|发现关键 offset 被随机损坏；计算时该字段按 0|
|`reserved`|`0x2C–0x40`|20 B|`byte[20] = 0`|一个连续保留区，不再拆分|

端序固定为 little-endian，排序约束也是 v1 格式强制条件，因此两者不占 flags。当前 parser 支持 major=1、minor=0、header_size=64、directory_entry_size=32 和当前文件的 section_count=14；这些是当前编码值和兼容条件，不是业务容量或架构永久上限。未来新增可选 section、扩大 Header 或 DirectoryEntry 时，通过 version、size 字段和 SectionDirectory 定义兼容规则；不兼容变化升级 major version。UUID、snapshot_epoch 与 build_time 不进入核心格式；索引通过“构建临时文件 → fsync → 原子替换”发布，Runtime 用索引文件 identity / fingerprint 区分快照。

|DirectoryEntry 字段|相对 offset|大小|类型|用途|
|---|---|---|---|---|
|`section_type`|`+0`|4 B|`u32`|Section 枚举；Runtime 按 type 建立 SectionView|
|`record_size`|`+4`|4 B|`u32`|定长记录大小；StringBytes 等 blob 为 0|
|`offset`|`+8`|8 B|`u64`|Section 相对索引文件起点的实际 offset|
|`length`|`+16`|8 B|`u64`|Section 字节长度|
|`count`|`+24`|4 B|`u32`|记录数；必须满足 `count × record_size ≤ length`|
|`crc32c`|`+28`|4 B|`u32`<br>|该 Section 的 CRC32C；不再需要独立 SectionChecksum|

当前 14 条 DirectoryEntry 位于 `0x40–0x200`；首个业务 Section 从 `align64(0x200) = 0x200` 开始。后续位置统一由 `next.offset = align64(current.offset + current.length)` 生成。这里的数值只描述当前 v1 编码；`TableRecord` 等业务 section 的 offset 必须读取对应 DirectoryEntry，不能写死为 `0x02C0`。

## 当前 v1 查询所需的 14 个 Section

|\#|Section|record\_size|保存内容|排序 / 访问约束|
|---|---|---|---|---|
|1|`StringOffsets`|4 B|offset\[0\.\.N\]:u32；每项 4 B，共 4 × \(N\+1\) B，N 是去重字符串数；offset\[N\] 是 StringBytes 的有效长度|sid 的字符串为 `StringBytes[offset[sid]..offset[sid+1])`|
|2|`StringBytes`<br>|0|byte\[length\]；变长 Section，每个对象是一段不含结尾 NUL 的 canonical UTF\-8 bytes，由相邻两个 StringOffsets 界定|单池小于 4 GiB；超出时分池或升级格式<br>|
|3|`TableNameIndex`<br>|16 B<br>|`name_hash:u64, name_sid:u32, table_id:u32；每行 16 B，表示一个 canonical table name 到一个逻辑 table_id 的映射`|按 \(name\_hash, name\_bytes\) 排序并允许同名多条；二分得到同名 table\_id 等值区间，再读取各 TableRecord 的 schema\_fingerprint 区分 schema variant|
|4|`TableRecord`<br>|48 B<br>|name\_sid:u32, reserved0:u32, schema\_fingerprint:u64, first\_device\_name\_index:u32, device\_count:u32, first\_column\_name\_index:u32, column\_count:u32, first\_file\_span:u32, file\_span\_count:u32, reserved1:u64；每行 48 B，描述一个 schema variant 及其 device\-name、column\-name、TableFileSpan 三个连续子区间|record ID 就是 table\_id<br>|
|5|`DeviceNameIndex`<br>|24 B<br>|`table_id:u32, device_id:u32, name_hash:u64, name_sid:u32, reserved:u32；每行 24 B，表示某一 table 内 canonical device name 到 device_id 的映射`|先按 table\_id 聚簇，再按 name\_hash/name\_bytes 排序|
|6|`DeviceRecord`<br>|48 B|table\_id:u32, name\_sid:u32, reserved0:u32, reserved1:u32, first\_series\_id:u32, series\_count:u32, first\_file\_span:u32, file\_span\_count:u32, min\_time:i64, max\_time:i64；每行 48 B，描述一个逻辑设备、它实际存在的连续 series 区间、跨文件区间及全局时间边界|record ID 就是 device\_id<br>|
|7|`ColumnNameIndex`|24 B|`table_id:u32, column_id:u32, name_hash:u64, name_sid:u32, reserved:u32；每行 24 B，表示某一 table 内 measurement name 到 column_id 的映射`|在 TableRecord 给出的 measurement 列索引区间内二分；时间列为隐式列|
|8|`ColumnSchema`|32 B|table\_id:u32, name\_sid:u32, column\_ordinal:u32, logical\_type:u16, physical\_type:u16, encoding:u16, compression:u16, role:u16, nullable:u16, reserved:u64；每行 32 B，描述 canonical 列定义|record ID 就是 column\_id；同一 table schema variant 只保存一份 canonical schema，实际 chunk 解码参数以 SeriesLocator/DeviceFileSpan 指向的 value/time TimeseriesMetadata 与 ChunkHeader 为准；aligned time 的 VECTOR 仅是物理时间列标记，不写入 value 列的 physical\_type|
|9|`LogicalSeries`|32 B<br>|`device_id:u32, column_id:u32, first_file_span:u32, file_span_count:u32, min_time:i64, max_time:i64；每行 32 B，表示一个真实存在的逻辑 series 及其连续物理 fragment 区间`|按 `(device_id, column_id)` 聚簇排序；在设备连续区间内二分，稀疏列不生成空记录<br>|
|10|`TsFileRecord`<br>|48 B|path\_sid:u32, reserved0:u32, file\_size:u64, min\_time:i64, max\_time:i64, file\_fingerprint:u64, reserved1:u64；每行 48 B，描述一个 TsFile 身份与文件级裁剪信息；TsFile 自身格式版本由 Reader 打开文件时校验，不在 Dataset Index 重复保存|record ID 就是 file\_id；文件身份由 path、file\_size 与 fingerprint 联合校验，不在本版本定义覆盖优先级|
|11|`TableFileSpan`<br>|32 B|table\_id:u32, file\_id:u32, min\_time:i64, max\_time:i64, row\_count:u64；每行 32 B，表示一张逻辑表在一个 TsFile 中的最小/最大时间及该表所有设备时间轴行数之和，用于表级文件裁剪与规模估算|按 `(table_id, min_time, file_id)` 聚簇排序<br>|
|12|`DeviceFileSpan`|48 B|device\_id:u32, file\_id:u32, time\_meta\_offset:u64, time\_meta\_length:u32, layout:u16, flags:u16, min\_time:i64, max\_time:i64, row\_count:u64；每行 48 B，表示一个逻辑设备在一个 TsFile 中的时间轴与共享 time metadata。layout 为 0=non\-aligned、1=aligned；non\-aligned 的 time\_meta\_offset/time\_meta\_length 必须为 0；aligned 时二者给出共享 time TimeseriesMetadata 的精确字节范围。flags 的 bit 0 表示构建期已验证 time/value chunk 可按 ordinal 一一配对，其余位必须为 0|按 \(device\_id, min\_time, file\_id\) 聚簇排序；同一个 device/file 的 aligned value locator 共享本记录中的 time metadata range|
|13|`SeriesFileSpan`|48 B|`series_id:u32, file_id:u32, locator_id:u32, reserved:u32, min_time:i64, max_time:i64, prefix_max_time:i64, point_count:u64；每行 48 B，表示一个逻辑 series 在一个 TsFile 中的物理 fragment、时间边界、累计重叠裁剪值和实际 value 数`|按 `(series_id, min_time, file_id)` 聚簇；prefix\_max\_time 支持重叠区间二分<br>|
|14|`SeriesLocator`|24 B|`device_file_span_id:u32, locator_kind:u16, flags:u16, timeseries_meta_offset:u64, timeseries_meta_length:u32, chunk_count:u32；每行 24 B。locator_kind 为 0=non-aligned TimeseriesMetadata、1=aligned value TimeseriesMetadata；flags 的 bit 0 表示 chunk_count 有效，其余位必须为 0。offset/length 始终指向一条完整、精确的 TimeseriesMetadata，而不是 measurement index block 或内部 chunk metadata 子区间`|record ID 就是 locator\_id；由 device\_file\_span\_id 取得 file\_id、layout 和共享 time metadata，再把精确 TimeseriesMetadata range 交给 TsFileReader|

**同名表与 schema variant。** `TableNameIndex` 不要求 table name 唯一：每一种规范化 schema 生成独立 `TableRecord`，同名记录形成连续等值区间。`schema_fingerprint` 由有序的 TAG/FIELD 名称、角色、逻辑类型和 nullable 属性计算；不同 TsFile 仅改变 encoding/compression 时仍属于同一逻辑 schema，实际物理解码参数以 SeriesLocator/DeviceFileSpan 指向的 value/time TimeseriesMetadata 与 ChunkHeader 为准。查询只给 table name 且等值区间中存在多个 fingerprint 时，Runtime 必须返回“schema variant 不明确”，由调用方显式选择 fingerprint；不得把 TAG/FIELD 集合或类型不同的表静默合并。以后如需 schema union，应在 Dataset 逻辑层显式定义列对齐与缺失值规则，不能改变本索引的精确映射。

## 核心记录布局

|Record|固定大小|字段|查询作用|
|---|---|---|---|
|`TableRecord`|48 B|`name_sid:u32, reserved0:u32, schema_fingerprint:u64, first_device_name_index:u32, device_count:u32, first_column_name_index:u32, column_count:u32, first_file_span:u32, file_span_count:u32, reserved1:u64`|限定设备名和 measurement 名的二分范围，并支持表级文件裁剪|
|`DeviceRecord`|48 B|`table_id:u32, name_sid:u32, reserved0:u32, reserved1:u32, first_series_id:u32, series_count:u32, first_file_span:u32, file_span_count:u32, min_time:i64, max_time:i64`|把设备和它的连续 LogicalSeries / DeviceFileSpan 关联起来|
|`ColumnSchema`|32 B|`table_id:u32, name_sid:u32, column_ordinal:u32, logical_type:u16, physical_type:u16, encoding:u16, compression:u16, role:u16, nullable:u16, reserved:u64`|提供 measurement 的逻辑类型与实际 value 类型、角色、nullable 属性和表内 ordinal；aligned time 的 VECTOR 物理标记不作为 value 列类型|
|`LogicalSeries`|32 B|`device_id:u32, column_id:u32, first_file_span:u32, file_span_count:u32, min_time:i64, max_time:i64`|直接取得该序列的物理文件区间|
|`SeriesFileSpan`|48 B|`series_id:u32, file_id:u32, locator_id:u32, reserved:u32, min_time:i64, max_time:i64, prefix_max_time:i64, point_count:u64`|按查询时间过滤 TsFile；prefix\_max\_time 处理文件时间范围重叠|
|`SeriesLocator`|24 B|`device_file_span_id:u32, locator_kind:u16, flags:u16, timeseries_meta_offset:u64, timeseries_meta_length:u32, chunk_count:u32`|locator\_kind 区分 non\-aligned metadata 与 aligned value metadata；通过 device\_file\_span\_id 复用共享 time metadata，不遍历 TsFile 文件级 metadata index 即可构造目标 TimeseriesIndex|

## A / B / C 的完整示例

本例完整定义如下：`shared_table` 的 schema 为 `s1:INT64`、`s2:DOUBLE`；A 保存 d0、d1 的 `[0,99]`，B 保存 d0、d2 的 `[100,199]`。A 另含 `table_A/a0/mA:INT64`，B 另含 `table_B/b0/mB:DOUBLE`，C 含 `table_C1/c0/mC1:INT64` 与 `table_C2/c1/mC2:DOUBLE`，这四条序列均取 `[0,99]`。每个设备在每个文件中有 100 行，每条 measurement 有 100 个 value。本节是 non\-aligned 基线示例；aligned 的共享时间轴与稀疏 value 示例在下一节单独给出。下表沿用上方六列表格样式，每个索引对象独占一行，列出 14 个 Section 的全部逻辑对象和物理 fragment。本例首个业务 Section 从 0x0200 开始，各 Section 按目录顺序以 64 B 对齐；“索引内直接地址”均由 section\_offset \+ record\_id × record\_size 计算。TsFile 内部 offset 仅为本例数值。

|Section / record|对象|索引内直接地址|实际保存值|关联记录|含义|
|---|---|---|---|---|---|
|**StringOffsets \+ StringBytes：每个 sid 一行，同时给出 offset 项与 UTF\-8 字节范围**||||||
|`StringPool[0]`|/dataset/A\.tsfile<br>|`offset[0]@0x0200；bytes[0x0280..0x0291)`|`offset=0，next_offset=17，UTF-8=/dataset/A.tsfile`|sid=0；StringOffsets\[0\.\.1\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[1]`|/dataset/B\.tsfile<br>|`offset[1]@0x0204；bytes[0x0291..0x02A2)`|`offset=17，next_offset=34，UTF-8=/dataset/B.tsfile`|sid=1；StringOffsets\[1\.\.2\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[2]`|/dataset/C\.tsfile<br>|`offset[2]@0x0208；bytes[0x02A2..0x02B3)`|`offset=34，next_offset=51，UTF-8=/dataset/C.tsfile`|sid=2；StringOffsets\[2\.\.3\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[3]`|shared\_table<br>|`offset[3]@0x020C；bytes[0x02B3..0x02BF)`|`offset=51，next_offset=63，UTF-8=shared_table`|sid=3；StringOffsets\[3\.\.4\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[4]`|table\_A<br>|`offset[4]@0x0210；bytes[0x02BF..0x02C6)`|`offset=63，next_offset=70，UTF-8=table_A`|sid=4；StringOffsets\[4\.\.5\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[5]`|table\_B|`offset[5]@0x0214；bytes[0x02C6..0x02CD)`|`offset=70，next_offset=77，UTF-8=table_B`|sid=5；StringOffsets\[5\.\.6\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[6]`|table\_C1|`offset[6]@0x0218；bytes[0x02CD..0x02D5)`|`offset=77，next_offset=85，UTF-8=table_C1`|sid=6；StringOffsets\[6\.\.7\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[7]`|table\_C2|`offset[7]@0x021C；bytes[0x02D5..0x02DD)`|`offset=85，next_offset=93，UTF-8=table_C2`|sid=7；StringOffsets\[7\.\.8\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[8]`|shared\_table/d0|`offset[8]@0x0220；bytes[0x02DD..0x02EC)`|`offset=93，next_offset=108，UTF-8=shared_table/d0`|sid=8；StringOffsets\[8\.\.9\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[9]`|shared\_table/d1|`offset[9]@0x0224；bytes[0x02EC..0x02FB)`|`offset=108，next_offset=123，UTF-8=shared_table/d1`|sid=9；StringOffsets\[9\.\.10\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[10]`|shared\_table/d2|`offset[10]@0x0228；bytes[0x02FB..0x030A)`|`offset=123，next_offset=138，UTF-8=shared_table/d2`|sid=10；StringOffsets\[10\.\.11\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[11]`|table\_A/a0|`offset[11]@0x022C；bytes[0x030A..0x0314)`|`offset=138，next_offset=148，UTF-8=table_A/a0`|sid=11；StringOffsets\[11\.\.12\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[12]`|table\_B/b0|`offset[12]@0x0230；bytes[0x0314..0x031E)`|`offset=148，next_offset=158，UTF-8=table_B/b0`|sid=12；StringOffsets\[12\.\.13\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[13]`|table\_C1/c0|`offset[13]@0x0234；bytes[0x031E..0x0329)`|`offset=158，next_offset=169，UTF-8=table_C1/c0`|sid=13；StringOffsets\[13\.\.14\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[14]`|table\_C2/c1|`offset[14]@0x0238；bytes[0x0329..0x0334)`|`offset=169，next_offset=180，UTF-8=table_C2/c1`|sid=14；StringOffsets\[14\.\.15\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[15]`|s1<br>|`offset[15]@0x023C；bytes[0x0334..0x0336)`|`offset=180，next_offset=182，UTF-8=s1`|sid=15；StringOffsets\[15\.\.16\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[16]`|s2|`offset[16]@0x0240；bytes[0x0336..0x0338)`|`offset=182，next_offset=184，UTF-8=s2`|sid=16；StringOffsets\[16\.\.17\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[17]`|mA|`offset[17]@0x0244；bytes[0x0338..0x033A)`|`offset=184，next_offset=186，UTF-8=mA`|sid=17；StringOffsets\[17\.\.18\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[18]`|mB|`offset[18]@0x0248；bytes[0x033A..0x033C)`|`offset=186，next_offset=188，UTF-8=mB`|sid=18；StringOffsets\[18\.\.19\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[19]`|mC1|`offset[19]@0x024C；bytes[0x033C..0x033F)`|`offset=188，next_offset=191，UTF-8=mC1`|sid=19；StringOffsets\[19\.\.20\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|`StringPool[20]`|mC2|`offset[20]@0x0250；bytes[0x033F..0x0342)`|`offset=191，next_offset=194，UTF-8=mC2`|sid=20；StringOffsets\[20\.\.21\]|相邻两个 u32 offset 直接切出字符串；不保存结尾 NUL|
|**TableNameIndex：每个 canonical table name / schema variant 一行**||||||
|`TableNameIndex[0]`|shared\_table<br>|`0x0380 + 0×16 = 0x0380`|`name_hash=H(shared_table)，name_sid=3，table_id=0`<br>|TableRecord\[0\]|示例行按名称展示；文件内实际按 \(name\_hash,name\_bytes\) 排序|
|`TableNameIndex[1]`|table\_A<br>|`0x0380 + 1×16 = 0x0390`|`name_hash=H(table_A)，name_sid=4，table_id=1`<br>|TableRecord\[1\]|示例行按名称展示；文件内实际按 \(name\_hash,name\_bytes\) 排序|
|`TableNameIndex[2]`|table\_B<br>|`0x0380 + 2×16 = 0x03A0`|`name_hash=H(table_B)，name_sid=5，table_id=2`<br>|TableRecord\[2\]|示例行按名称展示；文件内实际按 \(name\_hash,name\_bytes\) 排序|
|`TableNameIndex[3]`|table\_C1|`0x0380 + 3×16 = 0x03B0`|`name_hash=H(table_C1)，name_sid=6，table_id=3`<br>|TableRecord\[3\]|示例行按名称展示；文件内实际按 \(name\_hash,name\_bytes\) 排序|
|`TableNameIndex[4]`|table\_C2<br>|`0x0380 + 4×16 = 0x03C0`|`name_hash=H(table_C2)，name_sid=7，table_id=4`|TableRecord\[4\]<br>|示例行按名称展示；文件内实际按 \(name\_hash,name\_bytes\) 排序|
|**TableRecord：5 个 schema variant；shared\_table 跨 A、B 仍只保存一条逻辑记录**||||||
|`TableRecord[0]`<br>|shared\_table<br>|`0x0400 + 0×48 = 0x0400`<br>|`name_sid=3, reserved0=0, schema_fingerprint=fp_shared, first_device_name_index=0, device_count=3, first_column_name_index=0, column_count=2, first_file_span=0, file_span_count=2, reserved1=0`|DeviceNameIndex\[0\.\.2\]；ColumnNameIndex\[0\.\.1\]；TableFileSpan\[0\.\.1\]|record ID 即 table\_id|
|`TableRecord[1]`|table\_A<br>|`0x0400 + 1×48 = 0x0430`|`name_sid=4, reserved0=0, schema_fingerprint=fp_table_A, first_device_name_index=3, device_count=1, first_column_name_index=2, column_count=1, first_file_span=2, file_span_count=1, reserved1=0`|DeviceNameIndex\[3\.\.3\]；ColumnNameIndex\[2\.\.2\]；TableFileSpan\[2\.\.2\]|record ID 即 table\_id|
|`TableRecord[2]`|table\_B|`0x0400 + 2×48 = 0x0460`|`name_sid=5, reserved0=0, schema_fingerprint=fp_table_B, first_device_name_index=4, device_count=1, first_column_name_index=3, column_count=1, first_file_span=3, file_span_count=1, reserved1=0`|DeviceNameIndex\[4\.\.4\]；ColumnNameIndex\[3\.\.3\]；TableFileSpan\[3\.\.3\]|record ID 即 table\_id|
|`TableRecord[3]`|table\_C1|`0x0400 + 3×48 = 0x0490`|`name_sid=6, reserved0=0, schema_fingerprint=fp_table_C1, first_device_name_index=5, device_count=1, first_column_name_index=4, column_count=1, first_file_span=4, file_span_count=1, reserved1=0`|DeviceNameIndex\[5\.\.5\]；ColumnNameIndex\[4\.\.4\]；TableFileSpan\[4\.\.4\]|record ID 即 table\_id|
|`TableRecord[4]`|table\_C2|`0x0400 + 4×48 = 0x04C0`|`name_sid=7, reserved0=0, schema_fingerprint=fp_table_C2, first_device_name_index=6, device_count=1, first_column_name_index=5, column_count=1, first_file_span=5, file_span_count=1, reserved1=0`|DeviceNameIndex\[6\.\.6\]；ColumnNameIndex\[5\.\.5\]；TableFileSpan\[5\.\.5\]|record ID 即 table\_id|
|**DeviceNameIndex：每个 table 内的 canonical device name 一行**||||||
|`DeviceNameIndex[0]`|shared\_table/d0|`0x0500 + 0×24 = 0x0500`|`table_id=0, device_id=0, name_hash=H(shared_table/d0), name_sid=8, reserved=0`|DeviceRecord\[0\]|在 TableRecord 限定的连续区间内按 hash/name 二分|
|`DeviceNameIndex[1]`|shared\_table/d1|`0x0500 + 1×24 = 0x0518`|`table_id=0, device_id=1, name_hash=H(shared_table/d1), name_sid=9, reserved=0`|DeviceRecord\[1\]|在 TableRecord 限定的连续区间内按 hash/name 二分|
|`DeviceNameIndex[2]`|shared\_table/d2|`0x0500 + 2×24 = 0x0530`|`table_id=0, device_id=2, name_hash=H(shared_table/d2), name_sid=10, reserved=0`|DeviceRecord\[2\]|在 TableRecord 限定的连续区间内按 hash/name 二分|
|`DeviceNameIndex[3]`|table\_A/a0|`0x0500 + 3×24 = 0x0548`|`table_id=1, device_id=3, name_hash=H(table_A/a0), name_sid=11, reserved=0`|DeviceRecord\[3\]|在 TableRecord 限定的连续区间内按 hash/name 二分|
|`DeviceNameIndex[4]`|table\_B/b0|`0x0500 + 4×24 = 0x0560`|`table_id=2, device_id=4, name_hash=H(table_B/b0), name_sid=12, reserved=0`|DeviceRecord\[4\]|在 TableRecord 限定的连续区间内按 hash/name 二分|
|`DeviceNameIndex[5]`|table\_C1/c0|`0x0500 + 5×24 = 0x0578`|`table_id=3, device_id=5, name_hash=H(table_C1/c0), name_sid=13, reserved=0`|DeviceRecord\[5\]|在 TableRecord 限定的连续区间内按 hash/name 二分|
|`DeviceNameIndex[6]`|table\_C2/c1|`0x0500 + 6×24 = 0x0590`|`table_id=4, device_id=6, name_hash=H(table_C2/c1), name_sid=14, reserved=0`|DeviceRecord\[6\]|在 TableRecord 限定的连续区间内按 hash/name 二分|
|**DeviceRecord：7 个逻辑设备；d0 横跨 A、B 仍只保存一条逻辑记录**||||||
|`DeviceRecord[0]`|shared\_table/d0<br>|`0x05C0 + 0×48 = 0x05C0`<br>|`table_id=0, name_sid=8, reserved0=0, reserved1=0, first_series_id=0, series_count=2, first_file_span=0, file_span_count=2, min_time=0, max_time=199`|LogicalSeries\[0\.\.1\]；DeviceFileSpan\[0\.\.1\]<br>|record ID 即 device\_id|
|`DeviceRecord[1]`|shared\_table/d1|`0x05C0 + 1×48 = 0x05F0`|`table_id=0, name_sid=9, reserved0=0, reserved1=0, first_series_id=2, series_count=2, first_file_span=2, file_span_count=1, min_time=0, max_time=99`|LogicalSeries\[2\.\.3\]；DeviceFileSpan\[2\.\.2\]|record ID 即 device\_id|
|`DeviceRecord[2]`|shared\_table/d2|`0x05C0 + 2×48 = 0x0620`|`table_id=0, name_sid=10, reserved0=0, reserved1=0, first_series_id=4, series_count=2, first_file_span=3, file_span_count=1, min_time=100, max_time=199`|LogicalSeries\[4\.\.5\]；DeviceFileSpan\[3\.\.3\]|record ID 即 device\_id|
|`DeviceRecord[3]`|table\_A/a0|`0x05C0 + 3×48 = 0x0650`<br>|`table_id=1, name_sid=11, reserved0=0, reserved1=0, first_series_id=6, series_count=1, first_file_span=4, file_span_count=1, min_time=0, max_time=99`|LogicalSeries\[6\.\.6\]；DeviceFileSpan\[4\.\.4\]|record ID 即 device\_id|
|`DeviceRecord[4]`|table\_B/b0|`0x05C0 + 4×48 = 0x0680`|`table_id=2, name_sid=12, reserved0=0, reserved1=0, first_series_id=7, series_count=1, first_file_span=5, file_span_count=1, min_time=0, max_time=99`|LogicalSeries\[7\.\.7\]；DeviceFileSpan\[5\.\.5\]|record ID 即 device\_id|
|`DeviceRecord[5]`|table\_C1/c0|`0x05C0 + 5×48 = 0x06B0`|`table_id=3, name_sid=13, reserved0=0, reserved1=0, first_series_id=8, series_count=1, first_file_span=6, file_span_count=1, min_time=0, max_time=99`|LogicalSeries\[8\.\.8\]；DeviceFileSpan\[6\.\.6\]|record ID 即 device\_id|
|`DeviceRecord[6]`|table\_C2/c1|`0x05C0 + 6×48 = 0x06E0`|`table_id=4, name_sid=14, reserved0=0, reserved1=0, first_series_id=9, series_count=1, first_file_span=7, file_span_count=1, min_time=0, max_time=99`|LogicalSeries\[9\.\.9\]；DeviceFileSpan\[7\.\.7\]|record ID 即 device\_id|
|**ColumnNameIndex：每个 schema variant 内的 measurement name 一行**||||||
|`ColumnNameIndex[0]`|shared\_table\.s1|`0x0740 + 0×24 = 0x0740`<br>|`table_id=0, column_id=0, name_hash=H(s1), name_sid=15, reserved=0`|ColumnSchema\[0\]|在 TableRecord 限定的连续区间内按 hash/name 二分|
|`ColumnNameIndex[1]`|shared\_table\.s2|`0x0740 + 1×24 = 0x0758`|`table_id=0, column_id=1, name_hash=H(s2), name_sid=16, reserved=0`|ColumnSchema\[1\]|在 TableRecord 限定的连续区间内按 hash/name 二分|
|`ColumnNameIndex[2]`|table\_A\.mA|`0x0740 + 2×24 = 0x0770`|`table_id=1, column_id=2, name_hash=H(mA), name_sid=17, reserved=0`|ColumnSchema\[2\]|在 TableRecord 限定的连续区间内按 hash/name 二分|
|`ColumnNameIndex[3]`|table\_B\.mB|`0x0740 + 3×24 = 0x0788`|`table_id=2, column_id=3, name_hash=H(mB), name_sid=18, reserved=0`|ColumnSchema\[3\]|在 TableRecord 限定的连续区间内按 hash/name 二分|
|`ColumnNameIndex[4]`|table\_C1\.mC1|`0x0740 + 4×24 = 0x07A0`|`table_id=3, column_id=4, name_hash=H(mC1), name_sid=19, reserved=0`|ColumnSchema\[4\]|在 TableRecord 限定的连续区间内按 hash/name 二分|
|`ColumnNameIndex[5]`|table\_C2\.mC2|`0x0740 + 5×24 = 0x07B8`|`table_id=4, column_id=5, name_hash=H(mC2), name_sid=20, reserved=0`|ColumnSchema\[5\]|在 TableRecord 限定的连续区间内按 hash/name 二分|
|**ColumnSchema：每个 canonical measurement 定义一行**||||||
|`ColumnSchema[0]`|shared\_table\.s1|`0x0800 + 0×32 = 0x0800`|`table_id=0, name_sid=15, column_ordinal=0, logical_type=INT64, physical_type=INT64, encoding=INHERIT_TSFILE(0), compression=INHERIT_TSFILE(0), role=FIELD, nullable=0, reserved=0`|SeriesLocator 指向的 TsFile chunk metadata 提供实际 encoding/compression|record ID 即 column\_id；时间列为隐式列|
|`ColumnSchema[1]`|shared\_table\.s2|`0x0800 + 1×32 = 0x0820`|`table_id=0, name_sid=16, column_ordinal=1, logical_type=DOUBLE, physical_type=DOUBLE, encoding=INHERIT_TSFILE(0), compression=INHERIT_TSFILE(0), role=FIELD, nullable=0, reserved=0`|SeriesLocator 指向的 TsFile chunk metadata 提供实际 encoding/compression|record ID 即 column\_id；时间列为隐式列|
|`ColumnSchema[2]`|table\_A\.mA|`0x0800 + 2×32 = 0x0840`|`table_id=1, name_sid=17, column_ordinal=0, logical_type=INT64, physical_type=INT64, encoding=INHERIT_TSFILE(0), compression=INHERIT_TSFILE(0), role=FIELD, nullable=0, reserved=0`|SeriesLocator 指向的 TsFile chunk metadata 提供实际 encoding/compression|record ID 即 column\_id；时间列为隐式列|
|`ColumnSchema[3]`|table\_B\.mB|`0x0800 + 3×32 = 0x0860`|`table_id=2, name_sid=18, column_ordinal=0, logical_type=DOUBLE, physical_type=DOUBLE, encoding=INHERIT_TSFILE(0), compression=INHERIT_TSFILE(0), role=FIELD, nullable=0, reserved=0`|SeriesLocator 指向的 TsFile chunk metadata 提供实际 encoding/compression|record ID 即 column\_id；时间列为隐式列|
|`ColumnSchema[4]`|table\_C1\.mC1|`0x0800 + 4×32 = 0x0880`|`table_id=3, name_sid=19, column_ordinal=0, logical_type=INT64, physical_type=INT64, encoding=INHERIT_TSFILE(0), compression=INHERIT_TSFILE(0), role=FIELD, nullable=0, reserved=0`|SeriesLocator 指向的 TsFile chunk metadata 提供实际 encoding/compression|record ID 即 column\_id；时间列为隐式列|
|`ColumnSchema[5]`|table\_C2\.mC2|`0x0800 + 5×32 = 0x08A0`|`table_id=4, name_sid=20, column_ordinal=0, logical_type=DOUBLE, physical_type=DOUBLE, encoding=INHERIT_TSFILE(0), compression=INHERIT_TSFILE(0), role=FIELD, nullable=0, reserved=0`|SeriesLocator 指向的 TsFile chunk metadata 提供实际 encoding/compression|record ID 即 column\_id；时间列为隐式列|
|**LogicalSeries：每条真实存在的 device × measurement 一行**||||||
|`LogicalSeries[0]`|d0/s1|`0x08C0 + 0×32 = 0x08C0`|`device_id=0, column_id=0, first_file_span=0, file_span_count=2, min_time=0, max_time=199`|SeriesFileSpan\[0\.\.1\]|按 \(device\_id,column\_id\) 聚簇；不存在的稀疏列不生成记录|
|`LogicalSeries[1]`|d0/s2|`0x08C0 + 1×32 = 0x08E0`|`device_id=0, column_id=1, first_file_span=2, file_span_count=2, min_time=0, max_time=199`|SeriesFileSpan\[2\.\.3\]|按 \(device\_id,column\_id\) 聚簇；不存在的稀疏列不生成记录|
|`LogicalSeries[2]`|d1/s1|`0x08C0 + 2×32 = 0x0900`|`device_id=1, column_id=0, first_file_span=4, file_span_count=1, min_time=0, max_time=99`|SeriesFileSpan\[4\.\.4\]|按 \(device\_id,column\_id\) 聚簇；不存在的稀疏列不生成记录|
|`LogicalSeries[3]`|d1/s2|`0x08C0 + 3×32 = 0x0920`|`device_id=1, column_id=1, first_file_span=5, file_span_count=1, min_time=0, max_time=99`|SeriesFileSpan\[5\.\.5\]|按 \(device\_id,column\_id\) 聚簇；不存在的稀疏列不生成记录|
|`LogicalSeries[4]`|d2/s1|`0x08C0 + 4×32 = 0x0940`|`device_id=2, column_id=0, first_file_span=6, file_span_count=1, min_time=100, max_time=199`|SeriesFileSpan\[6\.\.6\]|按 \(device\_id,column\_id\) 聚簇；不存在的稀疏列不生成记录|
|`LogicalSeries[5]`|d2/s2|`0x08C0 + 5×32 = 0x0960`|`device_id=2, column_id=1, first_file_span=7, file_span_count=1, min_time=100, max_time=199`|SeriesFileSpan\[7\.\.7\]|按 \(device\_id,column\_id\) 聚簇；不存在的稀疏列不生成记录|
|`LogicalSeries[6]`|a0/mA|`0x08C0 + 6×32 = 0x0980`|`device_id=3, column_id=2, first_file_span=8, file_span_count=1, min_time=0, max_time=99`|SeriesFileSpan\[8\.\.8\]|按 \(device\_id,column\_id\) 聚簇；不存在的稀疏列不生成记录|
|`LogicalSeries[7]`|b0/mB|`0x08C0 + 7×32 = 0x09A0`|`device_id=4, column_id=3, first_file_span=9, file_span_count=1, min_time=0, max_time=99`|SeriesFileSpan\[9\.\.9\]|按 \(device\_id,column\_id\) 聚簇；不存在的稀疏列不生成记录|
|`LogicalSeries[8]`|c0/mC1|`0x08C0 + 8×32 = 0x09C0`|`device_id=5, column_id=4, first_file_span=10, file_span_count=1, min_time=0, max_time=99`|SeriesFileSpan\[10\.\.10\]|按 \(device\_id,column\_id\) 聚簇；不存在的稀疏列不生成记录|
|`LogicalSeries[9]`|c1/mC2|`0x08C0 + 9×32 = 0x09E0`|`device_id=6, column_id=5, first_file_span=11, file_span_count=1, min_time=0, max_time=99`|SeriesFileSpan\[11\.\.11\]|按 \(device\_id,column\_id\) 聚簇；不存在的稀疏列不生成记录|
|**TsFileRecord：3 个物理文件；文件版本由 Reader 打开文件时校验**||||||
|`TsFileRecord[0]`|A\.tsfile|`0x0A00 + 0×48 = 0x0A00`|`path_sid=0, reserved0=0, file_size=65536, min_time=0, max_time=99, file_fingerprint=0xA001A001A001A001, reserved1=0`|path=StringPool\[0\]|file\_id 由其 record ID 给出；size/fingerprint 为本例具体示例值|
|`TsFileRecord[1]`|B\.tsfile|`0x0A00 + 1×48 = 0x0A30`|`path_sid=1, reserved0=0, file_size=69632, min_time=100, max_time=199, file_fingerprint=0xB002B002B002B002, reserved1=0`|path=StringPool\[1\]|file\_id 由其 record ID 给出；size/fingerprint 为本例具体示例值|
|`TsFileRecord[2]`|C\.tsfile|`0x0A00 + 2×48 = 0x0A60`|`path_sid=2, reserved0=0, file_size=49152, min_time=0, max_time=99, file_fingerprint=0xC003C003C003C003, reserved1=0`|path=StringPool\[2\]|file\_id 由其 record ID 给出；size/fingerprint 为本例具体示例值|
|**TableFileSpan：每个 table × TsFile 物理出现一行**||||||
|`TableFileSpan[0]`|shared\_table in A|`0x0AC0 + 0×32 = 0x0AC0`|`table_id=0, file_id=0, min_time=0, max_time=99, row_count=200`|TableRecord\[0\]；TsFileRecord\[0\]|表级文件裁剪；row\_count 为该表各设备时间轴行数之和|
|`TableFileSpan[1]`|shared\_table in B|`0x0AC0 + 1×32 = 0x0AE0`|`table_id=0, file_id=1, min_time=100, max_time=199, row_count=200`|TableRecord\[0\]；TsFileRecord\[1\]|表级文件裁剪；row\_count 为该表各设备时间轴行数之和|
|`TableFileSpan[2]`|table\_A in A|`0x0AC0 + 2×32 = 0x0B00`|`table_id=1, file_id=0, min_time=0, max_time=99, row_count=100`|TableRecord\[1\]；TsFileRecord\[0\]|表级文件裁剪；row\_count 为该表各设备时间轴行数之和|
|`TableFileSpan[3]`|table\_B in B|`0x0AC0 + 3×32 = 0x0B20`|`table_id=2, file_id=1, min_time=0, max_time=99, row_count=100`|TableRecord\[2\]；TsFileRecord\[1\]|表级文件裁剪；row\_count 为该表各设备时间轴行数之和|
|`TableFileSpan[4]`|table\_C1 in C|`0x0AC0 + 4×32 = 0x0B40`|`table_id=3, file_id=2, min_time=0, max_time=99, row_count=100`|TableRecord\[3\]；TsFileRecord\[2\]|表级文件裁剪；row\_count 为该表各设备时间轴行数之和|
|`TableFileSpan[5]`|table\_C2 in C|`0x0AC0 + 5×32 = 0x0B60`|`table_id=4, file_id=2, min_time=0, max_time=99, row_count=100`|TableRecord\[4\]；TsFileRecord\[2\]|表级文件裁剪；row\_count 为该表各设备时间轴行数之和|
|**DeviceFileSpan：每个 device × TsFile 物理出现一行**||||||
|`DeviceFileSpan[0]`|d0 in A|`0x0B80 + 0×48 = 0x0B80`|`device_id=0, file_id=0, time_meta_offset=0, time_meta_length=0, layout=0(non-aligned), flags=0, min_time=0, max_time=99, row_count=100`|DeviceRecord\[0\]；TsFileRecord\[0\]|设备级文件裁剪；row\_count 是时间轴行数，不是 measurement 非空点数|
|`DeviceFileSpan[1]`|d0 in B|`0x0B80 + 1×48 = 0x0BB0`|`device_id=0, file_id=1, time_meta_offset=0, time_meta_length=0, layout=0(non-aligned), flags=0, min_time=100, max_time=199, row_count=100`|DeviceRecord\[0\]；TsFileRecord\[1\]|设备级文件裁剪；row\_count 是时间轴行数，不是 measurement 非空点数|
|`DeviceFileSpan[2]`|d1 in A|`0x0B80 + 2×48 = 0x0BE0`|`device_id=1, file_id=0, time_meta_offset=0, time_meta_length=0, layout=0(non-aligned), flags=0, min_time=0, max_time=99, row_count=100`|DeviceRecord\[1\]；TsFileRecord\[0\]|设备级文件裁剪；row\_count 是时间轴行数，不是 measurement 非空点数|
|`DeviceFileSpan[3]`|d2 in B|`0x0B80 + 3×48 = 0x0C10`|`device_id=2, file_id=1, time_meta_offset=0, time_meta_length=0, layout=0(non-aligned), flags=0, min_time=100, max_time=199, row_count=100`|DeviceRecord\[2\]；TsFileRecord\[1\]|设备级文件裁剪；row\_count 是时间轴行数，不是 measurement 非空点数|
|`DeviceFileSpan[4]`|a0 in A|`0x0B80 + 4×48 = 0x0C40`|`device_id=3, file_id=0, time_meta_offset=0, time_meta_length=0, layout=0(non-aligned), flags=0, min_time=0, max_time=99, row_count=100`|DeviceRecord\[3\]；TsFileRecord\[0\]|设备级文件裁剪；row\_count 是时间轴行数，不是 measurement 非空点数|
|`DeviceFileSpan[5]`|b0 in B|`0x0B80 + 5×48 = 0x0C70`|`device_id=4, file_id=1, time_meta_offset=0, time_meta_length=0, layout=0(non-aligned), flags=0, min_time=0, max_time=99, row_count=100`|DeviceRecord\[4\]；TsFileRecord\[1\]|设备级文件裁剪；row\_count 是时间轴行数，不是 measurement 非空点数|
|`DeviceFileSpan[6]`|c0 in C|`0x0B80 + 6×48 = 0x0CA0`|`device_id=5, file_id=2, time_meta_offset=0, time_meta_length=0, layout=0(non-aligned), flags=0, min_time=0, max_time=99, row_count=100`|DeviceRecord\[5\]；TsFileRecord\[2\]|设备级文件裁剪；row\_count 是时间轴行数，不是 measurement 非空点数|
|`DeviceFileSpan[7]`|c1 in C|`0x0B80 + 7×48 = 0x0CD0`|`device_id=6, file_id=2, time_meta_offset=0, time_meta_length=0, layout=0(non-aligned), flags=0, min_time=0, max_time=99, row_count=100`|DeviceRecord\[6\]；TsFileRecord\[2\]|设备级文件裁剪；row\_count 是时间轴行数，不是 measurement 非空点数|
|**SeriesFileSpan：每个 logical series × TsFile fragment 一行**||||||
|`SeriesFileSpan[0]`|d0/s1 in A|`0x0D00 + 0×48 = 0x0D00`|`series_id=0, file_id=0, locator_id=0, reserved=0, min_time=0, max_time=99, prefix_max_time=99, point_count=100`|LogicalSeries\[0\]；SeriesLocator\[0\]|按 \(series\_id,min\_time,file\_id\) 聚簇；prefix\_max\_time 支持重叠时间范围二分|
|`SeriesFileSpan[1]`|d0/s1 in B|`0x0D00 + 1×48 = 0x0D30`|`series_id=0, file_id=1, locator_id=1, reserved=0, min_time=100, max_time=199, prefix_max_time=199, point_count=100`|LogicalSeries\[0\]；SeriesLocator\[1\]|按 \(series\_id,min\_time,file\_id\) 聚簇；prefix\_max\_time 支持重叠时间范围二分|
|`SeriesFileSpan[2]`|d0/s2 in A|`0x0D00 + 2×48 = 0x0D60`|`series_id=1, file_id=0, locator_id=2, reserved=0, min_time=0, max_time=99, prefix_max_time=99, point_count=100`|LogicalSeries\[1\]；SeriesLocator\[2\]|按 \(series\_id,min\_time,file\_id\) 聚簇；prefix\_max\_time 支持重叠时间范围二分|
|`SeriesFileSpan[3]`|d0/s2 in B|`0x0D00 + 3×48 = 0x0D90`|`series_id=1, file_id=1, locator_id=3, reserved=0, min_time=100, max_time=199, prefix_max_time=199, point_count=100`|LogicalSeries\[1\]；SeriesLocator\[3\]|按 \(series\_id,min\_time,file\_id\) 聚簇；prefix\_max\_time 支持重叠时间范围二分|
|`SeriesFileSpan[4]`|d1/s1 in A|`0x0D00 + 4×48 = 0x0DC0`|`series_id=2, file_id=0, locator_id=4, reserved=0, min_time=0, max_time=99, prefix_max_time=99, point_count=100`|LogicalSeries\[2\]；SeriesLocator\[4\]|按 \(series\_id,min\_time,file\_id\) 聚簇；prefix\_max\_time 支持重叠时间范围二分|
|`SeriesFileSpan[5]`|d1/s2 in A|`0x0D00 + 5×48 = 0x0DF0`|`series_id=3, file_id=0, locator_id=5, reserved=0, min_time=0, max_time=99, prefix_max_time=99, point_count=100`|LogicalSeries\[3\]；SeriesLocator\[5\]|按 \(series\_id,min\_time,file\_id\) 聚簇；prefix\_max\_time 支持重叠时间范围二分|
|`SeriesFileSpan[6]`|d2/s1 in B|`0x0D00 + 6×48 = 0x0E20`|`series_id=4, file_id=1, locator_id=6, reserved=0, min_time=100, max_time=199, prefix_max_time=199, point_count=100`|LogicalSeries\[4\]；SeriesLocator\[6\]|按 \(series\_id,min\_time,file\_id\) 聚簇；prefix\_max\_time 支持重叠时间范围二分|
|`SeriesFileSpan[7]`|d2/s2 in B|`0x0D00 + 7×48 = 0x0E50`|`series_id=5, file_id=1, locator_id=7, reserved=0, min_time=100, max_time=199, prefix_max_time=199, point_count=100`|LogicalSeries\[5\]；SeriesLocator\[7\]|按 \(series\_id,min\_time,file\_id\) 聚簇；prefix\_max\_time 支持重叠时间范围二分|
|`SeriesFileSpan[8]`|a0/mA in A|`0x0D00 + 8×48 = 0x0E80`|`series_id=6, file_id=0, locator_id=8, reserved=0, min_time=0, max_time=99, prefix_max_time=99, point_count=100`|LogicalSeries\[6\]；SeriesLocator\[8\]|按 \(series\_id,min\_time,file\_id\) 聚簇；prefix\_max\_time 支持重叠时间范围二分|
|`SeriesFileSpan[9]`|b0/mB in B|`0x0D00 + 9×48 = 0x0EB0`|`series_id=7, file_id=1, locator_id=9, reserved=0, min_time=0, max_time=99, prefix_max_time=99, point_count=100`|LogicalSeries\[7\]；SeriesLocator\[9\]|按 \(series\_id,min\_time,file\_id\) 聚簇；prefix\_max\_time 支持重叠时间范围二分|
|`SeriesFileSpan[10]`|c0/mC1 in C|`0x0D00 + 10×48 = 0x0EE0`|`series_id=8, file_id=2, locator_id=10, reserved=0, min_time=0, max_time=99, prefix_max_time=99, point_count=100`|LogicalSeries\[8\]；SeriesLocator\[10\]|按 \(series\_id,min\_time,file\_id\) 聚簇；prefix\_max\_time 支持重叠时间范围二分|
|`SeriesFileSpan[11]`|c1/mC2 in C|`0x0D00 + 11×48 = 0x0F10`|`series_id=9, file_id=2, locator_id=11, reserved=0, min_time=0, max_time=99, prefix_max_time=99, point_count=100`|LogicalSeries\[9\]；SeriesLocator\[11\]|按 \(series\_id,min\_time,file\_id\) 聚簇；prefix\_max\_time 支持重叠时间范围二分|
|**SeriesLocator：每个 physical fragment 的精确 TimeseriesMetadata range 一行**||||||
|`SeriesLocator[0]`|d0/s1 in A|`0x0F40 + 0×24 = 0x0F40`<br>|`device_file_span_id=0, locator_kind=0(non-aligned), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x1200, timeseries_meta_length=0x80, chunk_count=1`|SeriesFileSpan\[0\]；TsFileRecord\[0\]|本例 offset 指向完整 TimeseriesMetadata；Runtime 从其中解析 ChunkMetadata，再按 offsetOfChunkHeader 读取实际 chunk<br>|
|`SeriesLocator[1]`|d0/s1 in B|`0x0F40 + 1×24 = 0x0F58`|`device_file_span_id=1, locator_kind=0(non-aligned), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x1600, timeseries_meta_length=0x80, chunk_count=1`|SeriesFileSpan\[1\]；TsFileRecord\[1\]|本例 offset 指向完整 TimeseriesMetadata；Runtime 从其中解析 ChunkMetadata，再按 offsetOfChunkHeader 读取实际 chunk|
|`SeriesLocator[2]`|d0/s2 in A<br>|`0x0F40 + 2×24 = 0x0F70`|`device_file_span_id=0, locator_kind=0(non-aligned), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x1280, timeseries_meta_length=0x80, chunk_count=1`|SeriesFileSpan\[2\]；TsFileRecord\[0\]|本例 offset 指向完整 TimeseriesMetadata；Runtime 从其中解析 ChunkMetadata，再按 offsetOfChunkHeader 读取实际 chunk|
|`SeriesLocator[3]`|d0/s2 in B|`0x0F40 + 3×24 = 0x0F88`|`device_file_span_id=1, locator_kind=0(non-aligned), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x1680, timeseries_meta_length=0x80, chunk_count=1`|SeriesFileSpan\[3\]；TsFileRecord\[1\]|本例 offset 指向完整 TimeseriesMetadata；Runtime 从其中解析 ChunkMetadata，再按 offsetOfChunkHeader 读取实际 chunk|
|`SeriesLocator[4]`|d1/s1 in A|`0x0F40 + 4×24 = 0x0FA0`|`device_file_span_id=2, locator_kind=0(non-aligned), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x1300, timeseries_meta_length=0x80, chunk_count=1`|SeriesFileSpan\[4\]；TsFileRecord\[0\]<br>|本例 offset 指向完整 TimeseriesMetadata；Runtime 从其中解析 ChunkMetadata，再按 offsetOfChunkHeader 读取实际 chunk|
|`SeriesLocator[5]`|d1/s2 in A|`0x0F40 + 5×24 = 0x0FB8`|`device_file_span_id=2, locator_kind=0(non-aligned), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x1380, timeseries_meta_length=0x80, chunk_count=1`|SeriesFileSpan\[5\]；TsFileRecord\[0\]|本例 offset 指向完整 TimeseriesMetadata；Runtime 从其中解析 ChunkMetadata，再按 offsetOfChunkHeader 读取实际 chunk|
|`SeriesLocator[6]`|d2/s1 in B|`0x0F40 + 6×24 = 0x0FD0`|`device_file_span_id=3, locator_kind=0(non-aligned), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x1700, timeseries_meta_length=0x80, chunk_count=1`|SeriesFileSpan\[6\]；TsFileRecord\[1\]|本例 offset 指向完整 TimeseriesMetadata；Runtime 从其中解析 ChunkMetadata，再按 offsetOfChunkHeader 读取实际 chunk|
|`SeriesLocator[7]`|d2/s2 in B|`0x0F40 + 7×24 = 0x0FE8`|`device_file_span_id=3, locator_kind=0(non-aligned), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x1780, timeseries_meta_length=0x80, chunk_count=1`|SeriesFileSpan\[7\]；TsFileRecord\[1\]|本例 offset 指向完整 TimeseriesMetadata；Runtime 从其中解析 ChunkMetadata，再按 offsetOfChunkHeader 读取实际 chunk|
|`SeriesLocator[8]`|a0/mA in A|`0x0F40 + 8×24 = 0x1000`|`device_file_span_id=4, locator_kind=0(non-aligned), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x1400, timeseries_meta_length=0x80, chunk_count=1`|SeriesFileSpan\[8\]；TsFileRecord\[0\]|本例 offset 指向完整 TimeseriesMetadata；Runtime 从其中解析 ChunkMetadata，再按 offsetOfChunkHeader 读取实际 chunk|
|`SeriesLocator[9]`|b0/mB in B|`0x0F40 + 9×24 = 0x1018`|`device_file_span_id=5, locator_kind=0(non-aligned), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x1800, timeseries_meta_length=0x80, chunk_count=1`|SeriesFileSpan\[9\]；TsFileRecord\[1\]|本例 offset 指向完整 TimeseriesMetadata；Runtime 从其中解析 ChunkMetadata，再按 offsetOfChunkHeader 读取实际 chunk|
|`SeriesLocator[10]`|c0/mC1 in C|`0x0F40 + 10×24 = 0x1030`|`device_file_span_id=6, locator_kind=0(non-aligned), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x1000, timeseries_meta_length=0x80, chunk_count=1`|SeriesFileSpan\[10\]；TsFileRecord\[2\]|本例 offset 指向完整 TimeseriesMetadata；Runtime 从其中解析 ChunkMetadata，再按 offsetOfChunkHeader 读取实际 chunk|
|`SeriesLocator[11]`|c1/mC2 in C|`0x0F40 + 11×24 = 0x1048`|`device_file_span_id=7, locator_kind=0(non-aligned), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x1080, timeseries_meta_length=0x80, chunk_count=1`|SeriesFileSpan\[11\]；TsFileRecord\[2\]|本例 offset 指向完整 TimeseriesMetadata；Runtime 从其中解析 ChunkMetadata，再按 offsetOfChunkHeader 读取实际 chunk|

## Aligned 序列的物理定位示例

本节是独立的小型示例，只展示与 aligned 有关的记录。A\.tsfile 中的 shared\_table/d0 采用 aligned 布局，共享 100 行时间轴；s1:INT64 有 95 个非空 value，s2:DOUBLE 有 90 个非空 value。时间列在逻辑 ColumnSchema 中保持隐式，但在物理定位中必须由 DeviceFileSpan 保存一份共享 time TimeseriesMetadata range。以下 record ID 为本节局部示意，不改变上一节 non\-aligned 示例的 ID。

|索引记录|对象|实际保存值|作用|
|---|---|---|---|
|`DeviceFileSpan[0]`|d0 in A|`device_id=0, file_id=0, time_meta_offset=0x1100, time_meta_length=0x60, layout=1(aligned), flags=1(ALIGNED_PAIRING_VALIDATED), min_time=0, max_time=99, row_count=100`|共享时间轴只保存一次；s1、s2 的 locator 都引用本记录|
|`SeriesFileSpan[0]`|d0/s1 in A|`series_id=0, file_id=0, locator_id=0, min_time=0, max_time=99, prefix_max_time=99, point_count=95`|point\_count 是非空 value 数，不是 aligned 行数|
|`SeriesLocator[0]`|d0/s1 value metadata|`device_file_span_id=0, locator_kind=1(aligned-value), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x1160, timeseries_meta_length=0x60, chunk_count=2`|与 DeviceFileSpan\[0\] 的 time metadata 组合|
|`SeriesFileSpan[1]`|d0/s2 in A|`series_id=1, file_id=0, locator_id=1, min_time=0, max_time=99, prefix_max_time=99, point_count=90`|同一时间轴可以对应不同非空 value 数|
|`SeriesLocator[1]`|d0/s2 value metadata|`device_file_span_id=0, locator_kind=1(aligned-value), flags=1(CHUNK_COUNT_VALID), timeseries_meta_offset=0x11C0, timeseries_meta_length=0x60, chunk_count=2`|与 s1 共享 DeviceFileSpan\[0\]，不重复保存 time offset|

|TsFile metadata 对象|精确字节范围|ChunkMetadata\[0\]|ChunkMetadata\[1\]|统计语义|
|---|---|---|---|---|
|time TimeseriesMetadata，measurement ID 为空，物理类型标记为 VECTOR|`[0x1100,0x1160)`|`offsetOfChunkHeader=0x2000, min=0, max=49, count=50`|`offsetOfChunkHeader=0x3000, min=50, max=99, count=50`|count 表示时间轴行数，共 100 行|
|s1 value TimeseriesMetadata，实际类型 INT64|`[0x1160,0x11C0)`|`offsetOfChunkHeader=0x2400, min=0, max=49, count=45`|`offsetOfChunkHeader=0x3400, min=50, max=99, count=50`|共 95 个非空 value；第一个 aligned chunk 有 5 个 null|
|s2 value TimeseriesMetadata，实际类型 DOUBLE|`[0x11C0,0x1220)`|`offsetOfChunkHeader=0x2800, min=0, max=49, count=50`|`offsetOfChunkHeader=0x3800, min=50, max=99, count=40`|共 90 个非空 value；第二个 aligned chunk 有 10 个 null|

配对规则是 `timeChunk[i] + valueChunk[i]`。构建器必须验证 aligned value 的 `chunk_count` 与共享 time metadata 中的 chunk 数相等，且每个 ordinal 对应同一 aligned chunk group；验证成功才设置 `ALIGNED_PAIRING_VALIDATED`。当前版本遇到无法验证的旧文件或 schema 演进文件时应拒绝生成 fast\-path locator，而不能按 ordinal 猜测。`timeseries_meta_length` 是单条 TimeseriesMetadata 的精确长度，使用 u32；若单条记录达到 4 GiB，构建器必须报格式不支持。

|步骤|读取对象|动作|结果|
|---|---|---|---|
|1|`SeriesFileSpan[0] → SeriesLocator[0]`|定点得到 s1 value TimeseriesMetadata 的 `[0x1160,0x11C0)`|不遍历 TsFile measurement metadata index|
|2|`DeviceFileSpan[0]`|由 device\_file\_span\_id 得到共享 time TimeseriesMetadata 的 `[0x1100,0x1160)`|同一查询选择 s1、s2 时只读取或缓存一次 time metadata|
|3|time/value TimeseriesMetadata|分别反序列化两个 ChunkMetadata list，并校验数量均为 2|生成 `(0x2000,0x2400)`、`(0x3000,0x3400)` 两个 time/value chunk pair|
|4|AlignedChunkReader|先按 time statistics 做时间裁剪，再分别读取配对的 time/value chunk header 与 page；null bitmap 决定每行是否存在 s1 value|输出 100 行时间轴、95 个非空 s1 value；不会把 point\_count 当作 row\_count|

ColumnSchema 仍只描述逻辑 measurement。s1 的 physical\_type 是 INT64，s2 是 DOUBLE；time chunk 的 VECTOR 只是 TsFile on\-wire 标记。encoding/compression 继续从 time/value ChunkHeader 分别读取。schema nullable 表示逻辑约束，某个 physical fragment 是否实际含 null 则由 time row\_count、value point\_count 和 value bitmap 共同确定。

**空间开销。** 相比旧布局，DeviceFileSpan 从 32 B 增加到 48 B，SeriesLocator 从 48 B 减少到 24 B；在不改变 SeriesFileSpan 的前提下，净变化为 `+16 × device-file 数量 − 24 × series-file 数量`。一个 device/file 包含多条 aligned measurement 时，共享 time metadata 只增加一次，因此 measurement 越多，节省越明显。

## 查询 shared\_table / d0 / s1，时间范围 \[80,120\]

|步骤|读取对象|计算|本例结果|
|---|---|---|---|
|0|Header \+ SectionDirectory<br>|校验 magic/version/size/file\_length/CRC、每个 section 的边界、对齐、record\_size 和乘法溢出；按当前 section_count 建立 SectionView|Runtime 只常驻 mmap\_base 与当前目录对应的小型 view，不复制整份索引|
|1|TableNameIndex<br>|计算 hash\("shared\_table"\)，在 TableNameIndex 中二分得到同名等值区间；比较 StringBytes 消除 hash 碰撞，再按可选 schema\_fingerprint 选择 variant|`table_id = 0`<br>|
|2|TableRecord\[0\] \+ DeviceNameIndex|在 `[first_device_name_index, +device_count)` 中二分 `d0`|`device_id = 0`|
|3|TableRecord\[0\] \+ ColumnNameIndex|在该表的列索引区间中二分 `s1`，读取 ColumnSchema<br>|`column_id = 0`，`column_ordinal = 0`，type=INT64|
|4|DeviceRecord\[0\] \+ LogicalSeries|在设备的 `[first_series_id, +series_count)` 连续区间内按 column\_id 二分；不为不存在的稀疏列生成 LogicalSeries|`series_id = 0`|
|5|LogicalSeries\[0\] \+ SeriesFileSpan|先用 series 全局 min/max 快速拒绝；在其连续 span 区间中二分首个 `prefix_max_time ≥ 80`，随后扫描到 `min_time > 120`|命中 span 0：A \[0,99\]；span 1：B \[100,199\]<br>|
|6|SeriesLocator\[0\.\.1\] \+ DeviceFileSpan \+ TsFileRecord|由 locator\_id 取得精确 TimeseriesMetadata offset/length、locator\_kind 与 device\_file\_span\_id；由 DeviceFileSpan 取得 file\_id、layout 和可选共享 time metadata，再由 TsFileRecord 取得路径、file\_size 与 fingerprint|生成 A/B 两个 value metadata range；本例 locator\_kind=non\-aligned，因此不读取 time metadata range。Reader 打开文件时自行校验 TsFile format version|
|7|ReaderSessionPool \+ TsFileReader|复用受 FD 预算约束的 ReaderSession，先反序列化目标 TimeseriesMetadata 中的 ChunkMetadata，再按 offsetOfChunkHeader 读取相关 chunk/page，分别裁剪为 \[80,99\] 和 \[100,120\]|只触碰 A/B 中 d0/s1 对应的数据页|
|8|Merge operator<br>|按 timestamp 归并；当前格式不定义 file\_order，跨 TsFile 出现相同 timestamp 时返回重复时间戳错误，后续如需覆盖语义再以新策略字段扩展|输出有序、去重的 s1 列，再构造 DataFrame<br>|

```text
dir = validate_header_and_directory(mmap_base)
table_ids = lookup_table_variants(dir.TABLE_NAME_INDEX, "shared_table")
table_id = select_schema_variant(table_ids, requested_schema_fingerprint)
table = record(dir.TABLE_RECORD, table_id)

device_id = lookup_child_name(dir.DEVICE_NAME_INDEX,
                              table.first_device_name_index,
                              table.device_count, "d0")
column_id = lookup_child_name(dir.COLUMN_NAME_INDEX,
                              table.first_column_name_index,
                              table.column_count, "s1")
device = record(dir.DEVICE_RECORD, device_id)
series_id = lookup_series_by_column(dir.LOGICAL_SERIES,
                                    device.first_series_id,
                                    device.series_count, column_id)
series = record(dir.LOGICAL_SERIES, series_id)

spans = slice(dir.SERIES_FILE_SPAN,
              series.first_file_span, series.file_span_count)
i = lower_bound(spans.prefix_max_time, query_start)
while i < len(spans) and spans[i].min_time <= query_end:
    if spans[i].max_time >= query_start:
        locator = record(dir.SERIES_LOCATOR, spans[i].locator_id)
        device_span = record(dir.DEVICE_FILE_SPAN,
                             locator.device_file_span_id)
        require spans[i].file_id == device_span.file_id
        reader = reader_pool.acquire(device_span.file_id)
        value_meta = range(locator.timeseries_meta_offset,
                           locator.timeseries_meta_length)

        if locator.locator_kind == NON_ALIGNED:
            yield reader.read_non_aligned(value_meta,
                                          query_start, query_end)
        else:
            require device_span.layout == ALIGNED
            require device_span.flags & ALIGNED_PAIRING_VALIDATED
            time_meta = range(device_span.time_meta_offset,
                              device_span.time_meta_length)
            require locator.flags & CHUNK_COUNT_VALID
            require chunk_count(time_meta) == locator.chunk_count
            yield reader.read_aligned(time_meta, value_meta,
                                      query_start, query_end)
    i += 1
```

**内存与 mmap。** Runtime 可以 mmap 整个索引的虚拟地址空间，但启动阶段仅主动读取 64 B Header 和 448 B Directory。SectionView 只保存 `base pointer + count + record_size`；名称索引、逻辑记录和 span 均在查询时按需触页。名称 lookup 的结果可以做有界缓存，但不要求把字符串、schema 或所有 record 反序列化成堆对象。
