// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package main

import (
	"fmt"
	"log"
	"os"

	"github.com/apache/tsfile/go/tsfile"
)

func main() {
	path := "table-example.tsfile"
	_ = os.Remove(path)
	writer, err := tsfile.NewWriter(path)
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()
	if err := writer.RegisterTable(tsfile.TableSchema{Table: "metrics", Columns: []tsfile.ColumnSchema{
		{Name: "device", DataType: tsfile.DataTypeString, Category: tsfile.ColumnCategoryTag},
		{Name: "value", DataType: tsfile.DataTypeInt64, Category: tsfile.ColumnCategoryField},
	}}); err != nil {
		log.Fatal(err)
	}
	tablet, err := tsfile.NewTablet("metrics", []tsfile.TabletColumn{
		{Name: "device", DataType: tsfile.DataTypeString},
		{Name: "value", DataType: tsfile.DataTypeInt64},
	}, 1)
	if err != nil {
		log.Fatal(err)
	}
	defer tablet.Close()
	if err := tablet.AddTimestamp(0, 1); err != nil {
		log.Fatal(err)
	}
	if err := tablet.SetString(0, 0, "d1"); err != nil {
		log.Fatal(err)
	}
	if err := tablet.SetInt64(0, 1, 42); err != nil {
		log.Fatal(err)
	}
	if err := writer.WriteTableTablet(tablet); err != nil {
		log.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		log.Fatal(err)
	}
	reader, err := tsfile.NewReader(path)
	if err != nil {
		log.Fatal(err)
	}
	defer reader.Close()
	result, err := reader.QueryTable(tsfile.TableQuery{
		Table: "metrics", Columns: []string{"device", "value"}, Start: 0, End: 10,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer result.Close()
	for {
		ok, err := result.Next()
		if err != nil {
			log.Fatal(err)
		}
		if !ok {
			break
		}
		device, err := result.String(1)
		if err != nil {
			log.Fatal(err)
		}
		value, err := result.Int64(2)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%s %d\n", device, value)
	}
}
