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
	path := "tree-example.tsfile"
	_ = os.Remove(path)
	writer, err := tsfile.NewWriter(path)
	if err != nil {
		log.Fatal(err)
	}
	defer writer.Close()
	if err := writer.RegisterTimeseries("root.d1", tsfile.TimeseriesSchema{
		Name: "temperature", DataType: tsfile.DataTypeDouble,
		Encoding: tsfile.EncodingPlain, Compression: tsfile.CompressionUncompressed,
	}); err != nil {
		log.Fatal(err)
	}
	tablet, err := tsfile.NewTablet("root.d1", []tsfile.TabletColumn{
		{Name: "temperature", DataType: tsfile.DataTypeDouble},
	}, 2)
	if err != nil {
		log.Fatal(err)
	}
	defer tablet.Close()
	for i, value := range []float64{20.5, 21.25} {
		if err := tablet.AddTimestamp(i, int64(i)); err != nil {
			log.Fatal(err)
		}
		if err := tablet.SetFloat64(i, 0, value); err != nil {
			log.Fatal(err)
		}
	}
	if err := writer.WriteTreeTablet(tablet); err != nil {
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
	result, err := reader.QueryTree(tsfile.TreeQuery{
		Paths: []string{"root.d1.temperature"}, Start: 0, End: 10,
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
		timestamp, err := result.Int64(0)
		if err != nil {
			log.Fatal(err)
		}
		value, err := result.Float64(1)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Printf("%d %.2f\n", timestamp, value)
	}
}
