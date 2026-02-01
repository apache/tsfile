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

using Apache.TsFile;
using Apache.TsFile.Enums;
using Apache.TsFile.IO;
using Apache.TsFile.Schema;

namespace BasicExample;

class Program
{
    static void Main(string[] args)
    {
        Console.WriteLine("Apache TSFile C# Library - Basic Example\n");
        
        var filePath = "example.tsfile";
        
        // Example 1: Write data to TSFile
        Console.WriteLine("Writing data to TSFile...");
        WriteExample(filePath);
        
        // Example 2: Read data from TSFile
        Console.WriteLine("\nReading data from TSFile...");
        ReadExample(filePath);
        
        // Example 3: Write with compression
        Console.WriteLine("\nWriting compressed data...");
        WriteCompressedExample("compressed.tsfile");
        
        Console.WriteLine("\nExamples completed successfully!");
        
        // Cleanup
        File.Delete(filePath);
        File.Delete("compressed.tsfile");
    }
    
    static void WriteExample(string filePath)
    {
        using var writer = new TsFileWriter(filePath);
        
        // Define measurement schemas
        var measurements = new List<MeasurementSchema>
        {
            new MeasurementSchema("temperature", TsDataType.Float, TsEncoding.Plain, CompressionType.Uncompressed),
            new MeasurementSchema("humidity", TsDataType.Int32, TsEncoding.Plain, CompressionType.Uncompressed),
            new MeasurementSchema("status", TsDataType.Boolean, TsEncoding.Plain, CompressionType.Uncompressed)
        };
        
        // Register device
        writer.RegisterDevice("sensor_1", measurements);
        
        // Create a tablet for batch writing
        var tablet = new Tablet("sensor_1", measurements, 100);
        
        // Add data rows
        for (int i = 0; i < 10; i++)
        {
            long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + i * 1000;
            float temperature = 20.0f + i * 0.5f;
            int humidity = 50 + i;
            bool status = i % 2 == 0;
            
            tablet.AddRow(timestamp, temperature, humidity, status);
        }
        
        // Write tablet to file
        writer.Write(tablet);
        
        // Alternative: Write single rows
        writer.WriteRow("sensor_1", 
            DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + 10000, 
            25.5f, 60, true);
        
        writer.Close();
        
        Console.WriteLine($"Written 11 rows to {filePath}");
    }
    
    static void ReadExample(string filePath)
    {
        using var reader = new TsFileReader(filePath);
        
        // Print available schemas
        Console.WriteLine($"Found {reader.Schemas.Count} device(s):");
        foreach (var schema in reader.Schemas.Values)
        {
            Console.WriteLine($"  - {schema.TableName} with {schema.MeasurementCount} measurement(s)");
        }
        
        // Query all data for a device
        var result = reader.Query("sensor_1");
        
        Console.WriteLine($"\nRead {result.Timestamps.Count} rows:");
        
        for (int i = 0; i < Math.Min(5, result.Timestamps.Count); i++)
        {
            Console.WriteLine($"  Row {i}: " +
                $"timestamp={result.Timestamps[i]}, " +
                $"temperature={result.MeasurementData["temperature"][i]}, " +
                $"humidity={result.MeasurementData["humidity"][i]}, " +
                $"status={result.MeasurementData["status"][i]}");
        }
        
        if (result.Timestamps.Count > 5)
        {
            Console.WriteLine($"  ... and {result.Timestamps.Count - 5} more rows");
        }
    }
    
    static void WriteCompressedExample(string filePath)
    {
        using var writer = new TsFileWriter(filePath);
        
        // Use compression for better storage efficiency
        var measurements = new List<MeasurementSchema>
        {
            new MeasurementSchema("temperature", TsDataType.Double, TsEncoding.Plain, CompressionType.Gzip),
            new MeasurementSchema("pressure", TsDataType.Double, TsEncoding.Plain, CompressionType.Lz4)
        };
        
        writer.RegisterDevice("weather_station", measurements);
        
        var tablet = new Tablet("weather_station", measurements, 1000);
        
        // Generate sample data
        for (int i = 0; i < 100; i++)
        {
            long timestamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + i * 60000;
            double temperature = 15.0 + Math.Sin(i * 0.1) * 10.0;
            double pressure = 1013.25 + Math.Cos(i * 0.1) * 20.0;
            
            tablet.AddRow(timestamp, temperature, pressure);
        }
        
        writer.Write(tablet);
        writer.Close();
        
        var fileSize = new FileInfo(filePath).Length;
        Console.WriteLine($"Written 100 rows to {filePath} (size: {fileSize} bytes)");
    }
}
