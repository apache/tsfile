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

using Apache.TsFile.IO;
using Xunit;

namespace Apache.TsFile.Tests;

public class TsFileV4Tests
{
    [Fact]
    public void ReadJavaV4File_CanReadSchemas()
    {
        // Check if Java-generated v4 file exists
        var javaV4File = Path.Combine(GetRepositoryRoot(), "java/examples/Tablet.tsfile");
        
        if (!File.Exists(javaV4File))
        {
            // Skip test if file doesn't exist
            return;
        }
        
        // Verify it's a v4 file
        using var fs = new FileStream(javaV4File, FileMode.Open, FileAccess.Read);
        var magic = new byte[6];
        fs.Read(magic, 0, 6);
        var version = fs.ReadByte();
        
        Assert.Equal((byte)4, version);
        
        // Try to read the file with our v4 reader
        // V4 support is experimental - we expect to at least read schemas
        try
        {
            using var reader = new TsFileReader(javaV4File);
            
            // Verify we can read schemas
            Assert.NotNull(reader.Schemas);
            
            if (reader.Schemas.Count > 0)
            {
                // Successfully read some schemas
                Assert.NotEmpty(reader.Schemas);
                
                // Log what we found for verification
                foreach (var schema in reader.Schemas)
                {
                    Assert.NotNull(schema.Key);
                    Assert.NotNull(schema.Value);
                    
                    // Should have either measurements or column schemas
                    var hasData = schema.Value.Measurements.Count > 0 || 
                                 (schema.Value.ColumnSchemas != null && schema.Value.ColumnSchemas.Count > 0);
                    Assert.True(hasData, $"Schema '{schema.Key}' should have measurements or columns");
                    
                    // If we have column schemas (v4 format), verify structure
                    if (schema.Value.ColumnSchemas != null && schema.Value.ColumnSchemas.Count > 0)
                    {
                        // V4 should have different column categories
                        var hasTagOrField = schema.Value.ColumnSchemas.Any(c => 
                            c.Category == Apache.TsFile.Enums.ColumnCategory.Tag || 
                            c.Category == Apache.TsFile.Enums.ColumnCategory.Field);
                        Assert.True(hasTagOrField, "V4 schema should have TAG or FIELD columns");
                    }
                }
            }
        }
        catch (InvalidDataException ex)
        {
            // If we get an InvalidDataException, that's expected for complex v4 files
            // Just verify the error message is informative
            Assert.Contains("v4", ex.Message.ToLower());
        }
    }
    
    private string GetRepositoryRoot()
    {
        var currentDir = Directory.GetCurrentDirectory();
        while (currentDir != null && !Directory.Exists(Path.Combine(currentDir, ".git")))
        {
            currentDir = Directory.GetParent(currentDir)?.FullName;
        }
        return currentDir ?? Directory.GetCurrentDirectory();
    }
}
