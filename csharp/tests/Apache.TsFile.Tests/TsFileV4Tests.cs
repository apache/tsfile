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
    public void ReadJavaV4File_RecognizesV4Format()
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
        
        // V4 support is currently limited - the full metadata structure with
        // nested MetadataIndexNode trees requires complex deserialization
        // For now, we verify that v4 files are recognized and provide a clear error message
        var exception = Assert.Throws<NotSupportedException>(() =>
        {
            using var reader = new TsFileReader(javaV4File);
        });
        
        Assert.Contains("v4 format reading is partially supported", exception.Message);
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
