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

using Apache.TsFile.Encoding.Encoder;
using Apache.TsFile.Encoding.Decoder;
using Apache.TsFile.Enums;

namespace Apache.TsFile.Encoding;

/// <summary>
/// Factory for creating decoder instances based on encoding type.
/// </summary>
public static class DecoderFactory
{
    /// <summary>
    /// Creates a decoder for the specified encoding type and data type.
    /// </summary>
    public static IDecoder CreateDecoder(TsEncoding encoding, TsDataType dataType)
    {
        return encoding switch
        {
            TsEncoding.Plain => new PlainDecoder(),
            TsEncoding.Rle => new RleDecoder(dataType),
            TsEncoding.Ts2Diff => new PlainDecoder(), // TODO: Implement Ts2DiffDecoder
            TsEncoding.Gorilla => new GorillaDecoder(dataType),
            TsEncoding.ZigZag => new ZigZagDecoder(dataType),
            TsEncoding.Dictionary => new DictionaryDecoder(dataType),
            TsEncoding.Chimp => new PlainDecoder(), // TODO: Implement ChimpDecoder
            TsEncoding.Sprintz => new PlainDecoder(), // TODO: Implement SprintzDecoder
            TsEncoding.Rlbe => new PlainDecoder(), // TODO: Implement RlbeDecoder
            TsEncoding.Bitmap => new PlainDecoder(), // TODO: Implement BitmapDecoder
            TsEncoding.Camel => new PlainDecoder(), // TODO: Implement CamelDecoder
            _ => new PlainDecoder()
        };
    }
}
