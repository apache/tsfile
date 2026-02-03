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
            TsEncoding.Diff => new DiffDecoder(dataType),
            TsEncoding.Ts2Diff => new Ts2DiffDecoder(dataType),
            TsEncoding.Bitmap => new BitmapDecoder(),
            TsEncoding.GorillaV1 => new GorillaV1Decoder(dataType),
            TsEncoding.Regular => new RegularDecoder(dataType),
            TsEncoding.Gorilla => new GorillaDecoder(dataType),
            TsEncoding.ZigZag => new ZigZagDecoder(dataType),
            #pragma warning disable CS0618 // FREQ is obsolete
            TsEncoding.Freq => new PlainDecoder(), // FREQ is deprecated, fallback to Plain
            #pragma warning restore CS0618
            TsEncoding.Chimp => new ChimpDecoder(dataType),
            TsEncoding.Sprintz => new SprintzDecoder(dataType),
            TsEncoding.Rlbe => new RlbeDecoder(dataType),
            TsEncoding.Dictionary => new DictionaryDecoder(dataType),
            TsEncoding.Camel => new PlainDecoder(), // TODO: Implement CamelDecoder
            _ => new PlainDecoder()
        };
    }
}
