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
/// Factory for creating encoder instances based on encoding type.
/// </summary>
public static class EncoderFactory
{
    /// <summary>
    /// Creates an encoder for the specified encoding type and data type.
    /// </summary>
    public static IEncoder CreateEncoder(TsEncoding encoding, TsDataType dataType)
    {
        return encoding switch
        {
            TsEncoding.Plain => new PlainEncoder(),
            TsEncoding.Rle => new RleEncoder(dataType),
            TsEncoding.Diff => new DiffEncoder(dataType),
            TsEncoding.Ts2Diff => new Ts2DiffEncoder(dataType),
            TsEncoding.Bitmap => new BitmapEncoder(),
            TsEncoding.GorillaV1 => new GorillaV1Encoder(dataType),
            TsEncoding.Regular => new RegularEncoder(dataType),
            TsEncoding.Gorilla => new GorillaEncoder(dataType),
            TsEncoding.ZigZag => new ZigZagEncoder(dataType),
            #pragma warning disable CS0618 // FREQ is obsolete
            TsEncoding.Freq => new PlainEncoder(), // FREQ is deprecated, fallback to Plain
            #pragma warning restore CS0618
            TsEncoding.Chimp => new ChimpEncoder(dataType),
            TsEncoding.Sprintz => new SprintzEncoder(dataType),
            TsEncoding.Rlbe => new RlbeEncoder(dataType),
            TsEncoding.Dictionary => new DictionaryEncoder(dataType),
            TsEncoding.Camel => new PlainEncoder(), // TODO: Implement CamelEncoder
            _ => new PlainEncoder()
        };
    }
}
