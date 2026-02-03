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

namespace Apache.TsFile.Encoding.Fire;

public class IntFire
{
    private readonly int _learnShift;
    private const int BitWidth = 8;
    private int _accumulator;
    private int _delta;

    public IntFire(int learningRate)
    {
        _learnShift = learningRate;
        _accumulator = 0;
        _delta = 0;
    }

    public void Reset()
    {
        _accumulator = 0;
        _delta = 0;
    }

    public int Predict(int value)
    {
        int alpha = _accumulator >> _learnShift;
        int diff = (alpha * _delta) >> BitWidth;
        return value + diff;
    }

    public void Train(int pre, int val, int err)
    {
        int gradient = err > 0 ? -_delta : _delta;
        _accumulator -= gradient;
        _delta = val - pre;
    }
}
