<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Extensibility

TsFile has three main ways to introduce new capabilities: assign a new type or codec identifier,
add a footer property, or define a new file layout. The choice depends on whether a reader can
still locate the next structure without understanding the new feature.

Footer properties are easy to delimit. Each has a `VarString` key and a nullable `VarBinary`
value, so a reader can preserve an unknown value without decoding it. A Chunk also has a known
byte length, which lets a scanner skip it even if it cannot decode the selected codec.
Data-section markers have no shared length field, so the same approach does not work for an
unknown marker.

## Safe Evolution Rules

A compatible extension keeps existing field meanings and boundaries intact:

- Give a new type or codec an unused identifier; keep the meaning of assigned identifiers.
- Define the supported data types, byte layout, output sizes, and error handling for a new codec.
- Keep the existing byte order and length conventions.
- Report an unsupported feature when it is needed to decode the requested data.

Adding bytes to a Header without a rule that selects the extended layout shifts the fields an
older reader expects. Such a change requires a new version or an explicit extension mechanism.

The Page AEAD extension is an example of the latter. Its `0x06` file encryption header selects
extra Chunk Header fields and profile-specific Page framing while retaining version byte `0x04`.
Older v4 readers may reject those files. [Encryption](DataPages/Encryption.md) defines the layout
and required capabilities; knowing the base format version alone is not enough.

## Properties and Semantics

Being able to skip a property does not mean its meaning can always be ignored. An informational
property may be optional, but a property that changes how Page bytes are decoded needs an explicit
support rule. If a reader cannot apply required encryption or another required interpretation,
it must report an error rather than decode the bytes with a default.
