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

# Encryption

Encryption applies only to the compressed Page `stored_data`. Page Headers, Chunk Headers,
Statistics, Metadata Indexes, and File Metadata are not encrypted.

The write pipeline is:

```text
encoded page payload
  -> compression
  -> encryption
  -> stored_data
```

Reading decrypts before decompression and decoding.

## File Properties

The property map in `TsFileMetadata` uses these keys to describe encryption:

| Property | Meaning |
| --- | --- |
| `encryptLevel` | `0` for no encryption; `1` for a directly stored data key; `2` for a data key encrypted by an external master key |
| `encryptType` | Encryption and decryption algorithm identifier |
| `encryptKey` | Text representation of the data key or key-wrapping result |

A Page Header has no separate ciphertext-length field. The encrypted result has the same length
as the compressed input, so `compressed_size` remains the length of `stored_data` in the file.

When `encryptLevel` is absent, it is interpreted as `0` for compatibility. For levels `1` and `2`,
both `encryptType` and a non-empty `encryptKey` are required. An unknown level or unavailable
algorithm makes the encrypted Page data unreadable; readers must not silently treat it as
unencrypted.

## Key Interpretation

At level `1`, `encryptKey` represents the data key used with `encryptType`. At level `2`, it
represents a wrapped data key: the reader obtains the external master-key decryptor, unwraps the
data key, and then decrypts Page payloads with `encryptType`. Key derivation, master-key storage,
rotation, and access control are outside the file format.

## Security Scope

Page encryption protects the confidentiality of encoded sample data. It does not hide the file
shape, device and measurement identifiers, types, time ranges, value Statistics, schemas, indexes,
or the encryption properties themselves. Applications should treat this metadata as visible.

TsFile v4 does not define a general authentication tag for Pages or for the file. Whether a
ciphertext modification is detected depends on the selected algorithm; the structural validation
and external integrity guidance in [Checksumming](Checksumming.md) still apply.

Readers should resolve the encryption properties and key material before allocating Page buffers,
then apply the exact pipeline `read -> decrypt -> decompress -> decode`. A decryption failure,
wrong output length, unknown algorithm, or missing key is an error for the affected data and must
not trigger plaintext fallback.
