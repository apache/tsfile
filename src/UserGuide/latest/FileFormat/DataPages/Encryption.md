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

Encryption is applied after Page compression and reversed before decompression. Headers,
Statistics, schemas, and indexes remain visible. TsFile v4 supports two encryption mechanisms,
both under version byte `0x04`:

| Mode | How the reader recognizes it | Chunk and Page layout |
| --- | --- | --- |
| Plaintext | No file encryption header; legacy level `0` or absent | Base Chunk Header; unencrypted Page body |
| Legacy encryption | No file encryption header; legacy level `1` or `2` | Base Chunk Header; algorithm-specific encrypted body |
| Page AEAD | `0x06` file encryption header at offset `7` | Extended Chunk Header; authenticated Page body defined by a profile |

Check for the file encryption header before parsing Chunks. In an AEAD file, the legacy footer
properties still say unencrypted; the header takes precedence.

The write and read paths are:

```text
encoded page payload -> compression -> encryption -> stored_data
stored_data -> authenticated decryption when applicable -> decompression -> value decoding
```

## File Encryption Header

An AEAD file puts one `FileEncryptionHeader` immediately after the seven-byte magic/version
prefix, before any Chunk Group or operation-index record:

```text
FileEncryptionHeader :=
    byte     marker = 0x06
    UVarInt  payload_size
    byte     header_version = 0x01
    VarString provider_id
    VarString profile_id
    VarString key_id
    VarString key_version
    UVarInt  file_crypto_id_size = 16
    byte     file_crypto_id[16]
    UVarInt  wrapped_data_key_size
    byte     wrapped_data_key[wrapped_data_key_size]
```

`payload_size` counts from `header_version` through the wrapped key, excluding the marker and the
length field itself. Its allowed range is `1..1048576`, and parsing must end exactly at that
boundary. The four string identifiers must be non-null and non-blank. The file crypto ID is
exactly 16 bytes, and the wrapped data key is non-empty. These binary lengths use `UVarInt`, not
the nullable `VarBinary` used for footer properties.

The **provider** identifies the encryption implementation or service. Its **profile** names a
fixed byte-format contract: Page framing, nonce and tag representation, size mapping, and Chunk
proof size. A provider can offer several profiles. The file stores these public identifiers and
the wrapped data key, rather than a Java implementation class or plaintext key. The reader needs
the matching provider/profile and access to the external key material.

Header version `0x01` is the supported layout. Unknown versions, truncated fields, trailing bytes,
a second encryption header, or a header anywhere except offset `7` are errors.

## AEAD Chunk Header

The file encryption header also changes how every Chunk Header is parsed. The Chunk marker
stays the same, but three fields follow `encoding_type`:

```text
    int64  chunk_ordinal
    int32  chunk_authentication_size
    byte   chunk_authentication[chunk_authentication_size]
```

`chunk_ordinal` is a non-negative identifier allocated uniquely across the file, including Time
and Value Chunks. The proof length must match the profile and lie in `1..1024`. These fields are
part of the Header, so none of them count towards `data_size`.

The proof binds the ordinal to the Chunk's absolute position. Verify it before using that
identity. Moving a Chunk changes the authenticated position and therefore requires a new proof.
Its associated data is the following big-endian sequence:

```text
int32 domain_size = 20
byte  domain[20] = UTF8("TsFile/ChunkPosition")
int32 associated_data_version = 1
byte  file_crypto_id[16]
int64 absolute_chunk_marker_offset
int64 chunk_ordinal
```

## AEAD Page Body and Context

For a non-empty Page, `compressed_size` records the entire encrypted body, including framing,
nonces, tags, and padding. It is larger than the compressed plaintext length, even with
`UNCOMPRESSED`. A profile may use fixed overhead or a size-dependent layout, so use its size
mapping to recover the candidate plaintext length.

Page authentication also binds the body to its file, Chunk, and Page position. The associated
data is the exact big-endian sequence below. `SizedBytes` means an `int32` byte length followed
by those bytes; text is UTF-8.

```text
int32      associated_data_version = 1
SizedBytes provider_id
SizedBytes profile_id
SizedBytes key_id
SizedBytes key_version
SizedBytes file_crypto_id
int64      chunk_ordinal
int32      page_index
int32      uncompressed_size
int32      compressed_plaintext_size
```

`page_index` starts at zero for each physical Chunk and counts empty aligned Value Pages as well.
An all-null Value Page is still the single `UVarInt(0)` described in [Nulls](../Nulls.md); it has
no ciphertext or Page authentication tag.

The candidate plaintext length becomes trusted only after authentication succeeds. Decryption
must return that length, and decompression must then return `uncompressed_size` bytes. An invalid
tag, wrong key, unsupported profile, or length mismatch is an error; do not retry as plaintext.

## Legacy File Properties

When there is no file encryption header, read the following `TsFileMetadata` properties:

| Property | Meaning |
| --- | --- |
| `encryptLevel` | `0` for no encryption; `1` for a directly stored data key; `2` for a data key encrypted by an external master key |
| `encryptType` | Legacy encryption/decryption algorithm identifier |
| `encryptKey` | Text representation of the data key or key-wrapping result |

These property values contain text. At level `1`, the data key is stored directly. At level `2`,
an external master-key decryptor unwraps it before Page decryption. Missing `encryptLevel` means
`0`; levels `1` and `2` require `encryptType` and a non-empty `encryptKey`. Unknown levels and
unavailable algorithms are errors.

Legacy encryption uses the algorithm's encrypt/decrypt operations without the AEAD Chunk suffix
or Page context. `compressed_size` is still the actual stored output length; it equals compressed
plaintext length only for a length-preserving algorithm.

AEAD writers set these legacy properties to level `0`, type
`org.apache.tsfile.encrypt.UNENCRYPTED`, and an empty key. The AEAD header supplies the real
mode and wrapped key, so those values must not be interpreted as a plaintext fallback.

## Security Scope

Page AEAD authenticates non-empty Page bodies and their context, while Chunk proofs bind ordinals
to file positions. It does not authenticate every visible field or provide a whole-file digest.
Legacy encryption does not necessarily provide authentication. See [Checksumming](Checksumming.md)
for whole-file integrity and [Error Recovery](Error-Recovery.md) for retaining encryption context
when rebuilding a footer.
