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

# Security

## Reporting a Vulnerability

Please report suspected, undisclosed vulnerabilities privately to the Apache Software Foundation
Security Team at [security@apache.org](mailto:security@apache.org). The Security Team will triage
the report and coordinate with the Apache TsFile PMC.

Do **not** open a public GitHub issue, discussion, or pull request for a suspected vulnerability.
Public disclosure before a fix is available may put users at risk. Please follow the
[ASF vulnerability handling process](https://www.apache.org/security/) and coordinate disclosure
with the Security Team until a fix and advisory have been published.

Send one plain-text, unencrypted email for each vulnerability. Include as much of the following as
possible:

* the affected TsFile release(s), commit, component, and language implementation;
* a description of the vulnerability, its impact, and the required attack conditions;
* the relevant configuration, operating system, architecture, and runtime versions;
* steps to reproduce the issue and a minimal proof of concept or sample file, if available; and
* relevant logs, stack traces, crash dumps, or sanitizer output.

The security address is only for undisclosed vulnerabilities. Use the public
[Apache TsFile issue tracker](https://github.com/apache/tsfile/issues) for ordinary bugs, build
problems, feature requests, and questions about already published vulnerabilities.

## Security Model

Apache TsFile is a file format and a set of in-process libraries and command-line tools. It is not
a network service or a security boundary. TsFile does not provide authentication, authorization,
tenant isolation, or sandboxing. An application embedding TsFile is responsible for those controls
and for restricting the library's filesystem and network access.

This model applies to the TsFile format readers and writers, the Java and C++ implementations, the
C API, the Python and Go bindings, and command-line tools distributed by this repository.

### Trust Boundaries

TsFile data and metadata may come from untrusted sources. Readers must treat serialized lengths,
offsets, counts, schemas, statistics, encoding and compression identifiers, and compressed payloads
as attacker-controlled. The same principle applies to other serialized input accepted by a TsFile
tool, such as CSV or TSV data imported by a command-line tool.

In contrast, callers are responsible for satisfying documented API preconditions. In-memory
objects, pointers, buffer sizes, callbacks, runtime configuration, the process class path or dynamic
library search path, and explicitly installed extensions are trusted inputs. Passing invalid native
pointers or violating an API's ownership and lifetime requirements is outside this security model.

### Reading Untrusted Files

A malformed or unsupported file may be rejected at any point. Parsing an untrusted file should not
result in arbitrary code execution, memory corruption, access to memory outside the supplied
buffers, disclosure of unrelated process data, or reads or writes outside resources selected by the
caller.

TsFile is optimized for large time-series datasets and supports compression. A valid or maliciously
crafted file may require substantial CPU time, memory, or output space. Applications that accept
files from untrusted sources should impose limits appropriate to their environment, including input
size, decompressed size, memory, processing time, concurrency, and result size. Applications with a
strong isolation requirement should parse untrusted files in a suitably restricted process.

Resource consumption proportional to the input or its declared decompressed size, or a clean error
while parsing malformed input, is normally a robustness issue rather than a vulnerability. Reports
that demonstrate disproportionate resource amplification or a meaningful availability impact
across an actual untrusted boundary may be security issues and will be evaluated case by case.

### Configuration and Optional Implementations

Some TsFile implementations can use configurable filesystem, compression, or encryption code from
the application's runtime environment. Applications must secure their software supply chain,
configuration, class path, and dynamic library search path. Installing or selecting such code grants
it the privileges of the TsFile process; these extension mechanisms are not sandboxes.

### Confidentiality and Integrity

Do not assume that a TsFile is confidential or authentic merely because it can be read successfully.
Unless a supported protection mechanism is explicitly and correctly configured, files should be
treated as plaintext and unauthenticated. Checksums used to detect accidental corruption are not a
substitute for cryptographic authenticity.

Storage permissions, key management, transport security, provenance checks, and access control are
the responsibility of the embedding application and its deployment environment.

### Security-Relevant Findings

Examples of findings that should be reported privately include:

* arbitrary code execution or memory corruption while processing attacker-controlled input;
* out-of-bounds access that exposes unrelated process data;
* reads or writes outside caller-designated resources caused by serialized file contents;
* bypass of a documented security guarantee; and
* remotely triggerable denial of service with significant, disproportionate impact.

Issues that require control of the host, process, trusted runtime configuration, class path, dynamic
library search path, or valid native pointers generally do not cross a boundary provided by TsFile.
