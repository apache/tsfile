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

# TsFile Documentation

This directory contains comprehensive documentation for the Apache TsFile project.

## Format Specifications

### [TSFILE_FORMAT_V4.md](./TSFILE_FORMAT_V4.md)
Complete specification of TsFile format version 4, including:
- Table-based data model overview
- File structure and layout
- Metadata organization
- Encoding and compression methods
- API examples for reading and writing
- Performance characteristics

**Audience:** Developers implementing TsFile readers/writers, architects designing systems using TsFile

### [VERSION_COMPATIBILITY.md](./VERSION_COMPATIBILITY.md)
Comprehensive compatibility matrix for TsFile versions across implementations:
- Version support by implementation (Java, C#, Python, C++)
- Read/write capabilities matrix
- Feature comparison across versions
- Encoding and compression support
- Cross-implementation interoperability
- Testing and validation guidance

**Audience:** System architects, DevOps engineers, developers working with multiple TsFile implementations

### [MIGRATION_GUIDE_V3_TO_V4.md](./MIGRATION_GUIDE_V3_TO_V4.md)
Step-by-step guide for migrating from TsFile v3 to v4:
- Migration checklist and timeline
- Code examples showing v3 vs v4 APIs
- Schema design patterns
- Data migration strategies
- Troubleshooting common issues
- Performance optimization tips
- Rollback procedures

**Audience:** Java developers upgrading to v4, teams planning migrations

## Quick Navigation

### By Role

**Application Developers:**
1. Start with [TSFILE_FORMAT_V4.md](./TSFILE_FORMAT_V4.md) - Understand the format
2. Check [VERSION_COMPATIBILITY.md](./VERSION_COMPATIBILITY.md) - Verify your implementation's capabilities
3. Follow code examples in TSFILE_FORMAT_V4.md

**System Architects:**
1. Read [VERSION_COMPATIBILITY.md](./VERSION_COMPATIBILITY.md) - Plan cross-platform compatibility
2. Review [TSFILE_FORMAT_V4.md](./TSFILE_FORMAT_V4.md) - Understand performance characteristics
3. Consult [MIGRATION_GUIDE_V3_TO_V4.md](./MIGRATION_GUIDE_V3_TO_V4.md) - Plan upgrade path

**Java Developers Upgrading:**
1. Start with [MIGRATION_GUIDE_V3_TO_V4.md](./MIGRATION_GUIDE_V3_TO_V4.md) - Follow step-by-step migration
2. Reference [TSFILE_FORMAT_V4.md](./TSFILE_FORMAT_V4.md) - Understand new concepts
3. Test with [VERSION_COMPATIBILITY.md](./VERSION_COMPATIBILITY.md) - Verify compatibility

**C#/Python/C++ Developers:**
1. Check [VERSION_COMPATIBILITY.md](./VERSION_COMPATIBILITY.md) - Current version 3 support
2. Review [TSFILE_FORMAT_V4.md](./TSFILE_FORMAT_V4.md) - Future v4 requirements
3. Monitor implementation progress for v4 support

### By Task

**Understanding TsFile Format:**
→ [TSFILE_FORMAT_V4.md](./TSFILE_FORMAT_V4.md)

**Checking Compatibility:**
→ [VERSION_COMPATIBILITY.md](./VERSION_COMPATIBILITY.md)

**Upgrading from v3 to v4:**
→ [MIGRATION_GUIDE_V3_TO_V4.md](./MIGRATION_GUIDE_V3_TO_V4.md)

**Cross-Language Interoperability:**
→ [VERSION_COMPATIBILITY.md](./VERSION_COMPATIBILITY.md) Section: "Cross-Implementation Interoperability"

**Troubleshooting Issues:**
→ [MIGRATION_GUIDE_V3_TO_V4.md](./MIGRATION_GUIDE_V3_TO_V4.md) Section: "Troubleshooting"

## Related Documentation

### Project Root
- [README.md](../README.md) - Project overview and quick start
- [IMPLEMENTATION_PROGRESS.md](../IMPLEMENTATION_PROGRESS.md) - Current implementation status
- [INTEROP_IMPLEMENTATION_SUMMARY.md](../INTEROP_IMPLEMENTATION_SUMMARY.md) - Interoperability details
- [INTEROP_TEST_RESULTS.md](../INTEROP_TEST_RESULTS.md) - Cross-platform test results

### Java Implementation
- [java/tsfile/README.md](../java/tsfile/README.md) - Java API documentation
- [java/tsfile/format-changelist.md](../java/tsfile/format-changelist.md) - Format version history
- [java/examples/](../java/examples/) - Java code examples

### C# Implementation
- [csharp/STATUS.md](../csharp/STATUS.md) - C# implementation status
- [csharp/README.md](../csharp/README.md) - C# API documentation
- [csharp/BENCHMARKS.md](../csharp/BENCHMARKS.md) - C# performance benchmarks

### Python Implementation
- [python/README.md](../python/README.md) - Python API documentation

### C++ Implementation
- [cpp/README.md](../cpp/README.md) - C++ API documentation

## Documentation Standards

All documentation in this directory follows these standards:

- **Apache License Header:** All files include the Apache 2.0 license header
- **Markdown Format:** All documentation uses Markdown for maximum compatibility
- **Code Examples:** Practical, runnable examples included where applicable
- **Cross-References:** Links to related documentation provided
- **Audience-Specific:** Clear target audience identified for each document
- **Version-Specific:** Documentation clearly states which versions are covered

## Contributing to Documentation

When adding or updating documentation:

1. **Follow the template:** Use existing documents as templates
2. **Add license header:** Include Apache 2.0 license at the top
3. **Cross-reference:** Link to related documents
4. **Test examples:** Verify all code examples compile and run
5. **Update this README:** Add your document to the navigation sections
6. **Version compatibility:** Clearly state which versions are covered
7. **Keep it current:** Update when implementation changes

## Getting Help

### Community Support
- **Mailing List:** dev@iotdb.apache.org
- **GitHub Issues:** https://github.com/apache/tsfile/issues
- **Documentation:** https://iotdb.apache.org/

### Reporting Documentation Issues
Found an error or need clarification? Please:
1. Check if issue already exists in GitHub Issues
2. Create new issue with "documentation" label
3. Include:
   - Document name and section
   - Description of issue
   - Suggested improvement (if applicable)

## Version History

| Date | Version | Changes |
|------|---------|---------|
| 2026-02-04 | 1.0 | Initial documentation set for v4 format |

## License

All documentation is licensed under the Apache License 2.0. See [LICENSE](../LICENSE) for details.
