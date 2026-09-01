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
#include <fcntl.h>
#include <gtest/gtest.h>
#include <sys/stat.h>

#include <cstring>
#include <string>

#include "common/tsfile_common.h"
#include "file/read_file.h"
#include "file/restorable_tsfile_io_writer.h"
#include "file/utf8_file_open.h"
#include "file/write_file.h"

#ifdef _WIN32
#include <io.h>
#include <windows.h>
#else
#include <unistd.h>
#endif

// Note: storage::WriteFile / storage::ReadFile are named after Win32 API
// functions that <windows.h> declares at global scope, so the class names are
// always written qualified here.
using namespace common;

namespace {

// The path bytes are spelled with explicit escapes so the test does not depend
// on how the compiler re-encodes non-ASCII literals from the source charset.
// UTF-8 for U+6D4B U+8BD5 ("test" in Chinese) followed by U+00E9 ("e-acute").
const char* kUtf8Stem = "\xE6\xB5\x8B\xE8\xAF\x95_\xC3\xA9_";

// Every gtest case runs as its own process (gtest_discover_tests) in a
// shared working directory, and ctest starts several of them at once
// (--parallel N).  A fixed file name would let one case's SetUp/TearDown
// unlink the file another case is mid-test with, so each case gets its
// own name, suffixed with the gtest case name (which is plain ASCII).
std::string Utf8Name() {
    const ::testing::TestInfo* info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    return std::string(kUtf8Stem) +
           (info != nullptr ? info->name() : "unknown") + ".tsfile";
}

#ifdef _WIN32
// Same name as Utf8Name(), spelled as UTF-16 code units.  Test names are
// ASCII, so widening them is a plain char-by-char copy.
std::wstring WideName() {
    const ::testing::TestInfo* info =
        ::testing::UnitTest::GetInstance()->current_test_info();
    std::wstring wide(L"\x6D4B\x8BD5_\x00E9_");
    const char* tag = info != nullptr ? info->name() : "unknown";
    while (*tag != '\0') {
        wide += static_cast<wchar_t>(*tag++);
    }
    wide += L".tsfile";
    return wide;
}
#endif

// Smallest byte sequence that counts as a complete TsFile: head magic, the
// current version byte, then the tail magic. This is what ReadFile::open()
// requires (>= MIN_FILE_SIZE bytes, magic at both ends) and also what
// RestorableTsFileIOWriter's self check treats as complete.
//
// Built on first use rather than at file scope: VERSION_NUM_BYTE is defined in
// another translation unit, so a file-scope initializer would depend on static
// initialization order.
const std::string& MinimalTsFile() {
    static const std::string bytes = std::string(storage::MAGIC_STRING_TSFILE) +
                                     storage::VERSION_NUM_BYTE +
                                     storage::MAGIC_STRING_TSFILE;
    return bytes;
}

// Remove both spellings: the intended wide name, and the mojibake name the
// narrow CRT produces from the UTF-8 bytes under a non-UTF-8 code page. Leaving
// the latter behind would let one test's stray file satisfy the next test.
void RemoveTestFiles() {
#ifdef _WIN32
    ::_wunlink(WideName().c_str());
    ::_unlink(Utf8Name().c_str());
#else
    ::_unlink(Utf8Name().c_str());
#endif
}

// Existence check that never routes the name through the narrow CRT, so it
// cannot be fooled by the same encoding bug the test is about.
bool ExistsByWidePath() {
#ifdef _WIN32
    return ::GetFileAttributesW(WideName().c_str()) != INVALID_FILE_ATTRIBUTES;
#else
    struct stat st;
    return ::stat(Utf8Name().c_str(), &st) == 0;
#endif
}

// Create the fixture through the wide API on Windows, so the file on disk
// really carries the non-ASCII name regardless of the active code page.
bool CreateFixtureByWidePath() {
#ifdef _WIN32
    const int fd = ::_wopen(WideName().c_str(),
                            _O_WRONLY | _O_CREAT | _O_TRUNC | _O_BINARY,
                            _S_IREAD | _S_IWRITE);
#else
    const int fd =
        ::open(Utf8Name().c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
#endif
    if (fd < 0) {
        return false;
    }
    const std::string& content = MinimalTsFile();
    const int written = static_cast<int>(
        ::write(fd, content.data(), static_cast<unsigned>(content.size())));
#ifdef _WIN32
    ::_close(fd);
#else
    ::close(fd);
#endif
    return written == static_cast<int>(content.size());
}

class Utf8PathTest : public ::testing::Test {
   protected:
    void SetUp() override { RemoveTestFiles(); }
    void TearDown() override { RemoveTestFiles(); }
};

}  // namespace

// WriteFile must create the file under the UTF-8 name it was given. On Windows
// the narrow ::open() interprets the bytes in the active code page, so the file
// lands under a mojibake name (or not at all) and the wide-path lookup fails.
TEST_F(Utf8PathTest, WriteFileCreatesUtf8Path) {
    storage::WriteFile write_file;
    ASSERT_EQ(write_file.create(Utf8Name(), O_WRONLY | O_CREAT | O_TRUNC, 0666),
              E_OK);
    ASSERT_TRUE(write_file.file_opened());
    const std::string& content = MinimalTsFile();
    ASSERT_EQ(
        write_file.write(content.data(), static_cast<uint32_t>(content.size())),
        E_OK);
    ASSERT_EQ(write_file.close(), E_OK);

    EXPECT_TRUE(ExistsByWidePath())
        << "the file was not created under the UTF-8 path that was requested";
}

// ReadFile must find a file that exists on disk under a non-ASCII name.
TEST_F(Utf8PathTest, ReadFileOpensUtf8Path) {
    ASSERT_TRUE(CreateFixtureByWidePath());
    ASSERT_TRUE(ExistsByWidePath());

    storage::ReadFile read_file;
    EXPECT_EQ(read_file.open(Utf8Name()), E_OK)
        << "an existing file with a non-ASCII name could not be opened";
    EXPECT_TRUE(read_file.is_opened());
    read_file.close();
}

// Round trip through both classes: what WriteFile wrote, ReadFile must read.
// The wide-path check matters even though the round trip alone would succeed
// while both sides are equally broken -- a consistently mangled name still
// round trips, so only the on-disk name proves the bytes were honoured.
TEST_F(Utf8PathTest, Utf8PathRoundTripsBetweenWriteAndRead) {
    storage::WriteFile write_file;
    ASSERT_EQ(write_file.create(Utf8Name(), O_WRONLY | O_CREAT | O_TRUNC, 0666),
              E_OK);
    const std::string& content = MinimalTsFile();
    ASSERT_EQ(
        write_file.write(content.data(), static_cast<uint32_t>(content.size())),
        E_OK);
    ASSERT_EQ(write_file.close(), E_OK);

    ASSERT_TRUE(ExistsByWidePath())
        << "the round trip used a name that is not the requested UTF-8 path";

    storage::ReadFile read_file;
    ASSERT_EQ(read_file.open(Utf8Name()), E_OK);
    EXPECT_EQ(read_file.file_size(), static_cast<int64_t>(content.size()));

    std::string buf(content.size(), '\0');
    int32_t read_len = 0;
    ASSERT_EQ(
        read_file.read(0, &buf[0], static_cast<int32_t>(buf.size()), read_len),
        E_OK);
    EXPECT_EQ(read_len, static_cast<int32_t>(content.size()));
    EXPECT_EQ(buf, content);
    read_file.close();
}

// The third caller of open_utf8() is the self-check reader inside
// RestorableTsFileIOWriter. Given a complete file under a non-ASCII name, the
// self check must recognise it as complete. When the name is mangled instead,
// the writer creates an empty file under the wrong name and reports a crashed
// file that may be written from scratch -- which would silently discard data.
TEST_F(Utf8PathTest, RestorableWriterSelfChecksUtf8Path) {
    ASSERT_TRUE(CreateFixtureByWidePath());

    storage::RestorableTsFileIOWriter writer;
    ASSERT_EQ(writer.open(Utf8Name(), true), E_OK);
    EXPECT_EQ(writer.get_truncated_size(), storage::TSFILE_CHECK_COMPLETE)
        << "the self check did not see the existing complete file";
    EXPECT_FALSE(writer.has_crashed());
    EXPECT_FALSE(writer.can_write());
    writer.close();
}

#ifdef _WIN32
// Rejection paths of the conversion helper. These are Windows-only: the POSIX
// branch hands the bytes to ::open() unchanged, where a path holding a NUL
// would silently be truncated at it rather than rejected.
TEST_F(Utf8PathTest, OpenUtf8RejectsInvalidPaths) {
    errno = 0;
    EXPECT_EQ(storage::file_internal::open_utf8("", O_RDONLY), -1);
    EXPECT_EQ(errno, ENOENT);

    std::string embedded_nul("a\0b.tsfile", 10);
    errno = 0;
    EXPECT_EQ(storage::file_internal::open_utf8(embedded_nul, O_RDONLY), -1);
    EXPECT_EQ(errno, EINVAL);

    // 0xFF 0xFE is not a valid UTF-8 sequence, so the conversion must fail
    // rather than fall back to a lossy interpretation.
    errno = 0;
    EXPECT_EQ(
        storage::file_internal::open_utf8("\xFF\xFE_bad.tsfile", O_RDONLY), -1);
    EXPECT_EQ(errno, EINVAL);
}
#endif
