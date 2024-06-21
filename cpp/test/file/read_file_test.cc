#include "file/read_file.h"

#include <gtest/gtest.h>

#include <fstream>
#include <string>

using namespace storage;
using namespace common;

class ReadFileTest : public ::testing::Test {
   protected:
    const char *MAGIC_STRING_TSFILE = "TsFile";

    void SetUp() override {
        test_file_path_ = "test_file.dat";
        std::ofstream test_file(test_file_path_, std::ios::binary);
        std::string magic = MAGIC_STRING_TSFILE;
        // Write header magic string
        test_file.write(magic.c_str(), magic.size());
        // Write some dummy content
        test_file.write("This is some test content.", 26);
        // Write footer magic string
        test_file.write(magic.c_str(), magic.size());
        test_file.close();
    }

    void TearDown() override { remove(test_file_path_.c_str()); }

    std::string test_file_path_;
};

TEST_F(ReadFileTest, OpenCloseFile) {
    ReadFile read_file;
    EXPECT_EQ(read_file.open(test_file_path_), E_OK);
    EXPECT_TRUE(read_file.is_opened());
    EXPECT_EQ(read_file.file_path(), test_file_path_);
    read_file.close();
    EXPECT_FALSE(read_file.is_opened());
}

TEST_F(ReadFileTest, GetFileSize) {
    ReadFile read_file;
    EXPECT_EQ(read_file.open(test_file_path_), E_OK);
    EXPECT_GT(read_file.file_size(), 0);
    read_file.close();
}

TEST_F(ReadFileTest, ReadFileContent) {
    ReadFile read_file;
    EXPECT_EQ(read_file.open(test_file_path_), E_OK);
    int32_t file_size = read_file.file_size();
    char buffer[64];
    int32_t read_len;
    EXPECT_EQ(read_file.read(0, buffer, 64, read_len), E_OK);
    EXPECT_GT(read_len, 0);
    EXPECT_EQ(read_len, file_size);

    // Check if the file content matches expected content
    std::string content(buffer, read_len);
    std::string expected_content = std::string(MAGIC_STRING_TSFILE) +
                                   "This is some test content." +
                                   MAGIC_STRING_TSFILE;
    EXPECT_EQ(content, expected_content);

    read_file.close();
}

TEST_F(ReadFileTest, FileNotExist) {
    ReadFile read_file;
    EXPECT_EQ(read_file.open("non_existent_file.dat"), E_FILE_OPEN_ERR);
    EXPECT_FALSE(read_file.is_opened());
}

TEST_F(ReadFileTest, ReadNonExistentOffset) {
    ReadFile read_file;
    EXPECT_EQ(read_file.open(test_file_path_), E_OK);
    char buffer[10];
    int32_t read_len;
    EXPECT_EQ(read_file.read(1000, buffer, 10, read_len), E_OK);
    EXPECT_EQ(read_len, 0);
    read_file.close();
}