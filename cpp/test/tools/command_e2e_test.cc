/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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

#include <gtest/gtest.h>

#include <cstdio>
#include <fstream>
#include <sstream>
#include <string>

#include "cli/run_cli.h"
#include "cli_test_util.h"

namespace {

struct Fixture {
    std::string path = tsfile_cli_test::write_table_fixture();
    ~Fixture() { std::remove(path.c_str()); }
};

size_t count_lines(const std::string& s) {
    size_t n = 0;
    for (char c : s) {
        if (c == '\n') {
            ++n;
        }
    }
    return n;
}

}  // namespace

TEST(CliE2E, LsListsTableNameTsv) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"ls", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "name\ntable1\n");
    EXPECT_TRUE(err.str().empty());
}

TEST(CliE2E, LsNoHeaderJustName) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"ls", "-f", "tsv", "--no-header", f.path},
                                   out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "table1\n");
}

TEST(CliE2E, OpenMissingFileReturnsFileError) {
    std::ostringstream out;
    std::ostringstream err;
    int code =
        tsfile_cli::run_cli({"ls", "definitely_missing.tsfile"}, out, err);
    EXPECT_EQ(code, 2);
    EXPECT_FALSE(err.str().empty());
}

TEST(CliE2E, SchemaShowsFieldColumnAndType) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"schema", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_NE(
        out.str().find("target\tmeasurement\tdatatype\tencoding\tcompression"),
        std::string::npos);
    EXPECT_NE(out.str().find("s1"), std::string::npos);
    EXPECT_NE(out.str().find("INT64"), std::string::npos);
}

TEST(CliE2E, SchemaTableMeasurementFilterOnlyShowsRequestedColumn) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"schema", "-m", "s1", "-f", "tsv", f.path},
                                   out, err);
    EXPECT_EQ(code, 0);
    EXPECT_NE(out.str().find("table1\ts1\tINT64"), std::string::npos);
    EXPECT_EQ(out.str().find("table1\tid1"), std::string::npos);
    EXPECT_EQ(out.str().find("table1\tid2"), std::string::npos);
}

TEST(CliE2E, StatsReportsCountAndTimeRange) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"stats", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_NE(out.str().find("target\tmeasurement\tcount\tstart_time\tend_"
                             "time\tmin\tmax\tfirst\tlast\tsum"),
              std::string::npos);
    EXPECT_NE(out.str().find("s1\t5\t0\t4\t0\t40\t0\t40\t100"),
              std::string::npos);
}

TEST(CliE2E, HeadProjectsAndLimits) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"head", "-m", "s1", "-n", "2", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "time\ts1\n0\t0\n1\t10\n");
}

TEST(CliE2E, CatReturnsAllRows) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code =
        tsfile_cli::run_cli({"cat", "-m", "s1", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(count_lines(out.str()), 6u);
    EXPECT_NE(out.str().find("time\ts1\n"), std::string::npos);
}

TEST(CliE2E, CatWithTimeRange) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"cat", "-m", "s1", "--start", "2", "--end", "3", "-f", "tsv", f.path},
        out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "time\ts1\n2\t20\n3\t30\n");
}

TEST(CliE2E, CatJsonIsNdjson) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"cat", "-m", "s1", "--start", "0", "--end", "0", "-f", "json", f.path},
        out, err);
    EXPECT_EQ(code, 0);
    EXPECT_EQ(out.str(), "{\"time\":0,\"s1\":0}\n");
}

TEST(CliE2E, MetaReportsFileSummary) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"meta", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_TRUE(err.str().empty());
    EXPECT_NE(out.str().find("file\tmodel\tdevice_count\ttable_count\tseries_"
                             "count\tstart_time\tend_time\tfile_size_bytes"),
              std::string::npos);
    EXPECT_NE(out.str().find("\ttable\t"), std::string::npos);
}

TEST(CliE2E, CountReportsSeriesCountsAndTotal) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"count", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_TRUE(err.str().empty());
    EXPECT_NE(out.str().find("target\tmeasurement\tcount"), std::string::npos);
    EXPECT_NE(out.str().find("\ts1\t5"), std::string::npos);
    EXPECT_NE(out.str().find("total\t\t"), std::string::npos);
}

TEST(CliE2E, SampleIsReproducibleWithSeed) {
    Fixture f;
    std::ostringstream out1;
    std::ostringstream err1;
    std::ostringstream out2;
    std::ostringstream err2;

    int code1 = tsfile_cli::run_cli(
        {"sample", "-m", "s1", "-n", "3", "--seed", "7", "-f", "tsv", f.path},
        out1, err1);
    int code2 = tsfile_cli::run_cli(
        {"sample", "-m", "s1", "-n", "3", "--seed", "7", "-f", "tsv", f.path},
        out2, err2);

    EXPECT_EQ(code1, 0);
    EXPECT_EQ(code2, 0);
    EXPECT_TRUE(err1.str().empty());
    EXPECT_TRUE(err2.str().empty());
    EXPECT_EQ(out1.str(), out2.str());
    EXPECT_EQ(count_lines(out1.str()), 4u);
    EXPECT_NE(out1.str().find("time\ts1\n"), std::string::npos);
}

TEST(CliE2E, WriteThenReadRoundTrip) {
    std::string csv_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_write_in", ".csv");
    {
        std::ofstream o(csv_path.c_str());
        o << "time,id1,s1\n0,dev,0\n1,dev,10\n2,dev,20\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_write_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t1", "--columns", "id1:STRING:tag,s1:INT64:field",
         "-o", out_path, csv_path},
        wout, werr);
    EXPECT_EQ(wc, 0) << werr.str();

    std::ostringstream cout_;
    std::ostringstream cerr_;
    int cc =
        tsfile_cli::run_cli({"count", "-f", "tsv", out_path}, cout_, cerr_);
    EXPECT_EQ(cc, 0);
    EXPECT_NE(cout_.str().find("\ts1\t3"), std::string::npos) << cout_.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    int rc = tsfile_cli::run_cli({"cat", "-m", "s1", "-f", "tsv", out_path},
                                 rout, rerr);
    EXPECT_EQ(rc, 0);
    EXPECT_EQ(rout.str(), "time\ts1\n0\t0\n1\t10\n2\t20\n");

    std::remove(csv_path.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteThenReadFloatDoubleRoundTripLossless) {
    std::string csv_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_fp_in", ".csv");
    {
        std::ofstream o(csv_path.c_str());
        o << "time,id1,f1,d1\n0,dev,0.1,0.1\n1,dev,3.4028235,"
             "3.141592653589793\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_fp_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc =
        tsfile_cli::run_cli({"write", "--table", "t1", "--columns",
                             "id1:STRING:tag,f1:FLOAT:field,d1:DOUBLE:field",
                             "-o", out_path, csv_path},
                            wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    int rc = tsfile_cli::run_cli({"cat", "-f", "json", out_path}, rout, rerr);
    ASSERT_EQ(rc, 0) << rerr.str();
    // Default ostream precision (6 sig digits) would print 0.1 / 3.40282 and
    // lose bits; max_digits10 keeps every digit needed to round-trip.
    EXPECT_NE(rout.str().find("0.100000001"), std::string::npos) << rout.str();
    EXPECT_NE(rout.str().find("3.40282345"), std::string::npos) << rout.str();
    EXPECT_NE(rout.str().find("3.1415926535897931"), std::string::npos)
        << rout.str();

    std::remove(csv_path.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteImportsQuotedFieldWithEmbeddedNewline) {
    std::string csv_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_nl_in", ".csv");
    {
        std::ofstream o(csv_path.c_str());
        // The note field on the first row spans two physical lines inside
        // quotes; it must import as a single row, not be split into two.
        o << "time,id1,note\n0,dev,\"line one\nline two\"\n1,dev,plain\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_nl_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t1", "--columns",
         "id1:STRING:tag,note:TEXT:field", "-o", out_path, csv_path},
        wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    std::ostringstream cout_;
    std::ostringstream cerr_;
    ASSERT_EQ(
        tsfile_cli::run_cli({"count", "-f", "tsv", out_path}, cout_, cerr_), 0);
    EXPECT_NE(cout_.str().find("\tnote\t2"), std::string::npos) << cout_.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    ASSERT_EQ(tsfile_cli::run_cli({"cat", "-f", "json", out_path}, rout, rerr),
              0);
    EXPECT_NE(rout.str().find("line one\\nline two"), std::string::npos)
        << rout.str();

    std::remove(csv_path.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteMissingColumnsIsUsageError) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"write", "--table", "t1", "-o", "x.tsfile", "in.csv"}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("--columns"), std::string::npos);
}

namespace {
bool path_exists(const std::string& p) {
    std::ifstream in(p.c_str());
    return in.good();
}
}  // namespace

TEST(CliE2E, WriteRejectsOutOfOrderTimestampsAndLeavesNoOutput) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_ooo", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n5,50\n1,10\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_ooo_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t", "--columns",
                                    "s1:INT64:field", "-o", out_path, csv},
                                   out, err);
    EXPECT_EQ(code, 3);
    EXPECT_NE(err.str().find("strictly increasing"), std::string::npos)
        << err.str();
    EXPECT_NE(err.str().find("line 3"), std::string::npos) << err.str();
    EXPECT_FALSE(path_exists(out_path)) << "failed import must leave no output";

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteAllowsSameTimestampAcrossDevices) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_md", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,id,s1\n1,A,10\n1,B,20\n2,A,30\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_md_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"write", "--table", "t", "--columns", "id:STRING:tag,s1:INT64:field",
         "-o", out_path, csv},
        out, err);
    EXPECT_EQ(code, 0) << err.str();

    std::ostringstream cout_;
    std::ostringstream cerr_;
    tsfile_cli::run_cli({"count", "-f", "tsv", out_path}, cout_, cerr_);
    EXPECT_NE(cout_.str().find("total\t\t3"), std::string::npos) << cout_.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteRejectsOutputEqualsInput) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_alias", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n0,1\n";
    }
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t", "--columns",
                                    "s1:INT64:field", "-o", csv, csv},
                                   out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("same as the input"), std::string::npos)
        << err.str();
    // The input file must be untouched.
    std::ifstream in(csv.c_str());
    std::stringstream buf;
    buf << in.rdbuf();
    EXPECT_EQ(buf.str(), "time,s1\n0,1\n");

    std::remove(csv.c_str());
}

TEST(CliE2E, WriteFailureOnBadValueLeavesNoOutput) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_badval", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n0,notanumber\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_badval_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t", "--columns",
                                    "s1:INT64:field", "-o", out_path, csv},
                                   out, err);
    EXPECT_EQ(code, 3);
    EXPECT_FALSE(path_exists(out_path));

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteRejectsDuplicateColumnNames) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"write", "--table", "t", "--columns", "s1:INT64:field,s1:INT64:field",
         "-o", "x.tsfile", "-"},
        out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("duplicate column"), std::string::npos)
        << err.str();
}

TEST(CliE2E, WriteRejectsHeaderMatchWithNoHeader) {
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"write", "--table", "t", "--columns", "s1:INT64:field", "-o",
         "x.tsfile", "--no-header", "--header-match", "-"},
        out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("--header-match"), std::string::npos) << err.str();
}

TEST(CliE2E, ReadRejectsWriteOnlyFlag) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"ls", "-o", "x.tsfile", f.path}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("only valid for write"), std::string::npos)
        << err.str();
}

TEST(CliE2E, MetaRejectsDeviceScopeFlag) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"meta", "-d", "dev", f.path}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("not valid for meta"), std::string::npos)
        << err.str();
}

TEST(CliE2E, SchemaTableShowsEncodingAndCompression) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"schema", "-f", "tsv", f.path}, out, err);
    EXPECT_EQ(code, 0);
    // Table-model schema must report real (non-empty) encoding and compression
    // rather than blanks. The INT64 field encodes as TS_2DIFF; the compression
    // is the engine default (build-dependent) but must not be empty.
    EXPECT_NE(out.str().find("\ts1\tINT64\tTS_2DIFF\t"), std::string::npos)
        << out.str();
    EXPECT_EQ(out.str().find("\ts1\tINT64\tTS_2DIFF\t\n"), std::string::npos)
        << out.str();
}

namespace {
// Run a one-row `write` whose single value cell is `value`, declaring the
// column as `type`. Returns the exit code; captures stderr into `err`.
int write_one_value(const std::string& type, const std::string& value,
                    std::string& err_out) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_ovf", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n0," << value << "\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_ovf_out", ".tsfile");
    std::ostringstream out;
    std::ostringstream err;
    int code =
        tsfile_cli::run_cli({"write", "--table", "t", "--columns",
                             "s1:" + type + ":field", "-o", out_path, csv},
                            out, err);
    err_out = err.str();
    std::remove(csv.c_str());
    std::remove(out_path.c_str());
    return code;
}
}  // namespace

TEST(CliE2E, WriteRejectsInt32Overflow) {
    std::string err;
    EXPECT_EQ(write_one_value("INT32", "3000000000", err), 3);
    EXPECT_NE(err.find("INT32 out of range"), std::string::npos) << err;
}

TEST(CliE2E, WriteAcceptsInt32Boundary) {
    std::string err;
    EXPECT_EQ(write_one_value("INT32", "2147483647", err), 0) << err;
}

TEST(CliE2E, WriteRejectsInt64Overflow) {
    std::string err;
    EXPECT_EQ(write_one_value("INT64", "99999999999999999999999999", err), 3);
    EXPECT_NE(err.find("INT64 out of range"), std::string::npos) << err;
}

TEST(CliE2E, WriteRejectsDoubleOverflow) {
    std::string err;
    EXPECT_EQ(write_one_value("DOUBLE", "1e400", err), 3);
    EXPECT_NE(err.find("DOUBLE out of range"), std::string::npos) << err;
}

TEST(CliE2E, WriteRejectsNonNumericInt64) {
    std::string err;
    EXPECT_EQ(write_one_value("INT64", "12abc", err), 3);
    EXPECT_NE(err.find("bad INT64"), std::string::npos) << err;
}

TEST(CliE2E, WriteRejectsOutOfOrderAcrossBatches) {
    // More than one 1024-row batch of ascending rows, then a violating
    // timestamp. The first batch is already flushed by the time the bad row is
    // read, so this proves both that per-device tracking survives a batch flush
    // and that the already-written output is removed on failure.
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_xbatch", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n";
        for (int i = 1; i <= 1100; ++i) {
            o << i << "," << i << "\n";
        }
        o << "500,999\n";  // <= the last timestamp for the tag-less device
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_xbatch_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "t", "--columns",
                                    "s1:INT64:field", "-o", out_path, csv},
                                   out, err);
    EXPECT_EQ(code, 3);
    EXPECT_NE(err.str().find("strictly increasing"), std::string::npos)
        << err.str();
    EXPECT_FALSE(path_exists(out_path));

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteStreamsLargeInputRoundTrips) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_large", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n";
        for (int i = 1; i <= 3000; ++i) {
            o << i << "," << (i * 2) << "\n";
        }
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_large_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"write", "--table", "big", "--columns",
                                    "s1:INT64:field", "-o", out_path, csv},
                                   out, err);
    EXPECT_EQ(code, 0) << err.str();

    std::ostringstream cout_;
    std::ostringstream cerr_;
    tsfile_cli::run_cli({"count", "-f", "tsv", out_path}, cout_, cerr_);
    EXPECT_NE(cout_.str().find("\ts1\t3000"), std::string::npos) << cout_.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, HelpWithPositionalFilePrintsUsage) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"cat", "--help", f.path}, out, err);
    EXPECT_EQ(code, 0);
    EXPECT_NE(out.str().find("Usage:"), std::string::npos) << out.str();
}

TEST(CliE2E, StatsRejectsRowOnlyFlag) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"stats", "--start", "1", f.path}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("only valid for head/cat/sample"),
              std::string::npos)
        << err.str();
}

TEST(CliE2E, LsRejectsMeasurementsFlag) {
    Fixture f;
    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli({"ls", "-m", "s1", f.path}, out, err);
    EXPECT_EQ(code, 1);
    EXPECT_NE(err.str().find("not valid for ls"), std::string::npos)
        << err.str();
}

TEST(CliE2E, WriteRoundTripsTimestampDateBlob) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_tdb", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,id1,ts1,d1,b1\n"
             "0,dev,1700000000000,2024-01-15,hello\n"
             "1,dev,1700000000001,2024-12-31,world\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_tdb_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t1", "--columns",
         "id1:STRING:tag,ts1:TIMESTAMP:field,d1:DATE:field,b1:BLOB:field", "-o",
         out_path, csv},
        wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    ASSERT_EQ(tsfile_cli::run_cli({"cat", "-f", "tsv", out_path}, rout, rerr),
              0)
        << rerr.str();
    // TIMESTAMP prints as raw epoch ms, DATE as YYYY-MM-DD, BLOB as its bytes.
    EXPECT_NE(rout.str().find("1700000000000"), std::string::npos)
        << rout.str();
    EXPECT_NE(rout.str().find("2024-01-15"), std::string::npos) << rout.str();
    EXPECT_NE(rout.str().find("2024-12-31"), std::string::npos) << rout.str();
    EXPECT_NE(rout.str().find("hello"), std::string::npos) << rout.str();
    EXPECT_NE(rout.str().find("world"), std::string::npos) << rout.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteRejectsBadDate) {
    std::string err;
    EXPECT_EQ(write_one_value("DATE", "not-a-date", err), 3);
    EXPECT_NE(err.find("bad DATE"), std::string::npos) << err;
}

TEST(CliE2E, WriteVerboseEchoesConfig) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_vb", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,s1\n0,1\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_vb_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code =
        tsfile_cli::run_cli({"write", "--table", "vt", "--columns",
                             "s1:INT64:field", "-v", "-o", out_path, csv},
                            out, err);
    EXPECT_EQ(code, 0) << err.str();
    EXPECT_NE(err.str().find("table=vt"), std::string::npos) << err.str();
    EXPECT_NE(err.str().find("column s1:INT64:field"), std::string::npos)
        << err.str();
    EXPECT_NE(err.str().find("wrote 1 rows"), std::string::npos) << err.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteHeaderMatchReportsMismatchPosition) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_hm", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,wrong\n0,1\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_hm_out", ".tsfile");

    std::ostringstream out;
    std::ostringstream err;
    int code = tsfile_cli::run_cli(
        {"write", "--table", "t", "--columns", "s1:INT64:field",
         "--header-match", "-o", out_path, csv},
        out, err);
    EXPECT_EQ(code, 3);
    EXPECT_NE(err.str().find("header column 2 is 'wrong'"), std::string::npos)
        << err.str();
    EXPECT_NE(err.str().find("expected 's1'"), std::string::npos) << err.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

// Every column carries a distinct, type-specific value, so the JSON output pins
// each column name to exactly one value. This is what guards the by-index
// add_value mapping in cmd_write: if any two columns were written to the wrong
// slot, a key/value pair below would mismatch.
TEST(CliE2E, WriteMapsEachColumnToItsOwnValue) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_colmap", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,a_bool,b_int,c_long,d_float,e_double,f_str,g_ts,h_date\n"
             "10,true,42,9000000000,1.5,3.25,hello,1700000000000,2024-06-15\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_colmap_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t1", "--columns",
         "a_bool:BOOLEAN:field,b_int:INT32:field,c_long:INT64:field,"
         "d_float:FLOAT:field,e_double:DOUBLE:field,f_str:STRING:field,"
         "g_ts:TIMESTAMP:field,h_date:DATE:field",
         "-o", out_path, csv},
        wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    ASSERT_EQ(tsfile_cli::run_cli({"cat", "-f", "json", out_path}, rout, rerr),
              0)
        << rerr.str();
    const std::string& j = rout.str();
    EXPECT_NE(j.find("\"a_bool\":true"), std::string::npos) << j;
    EXPECT_NE(j.find("\"b_int\":42"), std::string::npos) << j;
    EXPECT_NE(j.find("\"c_long\":9000000000"), std::string::npos) << j;
    EXPECT_NE(j.find("\"d_float\":1.5"), std::string::npos) << j;
    EXPECT_NE(j.find("\"e_double\":3.25"), std::string::npos) << j;
    EXPECT_NE(j.find("\"f_str\":\"hello\""), std::string::npos) << j;
    EXPECT_NE(j.find("\"g_ts\":1700000000000"), std::string::npos) << j;
    EXPECT_NE(j.find("\"h_date\":\"2024-06-15\""), std::string::npos) << j;

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

// A multi-type import spanning several batches must keep every value in its own
// column after each flush. Tags vary so timestamps may repeat across devices.
TEST(CliE2E, WriteMultiTypeAcrossBatchesRoundTrips) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_mtb", ".csv");
    const int kRows = 2500;  // > 2 batches of 1024
    {
        std::ofstream o(csv.c_str());
        o << "time,id,n,note\n";
        for (int i = 0; i < kRows; ++i) {
            o << i << ",dev," << (i * 3) << ",row" << i << "\n";
        }
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_mtb_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t", "--columns",
         "id:STRING:tag,n:INT64:field,note:TEXT:field", "-o", out_path, csv},
        wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    std::ostringstream cout_;
    std::ostringstream cerr_;
    ASSERT_EQ(
        tsfile_cli::run_cli({"count", "-f", "tsv", out_path}, cout_, cerr_), 0);
    EXPECT_NE(cout_.str().find("\tn\t2500"), std::string::npos) << cout_.str();

    // Spot-check a row from the last batch keeps n and note paired correctly.
    std::ostringstream rout;
    std::ostringstream rerr;
    ASSERT_EQ(tsfile_cli::run_cli({"cat", "--start", "2400", "--end", "2400",
                                   "-f", "json", out_path},
                                  rout, rerr),
              0)
        << rerr.str();
    EXPECT_NE(rout.str().find("\"n\":7200"), std::string::npos) << rout.str();
    EXPECT_NE(rout.str().find("\"note\":\"row2400\""), std::string::npos)
        << rout.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

// A quoted STRING field containing the delimiter and escaped quotes must
// survive import and re-export unchanged (RFC 4180 round-trip through the
// writer).
TEST(CliE2E, WriteRoundTripsQuotedSpecialChars) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_special", ".csv");
    {
        std::ofstream o(csv.c_str());
        // note = a,b "q" c  (comma + embedded quotes)
        o << "time,id,note\n0,dev,\"a,b \"\"q\"\" c\"\n";
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_special_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t", "--columns",
         "id:STRING:tag,note:STRING:field", "-o", out_path, csv},
        wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    // JSON escapes the embedded quotes; the comma is preserved verbatim.
    std::ostringstream rout;
    std::ostringstream rerr;
    ASSERT_EQ(tsfile_cli::run_cli({"cat", "-f", "json", out_path}, rout, rerr),
              0)
        << rerr.str();
    EXPECT_NE(rout.str().find("\"note\":\"a,b \\\"q\\\" c\""),
              std::string::npos)
        << rout.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}

TEST(CliE2E, WriteRejectsTimestampOverflow) {
    std::string err;
    EXPECT_EQ(write_one_value("TIMESTAMP", "99999999999999999999999999", err),
              3);
    EXPECT_NE(err.find("TIMESTAMP out of range"), std::string::npos) << err;
}

TEST(CliE2E, WriteRejectsNonNumericTimestampColumn) {
    std::string err;
    EXPECT_EQ(write_one_value("TIMESTAMP", "not-a-number", err), 3);
    EXPECT_NE(err.find("bad TIMESTAMP"), std::string::npos) << err;
}

TEST(CliE2E, WriteRejectsImpossibleDate) {
    // Syntactically YYYY-MM-DD but not a real calendar date.
    std::string err;
    EXPECT_EQ(write_one_value("DATE", "2024-13-40", err), 3);
    EXPECT_NE(err.find("bad DATE"), std::string::npos) << err;
}

TEST(CliE2E, WriteAcceptsDateBoundary) {
    std::string err;
    EXPECT_EQ(write_one_value("DATE", "2024-02-29", err), 0)
        << err;  // leap day
}

// An empty cell writes a null, which JSON renders as null (not the type's
// zero).
TEST(CliE2E, WriteEmptyCellBecomesNull) {
    std::string csv =
        tsfile_cli_test::unique_temp_path("tsfile_cli_null", ".csv");
    {
        std::ofstream o(csv.c_str());
        o << "time,id,n\n0,dev,\n";  // n is empty -> null
    }
    std::string out_path =
        tsfile_cli_test::unique_temp_path("tsfile_cli_null_out", ".tsfile");

    std::ostringstream wout;
    std::ostringstream werr;
    int wc = tsfile_cli::run_cli(
        {"write", "--table", "t", "--columns", "id:STRING:tag,n:INT64:field",
         "-o", out_path, csv},
        wout, werr);
    ASSERT_EQ(wc, 0) << werr.str();

    std::ostringstream rout;
    std::ostringstream rerr;
    ASSERT_EQ(tsfile_cli::run_cli({"cat", "-f", "json", out_path}, rout, rerr),
              0)
        << rerr.str();
    EXPECT_NE(rout.str().find("\"n\":null"), std::string::npos) << rout.str();

    std::remove(csv.c_str());
    std::remove(out_path.c_str());
}
