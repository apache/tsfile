/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License a
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
#include <vector>
#include <cmath>
#include <cstdlib>

// Include your new encoder and decoder header files
#include "encoding/kcluster_encoder.h"
#include "encoding/kcluster_decoder.h"
#include "encoding/acluster_encoder.h"
#include "encoding/acluster_decoder.h"

// Note: The original test uses raw new/delete. The framework might be using
// a custom memory allocator via the factory. For a standalone test, raw new/delete is fine.
// If integrated into the project's test runner, using the factory would be better.
// We will follow the style of ts2diff_codec_test.cc.

namespace storage {

class ClusterCodecTest : public ::testing::Test {
   protected:
    void SetUp() override {
        // KCluster instances
        kcluster_encoder_double_ = new DoubleKClusterEncoder();
        kcluster_decoder_double_ = new DoubleKClusterDecoder();
        kcluster_encoder_int_ = new IntKClusterEncoder();
        kcluster_decoder_int_ = new IntKClusterDecoder();
        
        // ACluster instances
        acluster_encoder_double_ = new DoubleAClusterEncoder();
        acluster_decoder_double_ = new DoubleAClusterDecoder();
        acluster_encoder_int_ = new IntAClusterEncoder();
        acluster_decoder_int_ = new IntAClusterDecoder();
    }

    void TearDown() override {
        // KCluster
        delete kcluster_encoder_double_;
        delete kcluster_decoder_double_;
        delete kcluster_encoder_int_;
        delete kcluster_decoder_int_;

        // ACluster
        delete acluster_encoder_double_;
        delete acluster_decoder_double_;
        delete acluster_encoder_int_;
        delete acluster_decoder_int_;
    }

    // KCluster pointers
    DoubleKClusterEncoder* kcluster_encoder_double_;
    DoubleKClusterDecoder* kcluster_decoder_double_;
    IntKClusterEncoder* kcluster_encoder_int_;
    IntKClusterDecoder* kcluster_decoder_int_;
    
    // ACluster pointers
    DoubleAClusterEncoder* acluster_encoder_double_;
    DoubleAClusterDecoder* acluster_decoder_double_;
    IntAClusterEncoder* acluster_encoder_int_;
    IntAClusterDecoder* acluster_decoder_int_;
};


// ===================================================================
//   KCluster Tests
// ===================================================================

TEST_F(ClusterCodecTest, KClusterDoubleEncoding) {
    common::ByteStream out_stream(1024, common::MOD_DEFAULT, false);
    const int row_num = 1000; // Your algorithm is page-based, so let's test with a number of points that fits in a page.
    std::vector<double> data(row_num);
    
    // Generate some test data, e.g., a sine wave with some noise
    for (int i = 0; i < row_num; i++) {
        data[i] = 100.0 * std::sin(i * 0.1) + (static_cast<double>(rand()) / RAND_MAX - 0.5);
    }

    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(kcluster_encoder_double_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(kcluster_encoder_double_->flush(out_stream), common::E_OK);

    double x;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(kcluster_decoder_double_->read_double(x, out_stream), common::E_OK);
        // Due to scaling and floating point precision, use ASSERT_NEAR for comparison
        ASSERT_NEAR(x, data[i], 1e-9); 
    }
}

TEST_F(ClusterCodecTest, KClusterIntEncoding) {
    common::ByteStream out_stream(1024, common::MOD_DEFAULT, false);
    const int row_num = 1000;
    std::vector<int64_t> data(row_num);
    
    // Generate some integer data
    for (int i = 0; i < row_num; i++) {
        data[i] = static_cast<int64_t>(i * i) - (i * 10);
    }

    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(kcluster_encoder_int_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(kcluster_encoder_int_->flush(out_stream), common::E_OK);

    int64_t x;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(kcluster_decoder_int_->read_int64(x, out_stream), common::E_OK);
        EXPECT_EQ(x, data[i]);
    }
}


// ===================================================================
//   ACluster Tests
// ===================================================================

TEST_F(ClusterCodecTest, AClusterDoubleEncoding) {
    common::ByteStream out_stream(1024, common::MOD_DEFAULT, false);
    const int row_num = 1000;
    std::vector<double> data(row_num);
    
    // Generate some test data, e.g., a linear slope with some periodic component
    for (int i = 0; i < row_num; i++) {
        data[i] = 0.5 * i + 50.0 * std::cos(i * 0.2);
    }

    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(acluster_encoder_double_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(acluster_encoder_double_->flush(out_stream), common::E_OK);

    double x;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(acluster_decoder_double_->read_double(x, out_stream), common::E_OK);
        ASSERT_NEAR(x, data[i], 1e-9);
    }
}

TEST_F(ClusterCodecTest, AClusterIntEncoding) {
    common::ByteStream out_stream(1024, common::MOD_DEFAULT, false);
    const int row_num = 1000;
    std::vector<int64_t> data(row_num);
    
    // Generate some different integer data
    for (int i = 0; i < row_num; i++) {
        data[i] = 10000 + (i % 50) * 100; // Data that should cluster well
    }

    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(acluster_encoder_int_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(acluster_encoder_int_->flush(out_stream), common::E_OK);

    int64_t x;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(acluster_decoder_int_->read_int64(x, out_stream), common::E_OK);
        EXPECT_EQ(x, data[i]);
    }
}

TEST_F(ClusterCodecTest, KClusterFloatAsDoubleEncoding) {
    common::ByteStream out_stream(1024, common::MOD_DEFAULT, false);
    const int row_num = 500;
    std::vector<float> data(row_num);
    
    for (int i = 0; i < row_num; i++) {
        data[i] = 123.45f + static_cast<float>(i);
    }

    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(kcluster_encoder_double_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(kcluster_encoder_double_->flush(out_stream), common::E_OK);

    float x;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(kcluster_decoder_double_->read_float(x, out_stream), common::E_OK);
        ASSERT_NEAR(x, data[i], 1e-6);
    }
}

TEST_F(ClusterCodecTest, AClusterInt32AsInt64Encoding) {
    common::ByteStream out_stream(1024, common::MOD_DEFAULT, false);
    const int row_num = 500;
    std::vector<int32_t> data(row_num);
    
    for (int i = 0; i < row_num; i++) {
        data[i] = 500 - i;
    }

    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(acluster_encoder_int_->encode(data[i], out_stream), common::E_OK);
    }
    EXPECT_EQ(acluster_encoder_int_->flush(out_stream), common::E_OK);

    int32_t x;
    for (int i = 0; i < row_num; i++) {
        EXPECT_EQ(acluster_decoder_int_->read_int32(x, out_stream), common::E_OK);
        EXPECT_EQ(x, data[i]);
    }
}

}  // namespace storage