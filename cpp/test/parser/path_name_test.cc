/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * License); you may not use this file except in compliance
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

#include "common/path.h"

namespace storage {

class PathNameTest : public ::testing::Test {};

TEST_F(PathNameTest, TestLegalPath) {
    // empty path
    Path a("", true);
    EXPECT_EQ("", a.device_);
    EXPECT_EQ("", a.measurement_);

    // empty device
    Path b("s1", true);
    EXPECT_EQ("s1", b.measurement_);
    EXPECT_EQ("", b.device_);

    // normal node
    Path c("root.sg.a", true);
    EXPECT_EQ("root.sg", c.device_);
    EXPECT_EQ("a", c.measurement_);

    // quoted node
    Path d("root.sg.`a.b`", true);
    EXPECT_EQ("root.sg", d.device_);
    EXPECT_EQ("`a.b`", d.measurement_);

    Path e("root.sg.`a.``b`", true);
    EXPECT_EQ("root.sg", e.device_);
    EXPECT_EQ("`a.``b`", e.measurement_);

    Path f("root.`sg\"`.`a.``b`", true);
    EXPECT_EQ("root.`sg\"`", f.device_);
    EXPECT_EQ("`a.``b`", f.measurement_);

    Path g("root.sg.`a.b\\\\`", true);
    EXPECT_EQ("root.sg", g.device_);
    EXPECT_EQ("`a.b\\\\`", g.measurement_);

    // quoted node of digits
    Path h("root.sg.`111`", true);
    EXPECT_EQ("root.sg", h.device_);
    EXPECT_EQ("`111`", h.measurement_);

    // quoted node of key word
    Path i("root.sg.`select`", true);
    EXPECT_EQ("root.sg", i.device_);
    EXPECT_EQ("select", i.measurement_);

    // wildcard
    Path j("root.sg.`a*b`", true);
    EXPECT_EQ("root.sg", j.device_);
    EXPECT_EQ("`a*b`", j.measurement_);

    Path k("root.sg.*", true);
    EXPECT_EQ("root.sg", k.device_);
    EXPECT_EQ("*", k.measurement_);

    Path l("root.sg.**", true);
    EXPECT_EQ("root.sg", l.device_);
    EXPECT_EQ("**", l.measurement_);

    // raw key word
    Path m("root.sg.select", true);
    EXPECT_EQ("root.sg", m.device_);
    EXPECT_EQ("select", m.measurement_);

    Path n("root.sg.device", true);
    EXPECT_EQ("root.sg", n.device_);
    EXPECT_EQ("device", n.measurement_);

    Path o("root.sg.drop_trigger", true);
    EXPECT_EQ("root.sg", o.device_);
    EXPECT_EQ("drop_trigger", o.measurement_);

    Path p("root.sg.and", true);
    EXPECT_EQ("root.sg", p.device_);
    EXPECT_EQ("and", p.measurement_);

    p = Path("root.sg.or", true);
    EXPECT_EQ("root.sg", p.device_);
    EXPECT_EQ("or", p.measurement_);

    p = Path("root.sg.not", true);
    EXPECT_EQ("root.sg", p.device_);
    EXPECT_EQ("not", p.measurement_);

    p = Path("root.sg.null", true);
    EXPECT_EQ("root.sg", p.device_);
    EXPECT_EQ("null", p.measurement_);

    p = Path("root.sg.contains", true);
    EXPECT_EQ("root.sg", p.device_);
    EXPECT_EQ("contains", p.measurement_);

    p = Path("root.sg.`0000`", true);
    EXPECT_EQ("root.sg", p.device_);
    EXPECT_EQ("`0000`", p.measurement_);

    p = Path("root.sg.`0e38`", true);
    EXPECT_EQ("root.sg", p.device_);
    EXPECT_EQ("`0e38`", p.measurement_);

    p = Path("root.sg.`00.12`", true);
    EXPECT_EQ("root.sg", p.device_);
    EXPECT_EQ("`00.12`", p.measurement_);
}

TEST_F(PathNameTest, TestIllegalPathName) {
    EXPECT_THROW ({
      Path("root.sg`", true);
    } , std::runtime_error);

    EXPECT_THROW ({
      Path("root.sg\na", true);
    }, std::runtime_error);

    EXPECT_THROW ({
      Path("root.select`", true);
    } , std::runtime_error);

    EXPECT_THROW ({
      // pure digits
      Path("root.111", true);
    } , std::runtime_error);

    EXPECT_THROW ({
      // single ` in quoted node
      Path("root.`a``", true);
    } , std::runtime_error);

    EXPECT_THROW ({
      // single ` in quoted node
      Path("root.``a`", true);
    } , std::runtime_error);

    EXPECT_THROW ({
      Path("root.a*%", true);
    } , std::runtime_error);

    EXPECT_THROW ({
      Path("root.a*b", true);
    } , std::runtime_error);

    EXPECT_THROW ({
      Path("root.0e38", true);
    } , std::runtime_error);

    EXPECT_THROW ({
      Path("root.0000", true);
    }, std::runtime_error);
}
 

}  // namespace storage
