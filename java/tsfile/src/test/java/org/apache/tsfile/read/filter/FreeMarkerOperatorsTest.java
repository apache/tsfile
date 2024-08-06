/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tsfile.read.filter;

import java.util.HashSet;
import java.util.Set;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.filter.basic.Filter;
import org.apache.tsfile.read.filter.factory.FilterFactory;
import org.apache.tsfile.read.filter.factory.ValueFilterApi;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.RegexUtils;
import org.junit.Assert;
import org.junit.Test;

public class FreeMarkerOperatorsTest {

  @Test
  public void testBooleanFilter() {
    Filter eqFilter = ValueFilterApi.eq(0, true, TSDataType.BOOLEAN);
    Assert.assertTrue(eqFilter.satisfy(100, true));
    Assert.assertFalse(eqFilter.satisfy(100, false));

    Filter notEqFilter = ValueFilterApi.notEq(0, true, TSDataType.BOOLEAN);
    Assert.assertTrue(notEqFilter.satisfy(100, false));
    Assert.assertFalse(notEqFilter.satisfy(100, true));

    Filter ltFilter = ValueFilterApi.lt(0, true, TSDataType.BOOLEAN);
    Assert.assertTrue(ltFilter.satisfy(100, false));
    Assert.assertFalse(ltFilter.satisfy(100, true));

    Filter ltEqFilter = ValueFilterApi.ltEq(0, true, TSDataType.BOOLEAN);
    Assert.assertTrue(ltEqFilter.satisfy(100, false));
    Assert.assertTrue(ltEqFilter.satisfy(100, true));

    Filter gtFilter = ValueFilterApi.gt(0, false, TSDataType.BOOLEAN);
    Assert.assertTrue(gtFilter.satisfy(100, true));
    Assert.assertFalse(gtFilter.satisfy(100, false));

    Filter gtEqFilter = ValueFilterApi.gtEq(0, true, TSDataType.BOOLEAN);
    Assert.assertTrue(gtEqFilter.satisfy(100, true));
    Assert.assertFalse(gtEqFilter.satisfy(100, false));

    Set<Boolean> set = new HashSet<>();
    set.add(true);
    set.add(false);

    Filter inFilter = ValueFilterApi.in(0, set, TSDataType.BOOLEAN);
    Assert.assertTrue(inFilter.satisfy(100, true));
    Assert.assertTrue(inFilter.satisfy(100, false));

    Filter notInFilter = ValueFilterApi.notIn(0, set, TSDataType.BOOLEAN);
    Assert.assertFalse(notInFilter.satisfy(100, true));
    Assert.assertFalse(notInFilter.satisfy(100, false));
  }

  @Test
  public void testIntegerFilter() {
    // untested: isNull、isNotNull、like、notLike、regexp、notRegexp

    Set<Integer> set = new HashSet<>();
    set.add(100);
    set.add(200);

    Filter gtFilter = ValueFilterApi.gt(0, 100, TSDataType.INT32);
    Assert.assertTrue(gtFilter.satisfy(100, 101));
    Assert.assertFalse(gtFilter.satisfy(100, 100));

    Filter gtEqFilter = ValueFilterApi.gtEq(0, 100, TSDataType.INT32);
    Assert.assertTrue(gtEqFilter.satisfy(100, 100));
    Assert.assertFalse(gtEqFilter.satisfy(100, 99));

    Filter ltFilter = ValueFilterApi.lt(0, 100, TSDataType.INT32);
    Assert.assertTrue(ltFilter.satisfy(100, 99));
    Assert.assertFalse(ltFilter.satisfy(100, 101));

    Filter ltEqFilter = ValueFilterApi.ltEq(0, 100, TSDataType.INT32);
    Assert.assertTrue(ltEqFilter.satisfy(100, 99));
    Assert.assertTrue(ltEqFilter.satisfy(100, 100));

    Filter eqFilter = ValueFilterApi.eq(0, 100, TSDataType.INT32);
    Assert.assertTrue(eqFilter.satisfy(100, 100));
    Assert.assertFalse(eqFilter.satisfy(100, 200));

    Filter notEqFilter = ValueFilterApi.notEq(0, 100, TSDataType.INT32);
    Assert.assertTrue(notEqFilter.satisfy(100, 200));
    Assert.assertFalse(notEqFilter.satisfy(100, 100));

    Filter betweenFilter = ValueFilterApi.between(0, 100, 200, TSDataType.INT32);
    Assert.assertTrue(betweenFilter.satisfy(100, 150));
    Assert.assertFalse(betweenFilter.satisfy(100, 300));

    Filter notBetweenFilter = ValueFilterApi.notBetween(0, 100, 200, TSDataType.INT32);
    Assert.assertTrue(notBetweenFilter.satisfy(100, 300));
    Assert.assertFalse(notBetweenFilter.satisfy(100, 150));

    Filter inFilter = ValueFilterApi.in(0, set, TSDataType.INT32);
    Assert.assertTrue(inFilter.satisfy(100, 100));
    Assert.assertFalse(inFilter.satisfy(100, 300));

    Filter notInFilter = ValueFilterApi.notIn(0, set, TSDataType.INT32);
    Assert.assertTrue(notInFilter.satisfy(100, 300));
    Assert.assertFalse(notInFilter.satisfy(100, 200));

    Filter andFilter =
        FilterFactory.and(
            ValueFilterApi.gt(0, 1, TSDataType.INT32), ValueFilterApi.lt(1, 3, TSDataType.INT32));
    Assert.assertTrue(andFilter.satisfy(100, 2));
    Assert.assertFalse(andFilter.satisfy(100, 4));
  }

  @Test
  public void testLongFilter() {
    // untested: isNull、isNotNull、like、notLike、regexp、notRegexp

    Set<Long> set = new HashSet<>();
    set.add(100L);
    set.add(200L);

    Filter gtFilter = ValueFilterApi.gt(0, 100L, TSDataType.INT64);
    Assert.assertTrue(gtFilter.satisfy(100, 101L));
    Assert.assertFalse(gtFilter.satisfy(100, 100L));

    Filter gtEqFilter = ValueFilterApi.gtEq(0, 100L, TSDataType.INT64);
    Assert.assertTrue(gtEqFilter.satisfy(100, 100L));
    Assert.assertFalse(gtEqFilter.satisfy(100, 99L));

    Filter ltFilter = ValueFilterApi.lt(0, 100L, TSDataType.INT64);
    Assert.assertTrue(ltFilter.satisfy(100, 99L));
    Assert.assertFalse(ltFilter.satisfy(100, 101L));

    Filter ltEqFilter = ValueFilterApi.ltEq(0, 100L, TSDataType.INT64);
    Assert.assertTrue(ltEqFilter.satisfy(100, 99L));
    Assert.assertTrue(ltEqFilter.satisfy(100, 100L));

    Filter eqFilter = ValueFilterApi.eq(0, 100L, TSDataType.INT64);
    Assert.assertTrue(eqFilter.satisfy(100, 100L));
    Assert.assertFalse(eqFilter.satisfy(100, 200L));

    Filter notEqFilter = ValueFilterApi.notEq(0, 100L, TSDataType.INT64);
    Assert.assertTrue(notEqFilter.satisfy(100, 200L));
    Assert.assertFalse(notEqFilter.satisfy(100, 100L));

    Filter betweenFilter = ValueFilterApi.between(0, 100L, 200L, TSDataType.INT64);
    Assert.assertTrue(betweenFilter.satisfy(100, 150L));
    Assert.assertFalse(betweenFilter.satisfy(100, 300L));

    Filter notBetweenFilter = ValueFilterApi.notBetween(0, 100L, 200L, TSDataType.INT64);
    Assert.assertTrue(notBetweenFilter.satisfy(100, 300L));
    Assert.assertFalse(notBetweenFilter.satisfy(100, 150L));

    Filter inFilter = ValueFilterApi.in(0, set, TSDataType.INT64);
    Assert.assertTrue(inFilter.satisfy(100, 100L));
    Assert.assertFalse(inFilter.satisfy(100, 300L));

    Filter notInFilter = ValueFilterApi.notIn(0, set, TSDataType.INT64);
    Assert.assertTrue(notInFilter.satisfy(100, 300L));
    Assert.assertFalse(notInFilter.satisfy(100, 200L));
  }

  @Test
  public void testFloatFilter() {
    // untested: isNull、isNotNull、like、notLike、regexp、notRegexp

    Set<Float> set = new HashSet<>();
    set.add(100.0F);
    set.add(200.0F);

    Filter gtFilter = ValueFilterApi.gt(0, 100.0F, TSDataType.FLOAT);
    Assert.assertTrue(gtFilter.satisfy(100, 101.0F));
    Assert.assertFalse(gtFilter.satisfy(100, 100.0F));

    Filter gtEqFilter = ValueFilterApi.gtEq(0, 100.0F, TSDataType.FLOAT);
    Assert.assertTrue(gtEqFilter.satisfy(100, 100.0F));
    Assert.assertFalse(gtEqFilter.satisfy(100, 99.0F));

    Filter ltFilter = ValueFilterApi.lt(0, 100.0F, TSDataType.FLOAT);
    Assert.assertTrue(ltFilter.satisfy(100, 99.0F));
    Assert.assertFalse(ltFilter.satisfy(100, 101.0F));

    Filter ltEqFilter = ValueFilterApi.ltEq(0, 100F, TSDataType.FLOAT);
    Assert.assertTrue(ltEqFilter.satisfy(100, 99.0F));
    Assert.assertTrue(ltEqFilter.satisfy(100, 100.0F));

    Filter eqFilter = ValueFilterApi.eq(0, 100.0F, TSDataType.FLOAT);
    Assert.assertTrue(eqFilter.satisfy(100, 100.0F));
    Assert.assertFalse(eqFilter.satisfy(100, 200.0F));

    Filter notEqFilter = ValueFilterApi.notEq(0, 100.0F, TSDataType.FLOAT);
    Assert.assertTrue(notEqFilter.satisfy(100, 200.0F));
    Assert.assertFalse(notEqFilter.satisfy(100, 100.0F));

    Filter betweenFilter = ValueFilterApi.between(0, 100.0F, 200.0F, TSDataType.FLOAT);
    Assert.assertTrue(betweenFilter.satisfy(100, 150.0F));
    Assert.assertFalse(betweenFilter.satisfy(100, 300.0F));

    Filter notBetweenFilter = ValueFilterApi.notBetween(0, 100.0F, 200.0F, TSDataType.FLOAT);
    Assert.assertTrue(notBetweenFilter.satisfy(100, 300.0F));
    Assert.assertFalse(notBetweenFilter.satisfy(100, 150.0F));

    Filter inFilter = ValueFilterApi.in(0, set, TSDataType.FLOAT);
    Assert.assertTrue(inFilter.satisfy(100, 100.0F));
    Assert.assertFalse(inFilter.satisfy(100, 300.0F));

    Filter notInFilter = ValueFilterApi.notIn(0, set, TSDataType.FLOAT);
    Assert.assertTrue(notInFilter.satisfy(100, 300.0F));
    Assert.assertFalse(notInFilter.satisfy(100, 200.0F));
  }

  @Test
  public void testDoubleFilter() {
    // untested: isNull、isNotNull、like、notLike、regexp、notRegexp

    Set<Double> set = new HashSet<>();
    set.add(100.0D);
    set.add(200.0D);

    Filter gtFilter = ValueFilterApi.gt(0, 100.0D, TSDataType.DOUBLE);
    Assert.assertTrue(gtFilter.satisfy(100, 101.0D));
    Assert.assertFalse(gtFilter.satisfy(100, 100.0D));

    Filter gtEqFilter = ValueFilterApi.gtEq(0, 100.0D, TSDataType.DOUBLE);
    Assert.assertTrue(gtEqFilter.satisfy(100, 100.0D));
    Assert.assertFalse(gtEqFilter.satisfy(100, 99.0D));

    Filter ltFilter = ValueFilterApi.lt(0, 100.0D, TSDataType.DOUBLE);
    Assert.assertTrue(ltFilter.satisfy(100, 99.0D));
    Assert.assertFalse(ltFilter.satisfy(100, 101.0D));

    Filter ltEqFilter = ValueFilterApi.ltEq(0, 100D, TSDataType.DOUBLE);
    Assert.assertTrue(ltEqFilter.satisfy(100, 99.0D));
    Assert.assertTrue(ltEqFilter.satisfy(100, 100.0D));

    Filter eqFilter = ValueFilterApi.eq(0, 100.0D, TSDataType.DOUBLE);
    Assert.assertTrue(eqFilter.satisfy(100, 100.0D));
    Assert.assertFalse(eqFilter.satisfy(100, 200.0D));

    Filter notEqFilter = ValueFilterApi.notEq(0, 100.0D, TSDataType.DOUBLE);
    Assert.assertTrue(notEqFilter.satisfy(100, 200.0D));
    Assert.assertFalse(notEqFilter.satisfy(100, 100.0D));

    Filter betweenFilter = ValueFilterApi.between(0, 100.0D, 200.0D, TSDataType.DOUBLE);
    Assert.assertTrue(betweenFilter.satisfy(100, 150.0D));
    Assert.assertFalse(betweenFilter.satisfy(100, 300.0D));

    Filter notBetweenFilter = ValueFilterApi.notBetween(0, 100.0D, 200.0D, TSDataType.DOUBLE);
    Assert.assertTrue(notBetweenFilter.satisfy(100, 300.0D));
    Assert.assertFalse(notBetweenFilter.satisfy(100, 150.0D));

    Filter inFilter = ValueFilterApi.in(0, set, TSDataType.DOUBLE);
    Assert.assertTrue(inFilter.satisfy(100, 100.0D));
    Assert.assertFalse(inFilter.satisfy(100, 300.0D));

    Filter notInFilter = ValueFilterApi.notIn(0, set, TSDataType.DOUBLE);
    Assert.assertTrue(notInFilter.satisfy(100, 300.0D));
    Assert.assertFalse(notInFilter.satisfy(100, 200.0D));
  }

  @Test
  public void testBinaryFilter() {
    // untested: gt、gtEq、lt、ltEq、isNull、between、notBetween、like、notLike
    Set<Binary> set = new HashSet<>();
    set.add(new Binary("100".getBytes()));
    set.add(new Binary("200".getBytes()));

    Filter eqFilter = ValueFilterApi.eq(0, new Binary("100".getBytes()), TSDataType.TEXT);
    Assert.assertTrue(eqFilter.satisfy(100, new Binary("100".getBytes())));
    Assert.assertFalse(eqFilter.satisfy(100, new Binary("200".getBytes())));

    Filter notEqFilter = ValueFilterApi.notEq(0, new Binary("100".getBytes()), TSDataType.TEXT);
    Assert.assertTrue(notEqFilter.satisfy(100, new Binary("200".getBytes())));
    Assert.assertFalse(notEqFilter.satisfy(100, new Binary("100".getBytes())));

    Filter isNullFilter = ValueFilterApi.isNotNull(0, TSDataType.TEXT);
    Assert.assertFalse(isNullFilter.satisfy(100, null));
    Assert.assertTrue(isNullFilter.satisfy(100, new Binary("100".getBytes())));

    Filter regexpFilter = ValueFilterApi.regexp(0, RegexUtils.compileRegex("1*0"), TSDataType.TEXT);
    Assert.assertTrue(regexpFilter.satisfy(100, new Binary("100".getBytes())));
    Assert.assertFalse(regexpFilter.satisfy(100, new Binary("99".getBytes())));

    Filter notRegexpFilter =
        ValueFilterApi.notRegexp(0, RegexUtils.compileRegex("1*0"), TSDataType.TEXT);
    Assert.assertTrue(notRegexpFilter.satisfy(100, new Binary("99".getBytes())));
    Assert.assertFalse(notRegexpFilter.satisfy(100, new Binary("100".getBytes())));

    Filter inFilter = ValueFilterApi.in(0, set, TSDataType.TEXT);
    Assert.assertTrue(inFilter.satisfy(100, new Binary("100".getBytes())));
    Assert.assertFalse(inFilter.satisfy(100, new Binary("300".getBytes())));

    Filter notInFilter = ValueFilterApi.notIn(0, set, TSDataType.TEXT);
    Assert.assertTrue(notInFilter.satisfy(100, new Binary("300".getBytes())));
    Assert.assertFalse(notInFilter.satisfy(100, new Binary("200".getBytes())));
  }
}
