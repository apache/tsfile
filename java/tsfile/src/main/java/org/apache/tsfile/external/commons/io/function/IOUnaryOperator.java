/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tsfile.external.commons.io.function;

import java.io.IOException;
import java.util.function.UnaryOperator;

/**
 * Like {@link UnaryOperator} but throws {@link IOException}.
 *
 * @param <T> the type of the operand and result of the operator.
 * @see UnaryOperator
 * @see IOFunction
 * @since 2.12.0
 */
@FunctionalInterface
public interface IOUnaryOperator<T> extends IOFunction<T, T> {}
