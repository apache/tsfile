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

package org.apache.tsfile.enums;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import static org.junit.Assert.assertSame;

public class TSDataTypeSerializationTest {

  /**
   * Flink's ClosureCleaner serializes every non-static, non-transient field of an object that a
   * user function holds, including the fields of an enum constant. A field that is not serializable
   * makes it reject the function, although Java serializes the enum constant only by its name.
   */
  @Test
  public void testEveryCheckedFieldIsSerializable() throws Exception {
    for (TSDataType dataType : TSDataType.values()) {
      for (Field field : TSDataType.class.getDeclaredFields()) {
        int modifiers = field.getModifiers();
        if (Modifier.isStatic(modifiers) || Modifier.isTransient(modifiers)) {
          continue;
        }
        field.setAccessible(true);
        serialize(field.get(dataType));
      }
    }
  }

  @Test
  public void testJavaSerializationKeepsTheConstant() throws Exception {
    for (TSDataType dataType : TSDataType.values()) {
      try (ObjectInputStream in =
          new ObjectInputStream(new ByteArrayInputStream(serialize(dataType)))) {
        assertSame(dataType, in.readObject());
      }
    }
  }

  private static byte[] serialize(Object value) throws IOException {
    ByteArrayOutputStream bytes = new ByteArrayOutputStream();
    try (ObjectOutputStream out = new ObjectOutputStream(bytes)) {
      out.writeObject(value);
    }
    return bytes.toByteArray();
  }
}
