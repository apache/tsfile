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
package org.apache.tsfile.file.header;

import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.TestAeadEncryptionProvider;
import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.file.MetaMarker;

import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;

public class FileEncryptionHeaderTest {

  @Test
  public void testRoundTrip() throws IOException {
    byte[] dataKey = new byte[16];
    byte[] fileCryptoId = new byte[EncryptParameter.FILE_CRYPTO_ID_LENGTH];
    for (int i = 0; i < dataKey.length; i++) {
      dataKey[i] = (byte) (i + 1);
      fileCryptoId[i] = (byte) (0x40 + i);
    }
    EncryptParameter parameter = TestAeadEncryptionProvider.createParameter(dataKey, fileCryptoId);

    ByteArrayOutputStream output = new ByteArrayOutputStream();
    FileEncryptionHeader.serialize(parameter, output);
    byte[] serialized = output.toByteArray();
    assertEquals(MetaMarker.ENCRYPTION_HEADER, serialized[0]);
    assertFalse(contains(serialized, dataKey));

    ByteArrayInputStream input = new ByteArrayInputStream(serialized);
    assertEquals(MetaMarker.ENCRYPTION_HEADER, input.read());
    EncryptParameter restored = FileEncryptionHeader.deserialize(input);

    assertNull(restored.getType());
    assertNull(restored.getKey());
    assertEquals(parameter.getProviderId(), restored.getProviderId());
    assertEquals(parameter.getProfileId(), restored.getProfileId());
    assertEquals(parameter.getKeyId(), restored.getKeyId());
    assertEquals(parameter.getKeyVersion(), restored.getKeyVersion());
    assertArrayEquals(parameter.getFileCryptoId(), restored.getFileCryptoId());
    assertArrayEquals(parameter.getWrappedDataKey(), restored.getWrappedDataKey());
  }

  @Test
  public void testRejectInvalidFileCryptoId() {
    EncryptParameter parameter =
        TestAeadEncryptionProvider.createParameter(new byte[16], new byte[8]);
    assertThrows(
        EncryptException.class,
        () -> FileEncryptionHeader.serialize(parameter, new ByteArrayOutputStream()));
  }

  private static boolean contains(byte[] data, byte[] target) {
    for (int i = 0; i <= data.length - target.length; i++) {
      boolean matches = true;
      for (int j = 0; j < target.length; j++) {
        if (data[i + j] != target[j]) {
          matches = false;
          break;
        }
      }
      if (matches) {
        return true;
      }
    }
    return false;
  }
}
