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

package org.apache.tsfile.encrypt;

import java.util.Arrays;

public class SM4Utils {
  public byte[] getSecretKey() {
    return secretKey;
  }

  public void setSecretKey(byte[] secretKey) {
    this.secretKey = secretKey;
  }

  private byte[] secretKey;

  public byte[] getIv() {
    return iv;
  }

  public void setIv(String iv) {
    this.iv = iv.getBytes();
  }

  private byte[] iv;

  private SM4_Context ctx;

  private SM4 sm4;

  public SM4Utils(byte[] key, byte[] iv) {
    this.secretKey = key;
    this.iv = iv;
    this.ctx = new SM4_Context();
    sm4 = new SM4();
    sm4.sm4_setkey_enc(ctx, secretKey);
  }

  public byte[] cryptData_CTR(byte[] data) {
    byte[] keyBytes = secretKey;
    byte[] pivBytes = iv;
    byte[] result = new byte[data.length];
    byte[] ivBytes = Arrays.copyOfRange(pivBytes, 0, 16);
    int begin_iv = 0;
    for (int i = 0; i < 4; i++) {
      begin_iv = (begin_iv << 8) | (ivBytes[i] & 0xFF);
    }
    int group_num = data.length / 16;
    for (int i = 0; i < group_num; i++) {
      ivBytes[0] = (byte) ((begin_iv >>> 24) & 0xFF);
      ivBytes[1] = (byte) ((begin_iv >>> 16) & 0xFF);
      ivBytes[2] = (byte) ((begin_iv >>> 8) & 0xFF);
      ivBytes[3] = (byte) (begin_iv & 0xFF);
      byte[] encrypted = sm4.sm4_crypt_ecb(ctx, ivBytes);
      for (int j = 0; j < 16; j++) {
        result[16 * i + j] = (byte) (data[16 * i + j] ^ encrypted[j]);
      }
      begin_iv++;
    }
    if (data.length % 16 != 0) {
      ivBytes[0] = (byte) ((begin_iv >>> 24) & 0xFF);
      ivBytes[1] = (byte) ((begin_iv >>> 16) & 0xFF);
      ivBytes[2] = (byte) ((begin_iv >>> 8) & 0xFF);
      ivBytes[3] = (byte) (begin_iv & 0xFF);
      byte[] encrypted = sm4.sm4_crypt_ecb(ctx, ivBytes);
      for (int j = 0; j < data.length % 16; j++) {
        result[16 * group_num + j] = (byte) (data[16 * group_num + j] ^ encrypted[j]);
      }
    }
    return result;
  }
}
