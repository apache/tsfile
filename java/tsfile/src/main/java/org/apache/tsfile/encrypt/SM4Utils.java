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

// import sun.nio.cs.GBK;

import java.util.Arrays;

// import sun.misc.BASE64Decoder;
// import sun.misc.BASE64Encoder;

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
    //			System.out.println("iv:"+iv);
    byte[] ivBytes = Arrays.copyOfRange(pivBytes, 0, 16);
    //      System.out.println(ivBytes.length);
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
      //        System.out.println(encrypted.length);
      for (int j = 0; j < 16; j++) {
        data[16 * i + j] = (byte) (data[16 * i + j] ^ encrypted[j]);
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
        data[16 * group_num + j] = (byte) (data[16 * group_num + j] ^ encrypted[j]);
      }
    }
    //      System.out.println(data.length);
    ////			Base64.Encoder encoder = Base64.getEncoder();
    ////			String cipherText = new BASE64Encoder().encode(encrypted);
    //			String cipherText = new String(data,Charset.forName("GBK"));
    //			System.out.println(cipherText);
    //			byte[] d = cipherText.getBytes(Charset.forName("GBK"));
    //			System.out.println(d.length);
    //			if (cipherText != null && cipherText.trim().length() > 0)
    //			{
    //				Pattern p = Pattern.compile("\\s*|\t|\r|\n");
    //				Matcher m = p.matcher(cipherText);
    //				cipherText = m.replaceAll("");
    //			}
    return data;
  }
}
