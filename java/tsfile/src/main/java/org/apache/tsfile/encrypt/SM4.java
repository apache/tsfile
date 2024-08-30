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

import org.apache.tsfile.exception.encrypt.EncryptException;
import org.apache.tsfile.exception.encrypt.EncryptKeyLengthNotMatchException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class SM4 {
  public static final int SM4_ENCRYPT = 1;

  public static final int SM4_DECRYPT = 0;

  private long GET_ULONG_BE(byte[] b, int i) {
    long n =
        (long) (b[i] & 0xff) << 24
            | (long) ((b[i + 1] & 0xff) << 16)
            | (long) ((b[i + 2] & 0xff) << 8)
            | (long) (b[i + 3] & 0xff) & 0xffffffffL;
    return n;
  }

  private void PUT_ULONG_BE(long n, byte[] b, int i) {
    b[i] = (byte) (int) (0xFF & n >> 24);
    b[i + 1] = (byte) (int) (0xFF & n >> 16);
    b[i + 2] = (byte) (int) (0xFF & n >> 8);
    b[i + 3] = (byte) (int) (0xFF & n);
  }

  private long SHL(long x, int n) {
    return (x & 0xFFFFFFFF) << n;
  }

  private long ROTL(long x, int n) {
    return SHL(x, n) | x >> (32 - n);
  }

  private void SWAP(long[] sk, int i) {
    long t = sk[i];
    sk[i] = sk[(31 - i)];
    sk[(31 - i)] = t;
  }

  public static final byte[] SboxTable =
      EncryptUtils.hexStringToByteArray(
          "D690E9FECCE13DB716B614C228FB2C052B679A762ABE04C3AA441326498606999C4250F491EF987A33540B43EDCFAC62E4B31CA9C908E89580DF94FA758F3FA64707A7FCF37317BA83593C19E6854FA8686B81B27164DA8BF8EB0F4B70569D351E240E5E6358D1A225227C3B01217887D40046579FD327524C3602E7A0C4C89EEABF8AD240C738B5A3F7F2CEF96115A1E0AE5DA49B341A55AD933230F58CB1E31DF6E22E8266CA60C02923AB0D534E6FD5DB3745DEFD8E2F03FF6A726D6C5B518D1BAF92BBDDBC7F11D95C411F105AD80AC13188A5CD7BBD2D74D012B8E5B4B08969974A0C96777E65B9F109C56EC68418F07DEC3ADC4D2079EE5F3ED7CB3948");

  public static final int[] FK = {0xa3b1bac6, 0x56aa3350, 0x677d9197, 0xb27022dc};

  public static final int[] CK = {
    0x00070e15,
    0x1c232a31,
    0x383f464d,
    0x545b6269,
    0x70777e85,
    0x8c939aa1,
    0xa8afb6bd,
    0xc4cbd2d9,
    0xe0e7eef5,
    0xfc030a11,
    0x181f262d,
    0x343b4249,
    0x50575e65,
    0x6c737a81,
    0x888f969d,
    0xa4abb2b9,
    0xc0c7ced5,
    0xdce3eaf1,
    0xf8ff060d,
    0x141b2229,
    0x30373e45,
    0x4c535a61,
    0x686f767d,
    0x848b9299,
    0xa0a7aeb5,
    0xbcc3cad1,
    0xd8dfe6ed,
    0xf4fb0209,
    0x10171e25,
    0x2c333a41,
    0x484f565d,
    0x646b7279
  };

  private byte sm4Sbox(byte inch) {
    int i = inch & 0xFF;
    byte retVal = SboxTable[i];
    return retVal;
  }

  private long sm4Lt(long ka) {
    long bb = 0L;
    long c = 0L;
    byte[] a = new byte[4];
    byte[] b = new byte[4];
    PUT_ULONG_BE(ka, a, 0);
    b[0] = sm4Sbox(a[0]);
    b[1] = sm4Sbox(a[1]);
    b[2] = sm4Sbox(a[2]);
    b[3] = sm4Sbox(a[3]);
    bb = GET_ULONG_BE(b, 0);
    c = bb ^ ROTL(bb, 2) ^ ROTL(bb, 10) ^ ROTL(bb, 18) ^ ROTL(bb, 24);
    return c;
  }

  private long sm4F(long x0, long x1, long x2, long x3, long rk) {
    return x0 ^ sm4Lt(x1 ^ x2 ^ x3 ^ rk);
  }

  private long sm4CalciRK(long ka) {
    long bb = 0L;
    long rk = 0L;
    byte[] a = new byte[4];
    byte[] b = new byte[4];
    PUT_ULONG_BE(ka, a, 0);
    b[0] = sm4Sbox(a[0]);
    b[1] = sm4Sbox(a[1]);
    b[2] = sm4Sbox(a[2]);
    b[3] = sm4Sbox(a[3]);
    bb = GET_ULONG_BE(b, 0);
    rk = bb ^ ROTL(bb, 13) ^ ROTL(bb, 23);
    return rk;
  }

  private void sm4_setkey(long[] SK, byte[] key) {
    long[] MK = new long[4];
    long[] k = new long[36];
    int i = 0;
    MK[0] = GET_ULONG_BE(key, 0);
    MK[1] = GET_ULONG_BE(key, 4);
    MK[2] = GET_ULONG_BE(key, 8);
    MK[3] = GET_ULONG_BE(key, 12);
    k[0] = MK[0] ^ (long) FK[0];
    k[1] = MK[1] ^ (long) FK[1];
    k[2] = MK[2] ^ (long) FK[2];
    k[3] = MK[3] ^ (long) FK[3];
    for (; i < 32; i++) {
      k[(i + 4)] = (k[i] ^ sm4CalciRK(k[(i + 1)] ^ k[(i + 2)] ^ k[(i + 3)] ^ (long) CK[i]));
      SK[i] = k[(i + 4)];
    }
  }

  private void sm4_one_round(long[] sk, byte[] input, byte[] output) {
    int i = 0;
    long[] ulbuf = new long[36];
    ulbuf[0] = GET_ULONG_BE(input, 0);
    ulbuf[1] = GET_ULONG_BE(input, 4);
    ulbuf[2] = GET_ULONG_BE(input, 8);
    ulbuf[3] = GET_ULONG_BE(input, 12);
    while (i < 32) {
      ulbuf[(i + 4)] = sm4F(ulbuf[i], ulbuf[(i + 1)], ulbuf[(i + 2)], ulbuf[(i + 3)], sk[i]);
      i++;
    }
    PUT_ULONG_BE(ulbuf[35], output, 0);
    PUT_ULONG_BE(ulbuf[34], output, 4);
    PUT_ULONG_BE(ulbuf[33], output, 8);
    PUT_ULONG_BE(ulbuf[32], output, 12);
  }

  private byte[] padding(byte[] input, int mode) {
    if (input == null) {
      return null;
    }

    byte[] ret = (byte[]) null;
    if (mode == SM4_ENCRYPT) {
      int p = 16 - input.length % 16;
      ret = new byte[input.length + p];
      System.arraycopy(input, 0, ret, 0, input.length);
      for (int i = 0; i < p; i++) {
        ret[input.length + i] = (byte) p;
      }
    } else {
      int p = input[input.length - 1];
      ret = new byte[input.length - p];
      System.arraycopy(input, 0, ret, 0, input.length - p);
    }
    return ret;
  }

  public void sm4_setkey_enc(SM4_Context ctx, byte[] key) {
    if (ctx == null) {
      throw new EncryptException("sm4 ctx is null!");
    }

    if (key == null) {
      throw new EncryptException("sm4 key null error!");
    }
    if (key.length != 16) {
      throw new EncryptKeyLengthNotMatchException(key.length, 16);
    }

    ctx.mode = SM4_ENCRYPT;
    sm4_setkey(ctx.sk, key);
  }

  public void sm4_setkey_dec(SM4_Context ctx, byte[] key) {
    if (ctx == null) {
      throw new EncryptException("sm4 ctx is null!");
    }

    if (key == null) {
      throw new EncryptException("sm4 key null error!");
    }
    if (key.length != 16) {
      throw new EncryptKeyLengthNotMatchException(key.length, 16);
    }

    int i = 0;
    ctx.mode = SM4_DECRYPT;
    sm4_setkey(ctx.sk, key);
    for (i = 0; i < 16; i++) {
      SWAP(ctx.sk, i);
    }
  }

  public byte[] sm4_crypt_ecb(SM4_Context ctx, byte[] input) {
    try {
      if (input == null) {
        throw new EncryptException("sm4 input is null!");
      }
      if ((ctx.isPadding) && (ctx.mode == SM4_ENCRYPT)) {
        input = padding(input, SM4_ENCRYPT);
      }

      int length = input.length;
      ByteArrayInputStream bins = new ByteArrayInputStream(input);
      ByteArrayOutputStream bous = new ByteArrayOutputStream();
      for (; length > 0; length -= 16) {
        byte[] in = new byte[16];
        byte[] out = new byte[16];
        bins.read(in);
        sm4_one_round(ctx.sk, in, out);
        bous.write(out);
      }

      byte[] output = bous.toByteArray();
      if (ctx.isPadding && ctx.mode == SM4_DECRYPT) {
        output = padding(output, SM4_DECRYPT);
      }
      bins.close();
      bous.close();
      return output;
    } catch (IOException e) {
      throw new EncryptException("sm4 crypt error");
    }
  }

  public byte[] sm4_crypt_cbc(SM4_Context ctx, byte[] iv, byte[] input) throws Exception {
    if (iv == null || iv.length != 16) {
      throw new EncryptException("iv error!");
    }

    if (input == null) {
      throw new EncryptException("input is null!");
    }

    if (ctx.isPadding && ctx.mode == SM4_ENCRYPT) {
      input = padding(input, SM4_ENCRYPT);
    }

    int i = 0;
    int length = input.length;
    ByteArrayInputStream bins = new ByteArrayInputStream(input);
    ByteArrayOutputStream bous = new ByteArrayOutputStream();
    if (ctx.mode == SM4_ENCRYPT) {
      for (; length > 0; length -= 16) {
        byte[] in = new byte[16];
        byte[] out = new byte[16];
        byte[] out1 = new byte[16];

        bins.read(in);
        for (i = 0; i < 16; i++) {
          out[i] = ((byte) (in[i] ^ iv[i]));
        }
        sm4_one_round(ctx.sk, out, out1);
        System.arraycopy(out1, 0, iv, 0, 16);
        bous.write(out1);
      }
    } else {
      byte[] temp = new byte[16];
      for (; length > 0; length -= 16) {
        byte[] in = new byte[16];
        byte[] out = new byte[16];
        byte[] out1 = new byte[16];

        bins.read(in);
        System.arraycopy(in, 0, temp, 0, 16);
        sm4_one_round(ctx.sk, in, out);
        for (i = 0; i < 16; i++) {
          out1[i] = ((byte) (out[i] ^ iv[i]));
        }
        System.arraycopy(temp, 0, iv, 0, 16);
        bous.write(out1);
      }
    }

    byte[] output = bous.toByteArray();
    if (ctx.isPadding && ctx.mode == SM4_DECRYPT) {
      output = padding(output, SM4_DECRYPT);
    }
    bins.close();
    bous.close();
    return output;
  }
}
