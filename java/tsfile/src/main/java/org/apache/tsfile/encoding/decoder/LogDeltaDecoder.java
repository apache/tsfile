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

package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * LogDeltaDecoder is a decoder for decompressing text data that was encoded using LogDelta encoding.
 * This decoder works in conjunction with LogDeltaEncoder to provide efficient decompression of
 * text data that was compressed using the LogDelta algorithm.
 * 
 * The decoder reconstructs the original text by:
 * 1. Reading record counts and method information
 * 2. Decoding RLE-encoded method data
 * 3. Reconstructing text strings from operations or direct storage
 * 4. Maintaining a sliding window for reference strings
 */
public class LogDeltaDecoder extends Decoder {
  private static final Logger logger = LoggerFactory.getLogger(LogDeltaDecoder.class);
  
  // Internal state
  private int method0Count;
  private int method1Count;
  private int currentMethod0Index;
  private int currentMethod1Index;
  private List<Integer> methodList;
  private int currentMethodIndex;
  
  // Decoded data
  private List<Integer> beginPositions;
  private List<Integer> operationSizes;
  private List<Integer> allLengths;
  private List<Integer> allPositions;
  private List<String> allStrings;
  private List<String> slidingWindow;
  
  // Current decoding state
  private boolean initialized;
  private int currentStringIndex;
  private String currentString;
  private int currentStringPosition;

  public LogDeltaDecoder() {
    super(TSEncoding.LOG_DELTA);
    this.reset();
    logger.debug("tsfile-decoding LogDeltaDecoder: text compression decoder initialized");
  }

  @Override
  public Binary readBinary(ByteBuffer buffer) {
    if (!initialized) {
      initializeDecoder(buffer);
    }
    
    if (currentString == null) {
      currentString = decodeNextString(buffer);
    }
    
    if (currentString == null) {
      throw new IllegalStateException("No more data to read");
    }
    
    String result = currentString;
    currentString = null; // Prepare for next call
    return new Binary(result);
  }

  @Override
  public boolean hasNext(ByteBuffer buffer) {
    if (!initialized) {
      initializeDecoder(buffer);
    }
    
    return currentMethodIndex < methodList.size() || buffer.remaining() > 0;
  }

  @Override
  public void reset() {
    this.method0Count = 0;
    this.method1Count = 0;
    this.currentMethod0Index = 0;
    this.currentMethod1Index = 0;
    this.methodList = new ArrayList<>();
    this.currentMethodIndex = 0;
    this.beginPositions = new ArrayList<>();
    this.operationSizes = new ArrayList<>();
    this.allLengths = new ArrayList<>();
    this.allPositions = new ArrayList<>();
    this.allStrings = new ArrayList<>();
    this.slidingWindow = new ArrayList<>();
    this.initialized = false;
    this.currentStringIndex = 0;
    this.currentString = null;
    this.currentStringPosition = 0;
  }

  /**
   * Initialize the decoder with header information
   */
  private void initializeDecoder(ByteBuffer buffer) {
    if (initialized) {
      return;
    }
    
    try {
      // Read record counts
      method0Count = buffer.getInt();
      method1Count = buffer.getInt();
      
      // Decode method information using RLE
      methodList = decodeRLE(buffer);
      
      // Decode method 0 records (with operations)
      if (method0Count > 0) {
        decodeMethod0Records(buffer);
      }
      
      // Decode method 1 records (direct strings)
      if (method1Count > 0) {
        decodeMethod1Records(buffer);
      }
      
      initialized = true;
      logger.debug("LogDeltaDecoder initialized with {} method0 and {} method1 records", 
                   method0Count, method1Count);
    } catch (Exception e) {
      logger.error("Error initializing LogDeltaDecoder", e);
      throw new RuntimeException("Failed to initialize decoder", e);
    }
  }

  /**
   * Decode RLE-encoded method information
   */
  private List<Integer> decodeRLE(ByteBuffer buffer) {
    int length = buffer.getInt();
    int intervalCount = buffer.getInt();
    
    if (length == 0 || intervalCount == 0) {
      return new ArrayList<>();
    }
    
    // Read encoded data
    byte[] encodedData = new byte[length];
    buffer.get(encodedData);
    
    // Convert to binary string
    StringBuilder binaryString = new StringBuilder();
    for (byte b : encodedData) {
      String binary = String.format("%8s", Integer.toBinaryString(b & 0xFF))
          .replace(' ', '0');
      binaryString.append(binary);
    }
    
    // Decode intervals
    List<Integer> result = new ArrayList<>();
    int currentValue = binaryString.charAt(0) - '0';
    int pos = 1;
    
    for (int i = 0; i < intervalCount; i++) {
      int interval = decodeNumber(binaryString, pos);
      pos = findNextNumberPosition(binaryString, pos);
      
      for (int j = 0; j < interval; j++) {
        result.add(currentValue);
      }
      currentValue = 1 - currentValue; // Toggle between 0 and 1
    }
    
    return result;
  }

  /**
   * Decode a number from binary string
   */
  private int decodeNumber(StringBuilder binary, int startPos) {
    int pos = startPos;
    int countOnes = 0;
    
    // Count leading ones
    while (pos < binary.length() && binary.charAt(pos) == '1') {
      countOnes++;
      pos++;
    }
    
    // Skip the '0'
    if (pos < binary.length()) {
      pos++;
    }
    
    // Read the binary number
    int bitLength = countOnes + 2;
    int value = 0;
    
    for (int i = 0; i < bitLength && pos + i < binary.length(); i++) {
      value = (value << 1) | (binary.charAt(pos + i) - '0');
    }
    
    return value;
  }

  /**
   * Find the next number position in binary string
   */
  private int findNextNumberPosition(StringBuilder binary, int currentPos) {
    int pos = currentPos;
    
    // Skip current number
    while (pos < binary.length() && binary.charAt(pos) == '1') {
      pos++;
    }
    if (pos < binary.length()) {
      pos++; // Skip the '0'
    }
    
    // Skip the binary digits
    int countOnes = 0;
    int startPos = pos;
    while (startPos > 0 && binary.charAt(startPos - 1) == '1') {
      countOnes++;
      startPos--;
    }
    int bitLength = countOnes + 2;
    pos += bitLength;
    
    return pos;
  }

  /**
   * Decode method 0 records (with operations)
   */
  private void decodeMethod0Records(ByteBuffer buffer) {
    // Decode begin positions
    beginPositions = decodeDeltaSequence(buffer);
    
    // Decode operation sizes
    operationSizes = decodeDeltaSequence(buffer);
    
    // Decode all lengths
    allLengths = decodeDeltaSequence(buffer);
    
    // Decode all positions
    allPositions = decodeDeltaSequence(buffer);
    
    // Decode strings (they are stored as raw bytes)
    // Note: In a real implementation, we would need to know the string boundaries
    // For now, we'll assume strings are null-terminated or have length prefixes
    decodeStrings(buffer);
  }

  /**
   * Decode method 1 records (direct strings)
   */
  private void decodeMethod1Records(ByteBuffer buffer) {
    // Read strings until we have method1Count strings
    for (int i = 0; i < method1Count; i++) {
      String str = readStringUntilNewline(buffer);
      allStrings.add(str);
    }
  }

  /**
   * Decode a delta-encoded sequence
   */
  private List<Integer> decodeDeltaSequence(ByteBuffer buffer) {
    if (buffer.remaining() < 4) {
      return new ArrayList<>();
    }
    
    int firstValue = buffer.getInt();
    int minDelta = buffer.getInt();
    int deltaCount = buffer.getInt();
    int bitWidth = buffer.get() & 0xFF;
    
    if (deltaCount == 0) {
      List<Integer> result = new ArrayList<>();
      result.add(firstValue);
      return result;
    }
    
    // Decode deltas
    List<Integer> deltas = new ArrayList<>();
    if (bitWidth > 0) {
      int totalBits = deltaCount * bitWidth;
      int totalBytes = (totalBits + 7) / 8;
      
      byte[] packedData = new byte[totalBytes];
      buffer.get(packedData);
      
      int bitOffset = 0;
      for (int i = 0; i < deltaCount; i++) {
        int delta = readBits(packedData, bitOffset, bitWidth);
        deltas.add(delta);
        bitOffset += bitWidth;
      }
    } else {
      // All deltas are 0
      for (int i = 0; i < deltaCount; i++) {
        deltas.add(0);
      }
    }
    
    // Reconstruct original values
    List<Integer> result = new ArrayList<>();
    result.add(firstValue);
    
    int currentValue = firstValue;
    for (int normalizedDelta : deltas) {
      int actualDelta = normalizedDelta + minDelta;
      currentValue += actualDelta;
      result.add(currentValue);
    }
    
    return result;
  }

  /**
   * Read bits from packed data
   */
  private int readBits(byte[] data, int bitOffset, int bitWidth) {
    int value = 0;
    int byteIndex = bitOffset / 8;
    int bitIndex = bitOffset % 8;
    
    for (int i = 0; i < bitWidth; i++) {
      if (byteIndex >= data.length) {
        break;
      }
      
      int bit = (data[byteIndex] >> (7 - bitIndex)) & 1;
      value = (value << 1) | bit;
      
      bitIndex++;
      if (bitIndex == 8) {
        bitIndex = 0;
        byteIndex++;
      }
    }
    
    return value;
  }

  /**
   * Decode strings from buffer
   */
  private void decodeStrings(ByteBuffer buffer) {
    // This is a simplified implementation
    // In practice, we would need to know the exact string boundaries
    while (buffer.hasRemaining()) {
      String str = readStringUntilNewline(buffer);
      if (str != null) {
        allStrings.add(str);
      }
    }
  }

  /**
   * Read a string until newline or end of buffer
   */
  private String readStringUntilNewline(ByteBuffer buffer) {
    if (!buffer.hasRemaining()) {
      return null;
    }
    
    StringBuilder sb = new StringBuilder();
    while (buffer.hasRemaining()) {
      byte b = buffer.get();
      if (b == '\n') {
        break;
      }
      sb.append((char) b);
    }
    
    return sb.toString();
  }

  /**
   * Decode the next string from the buffer
   */
  private String decodeNextString(ByteBuffer buffer) {
    if (currentMethodIndex >= methodList.size()) {
      return null;
    }
    
    int method = methodList.get(currentMethodIndex);
    currentMethodIndex++;
    
    if (method == 1) {
      // Direct string storage
      if (currentMethod1Index < method1Count) {
        String result = allStrings.get(currentMethod1Index);
        currentMethod1Index++;
        updateSlidingWindow(result);
        return result;
      }
    } else {
      // String with operations
      if (currentMethod0Index < method0Count) {
        String result = reconstructStringFromOperations(buffer);
        currentMethod0Index++;
        updateSlidingWindow(result);
        return result;
      }
    }
    
    return null;
  }

  /**
   * Reconstruct a string from operations
   */
  private String reconstructStringFromOperations(ByteBuffer buffer) {
    if (currentMethod0Index >= beginPositions.size()) {
      return "";
    }
    
    int beginPos = beginPositions.get(currentMethod0Index);
    int operationSize = operationSizes.get(currentMethod0Index);
    
    // Get reference string from sliding window
    String referenceString = "";
    if (beginPos < slidingWindow.size()) {
      referenceString = slidingWindow.get(beginPos);
    }
    
    // Apply operations
    StringBuilder result = new StringBuilder();
    int currentPos = 0;
    
    for (int i = 0; i < operationSize; i++) {
      if (currentStringPosition >= allPositions.size()) {
        break;
      }
      
      int position = allPositions.get(currentStringPosition++);
      int deleteLength = allLengths.get(currentStringPosition++);
      int insertLength = allLengths.get(currentStringPosition++);
      
      // Copy unchanged part
      if (position > currentPos) {
        result.append(referenceString.substring(currentPos, position));
      }
      
      // Skip deleted part
      currentPos = position + deleteLength;
      
      // Insert new text
      if (insertLength > 0) {
        // Find the corresponding string in allStrings
        // This is simplified - in practice we'd need better string management
        if (currentStringIndex < allStrings.size()) {
          String insertText = allStrings.get(currentStringIndex++);
          result.append(insertText);
        }
      }
    }
    
    // Append remaining part
    if (currentPos < referenceString.length()) {
      result.append(referenceString.substring(currentPos));
    }
    
    return result.toString();
  }

  /**
   * Update the sliding window with a new string
   */
  private void updateSlidingWindow(String text) {
    slidingWindow.add(text);
    
    // Keep window size manageable
    if (slidingWindow.size() > 8) {
      slidingWindow.remove(0);
    }
  }
}
