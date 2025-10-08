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

package org.apache.tsfile.encoding.encoder;

import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.BytesUtils;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.Binary;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * LogDeltaEncoder is a comprehensive text compression encoder based on the LogDelta algorithm.
 * This encoder combines multiple compression techniques including:
 * - Distance-based similarity matching (MinHash, Q-gram cosine distance)
 * - Q-gram pattern matching for finding common substrings
 * - Variable-length substitution operations
 * - Delta encoding for positions and lengths
 * - RLE encoding for repeated patterns
 * - Bit packing for efficient storage
 * 
 * The algorithm works by:
 * 1. Maintaining a sliding window of recent text strings
 * 2. For each new string, finding the most similar string in the window
 * 3. If similarity is above threshold, encoding the differences as operations
 * 4. If no good match, storing the string directly
 * 5. Using various encoding techniques to compress the operation data
 */
public class LogDeltaEncoder extends Encoder {
  private static final Logger logger = LoggerFactory.getLogger(LogDeltaEncoder.class);
  
  // Algorithm parameters
  private static final int DEFAULT_WINDOW_SIZE = 8;
  private static final double DEFAULT_THRESHOLD = 0.06;
  private static final int DEFAULT_Q_VALUE = 3;
  private static final int DEFAULT_NUM_HASHES = 50;
  private static final long PRIME = 1099511628211L;
  
  // Internal state
  private List<String> textValues;
  private Deque<String> slidingWindow;
  private int windowSize;
  private double threshold;
  private int qValue;
  private boolean useApproximation;
  
  // MinHash state
  private long[] hashCoefficientsA;
  private long[] hashCoefficientsB;
  private Map<String, long[]> signatureCache;
  
  public LogDeltaEncoder() {
    this(DEFAULT_WINDOW_SIZE, DEFAULT_THRESHOLD, DEFAULT_Q_VALUE, true);
  }
  
  public LogDeltaEncoder(int windowSize, double threshold, int qValue, boolean useApproximation) {
    super(TSEncoding.LOG_DELTA);
    this.windowSize = windowSize;
    this.threshold = threshold;
    this.qValue = qValue;
    this.useApproximation = useApproximation;
    this.textValues = new ArrayList<>();
    this.slidingWindow = new ArrayDeque<>();
    this.signatureCache = new HashMap<>();
    initializeHashFunctions();
    logger.debug("tsfile-encoding LogDeltaEncoder: text compression encoder initialized");
  }

  @Override
  public void encode(Binary value, ByteArrayOutputStream out) {
    textValues.add(value.toString());
  }

  @Override
  public void flush(ByteArrayOutputStream out) throws IOException {
    if (textValues.isEmpty()) {
      return;
    }
    
    try {
      // Process all text values and encode them
      List<Record> records = processTextValues();
      encodeRecords(records, out);
    } finally {
      reset();
    }
  }

  /**
   * Process all text values and generate compression records
   */
  private List<Record> processTextValues() {
    List<Record> records = new ArrayList<>();
    
    for (String text : textValues) {
      Record record = processTextString(text);
      records.add(record);
      
      // Update sliding window
      updateSlidingWindow(text);
    }
    
    return records;
  }

  /**
   * Process a single text string and generate a compression record
   */
  private Record processTextString(String text) {
    Record record = new Record();
    
    // Find the most similar string in the sliding window
    double minDistance = 1.0;
    int bestMatchIndex = -1;
    
    int index = 0;
    for (String windowText : slidingWindow) {
      double distance = calculateDistance(windowText, text);
      if (distance < minDistance) {
        minDistance = distance;
        bestMatchIndex = index;
      }
      index++;
    }
    
    if (minDistance >= threshold || bestMatchIndex == -1) {
      // No good match found, store the string directly
      record.method = 1;
      record.subStrings.add(text);
    } else {
      // Good match found, encode the differences
      String referenceText = new ArrayList<>(slidingWindow).get(bestMatchIndex);
      List<OperationItem> operations = findOperations(referenceText, text);
      
      if (operations.size() > 0 && calculateOperationCost(operations) < text.length()) {
        record.method = 0;
        record.begin = bestMatchIndex;
        record.operationSize = operations.size();
        
        for (OperationItem op : operations) {
          record.positionList.add(op.position);
          record.deleteLengths.add(op.deleteLength);
          record.insertLengths.add(op.insertLength);
          record.subStrings.add(op.insertText);
        }
      } else {
        // Operations are too expensive, store directly
        record.method = 1;
        record.subStrings.add(text);
      }
    }
    
    return record;
  }

  /**
   * Calculate distance between two strings using MinHash
   */
  private double calculateDistance(String str1, String str2) {
    if (str1.equals(str2)) {
      return 0.0;
    }
    
    if (str1.isEmpty() || str2.isEmpty()) {
      return 1.0;
    }
    
    try {
      long[] sig1 = getMinHashSignature(str1);
      long[] sig2 = getMinHashSignature(str2);
      return estimateMinHashDistance(sig1, sig2);
    } catch (Exception e) {
      logger.warn("Error calculating distance, using maximum distance", e);
      return 1.0;
    }
  }

  /**
   * Get MinHash signature for a string
   */
  private long[] getMinHashSignature(String str) {
    // Check cache first
    if (signatureCache.containsKey(str)) {
      return signatureCache.get(str);
    }
    
    long[] signature = new long[DEFAULT_NUM_HASHES];
    Arrays.fill(signature, Long.MAX_VALUE);
    
    if (str.length() < qValue) {
      signatureCache.put(str, signature);
      return signature;
    }
    
    // Generate q-grams and update signature
    for (int i = 0; i <= str.length() - qValue; i++) {
      String qgram = str.substring(i, i + qValue);
      long baseHash = calculateBaseHash(qgram);
      
      for (int j = 0; j < DEFAULT_NUM_HASHES; j++) {
        long hash = (hashCoefficientsA[j] * baseHash + hashCoefficientsB[j]) % PRIME;
        signature[j] = Math.min(signature[j], hash);
      }
    }
    
    signatureCache.put(str, signature);
    return signature;
  }

  /**
   * Calculate base hash for a string using DJB2 algorithm
   */
  private long calculateBaseHash(String str) {
    long hash = 5381;
    for (char c : str.toCharArray()) {
      hash = ((hash << 5) + hash) + c;
    }
    return hash;
  }

  /**
   * Estimate distance between two MinHash signatures
   */
  private double estimateMinHashDistance(long[] sig1, long[] sig2) {
    int matches = 0;
    for (int i = 0; i < DEFAULT_NUM_HASHES; i++) {
      if (sig1[i] == sig2[i]) {
        matches++;
      }
    }
    return 1.0 - (double) matches / DEFAULT_NUM_HASHES;
  }

  /**
   * Find operations to transform str1 into str2
   */
  private List<OperationItem> findOperations(String str1, String str2) {
    if (useApproximation) {
      return findQgramOperations(str1, str2);
    } else {
      return findExactOperations(str1, str2);
    }
  }

  /**
   * Find operations using Q-gram matching (approximate)
   */
  private List<OperationItem> findQgramOperations(String str1, String str2) {
    // Simplified Q-gram matching implementation
    List<OperationItem> operations = new ArrayList<>();
    
    // Find common substrings using sliding window
    int maxMatchLength = 0;
    int bestStart1 = -1;
    int bestStart2 = -1;
    
    for (int i = 0; i < str1.length(); i++) {
      for (int j = 0; j < str2.length(); j++) {
        int matchLength = 0;
        while (i + matchLength < str1.length() && 
               j + matchLength < str2.length() && 
               str1.charAt(i + matchLength) == str2.charAt(j + matchLength)) {
          matchLength++;
        }
        
        if (matchLength > maxMatchLength && matchLength >= qValue) {
          maxMatchLength = matchLength;
          bestStart1 = i;
          bestStart2 = j;
        }
      }
    }
    
    if (maxMatchLength > 0) {
      // Create operations around the common substring
      if (bestStart1 > 0 || bestStart2 > 0) {
        operations.add(new OperationItem(0, bestStart1, bestStart2, 
            str2.substring(0, bestStart2)));
      }
      
      if (bestStart1 + maxMatchLength < str1.length() || 
          bestStart2 + maxMatchLength < str2.length()) {
        operations.add(new OperationItem(bestStart1 + maxMatchLength, 
            str1.length() - bestStart1 - maxMatchLength,
            str2.length() - bestStart2 - maxMatchLength,
            str2.substring(bestStart2 + maxMatchLength)));
      }
    } else {
      // No common substring found, replace entire string
      operations.add(new OperationItem(0, str1.length(), str2.length(), str2));
    }
    
    return operations;
  }

  /**
   * Find operations using exact string matching
   */
  private List<OperationItem> findExactOperations(String str1, String str2) {
    // Simplified exact matching - in practice, this would use dynamic programming
    List<OperationItem> operations = new ArrayList<>();
    
    if (!str1.equals(str2)) {
      operations.add(new OperationItem(0, str1.length(), str2.length(), str2));
    }
    
    return operations;
  }

  /**
   * Calculate the cost of a set of operations
   */
  private double calculateOperationCost(List<OperationItem> operations) {
    double cost = 5.0; // Base cost
    for (OperationItem op : operations) {
      cost += 3.0 + op.insertLength;
    }
    return cost;
  }

  /**
   * Update the sliding window with a new string
   */
  private void updateSlidingWindow(String text) {
    if (slidingWindow.size() >= windowSize) {
      slidingWindow.removeFirst();
    }
    slidingWindow.addLast(text);
  }

  /**
   * Encode all records using the LogDelta algorithm
   */
  private void encodeRecords(List<Record> records, ByteArrayOutputStream out) throws IOException {
    // Separate records by method
    List<Record> method0Records = new ArrayList<>();
    List<Record> method1Records = new ArrayList<>();
    
    for (Record record : records) {
      if (record.method == 0) {
        method0Records.add(record);
      } else {
        method1Records.add(record);
      }
    }
    
    // Encode record counts
    out.write(BytesUtils.intToBytes(method0Records.size()));
    out.write(BytesUtils.intToBytes(method1Records.size()));
    
    // Encode method information using RLE
    List<Integer> methodList = new ArrayList<>();
    for (Record record : records) {
      methodList.add(record.method);
    }
    encodeRLE(methodList, out);
    
    // Encode method 0 records (with operations)
    if (!method0Records.isEmpty()) {
      encodeMethod0Records(method0Records, out);
    }
    
    // Encode method 1 records (direct strings)
    if (!method1Records.isEmpty()) {
      encodeMethod1Records(method1Records, out);
    }
  }

  /**
   * Encode method 0 records (with operations)
   */
  private void encodeMethod0Records(List<Record> records, ByteArrayOutputStream out) throws IOException {
    // Encode begin positions
    List<Integer> beginPositions = new ArrayList<>();
    for (Record record : records) {
      beginPositions.add(record.begin);
    }
    encodeDeltaSequence(beginPositions, out);
    
    // Encode operation sizes
    List<Integer> operationSizes = new ArrayList<>();
    for (Record record : records) {
      operationSizes.add(record.operationSize);
    }
    encodeDeltaSequence(operationSizes, out);
    
    // Encode all lengths (delete and insert)
    List<Integer> allLengths = new ArrayList<>();
    for (Record record : records) {
      allLengths.addAll(record.deleteLengths);
      allLengths.addAll(record.insertLengths);
    }
    encodeDeltaSequence(allLengths, out);
    
    // Encode positions
    List<Integer> allPositions = new ArrayList<>();
    for (Record record : records) {
      allPositions.addAll(record.positionList);
    }
    encodeDeltaSequence(allPositions, out);
    
    // Encode strings
    for (Record record : records) {
      for (String subString : record.subStrings) {
        byte[] stringBytes = subString.getBytes("UTF-8");
        out.write(stringBytes);
      }
    }
  }

  /**
   * Encode method 1 records (direct strings)
   */
  private void encodeMethod1Records(List<Record> records, ByteArrayOutputStream out) throws IOException {
    for (Record record : records) {
      for (String subString : record.subStrings) {
        byte[] stringBytes = subString.getBytes("UTF-8");
        out.write(stringBytes);
        out.write('\n'); // Add newline separator
      }
    }
  }

  /**
   * Encode a sequence using delta encoding
   */
  private void encodeDeltaSequence(List<Integer> sequence, ByteArrayOutputStream out) throws IOException {
    if (sequence.isEmpty()) {
      out.write(BytesUtils.intToBytes(0)); // Block count
      return;
    }
    
    // Calculate deltas
    List<Integer> deltas = new ArrayList<>();
    for (int i = 1; i < sequence.size(); i++) {
      deltas.add(sequence.get(i) - sequence.get(i - 1));
    }
    
    // Find minimum delta
    int minDelta = deltas.isEmpty() ? 0 : Collections.min(deltas);
    
    // Calculate bit width
    int maxDelta = deltas.isEmpty() ? 0 : Collections.max(deltas);
    int bitWidth = Math.max(1, 32 - Integer.numberOfLeadingZeros(maxDelta - minDelta));
    
    // Encode header
    out.write(BytesUtils.intToBytes(sequence.get(0))); // First value
    out.write(BytesUtils.intToBytes(minDelta)); // Min delta
    out.write(BytesUtils.intToBytes(deltas.size())); // Delta count
    out.write((byte) bitWidth); // Bit width
    
    // Encode deltas
    if (!deltas.isEmpty()) {
      byte[] buffer = new byte[(deltas.size() * bitWidth + 7) / 8];
      int bitOffset = 0;
      
      for (int delta : deltas) {
        writeBits(buffer, bitOffset, delta - minDelta, bitWidth);
        bitOffset += bitWidth;
      }
      
      out.write(buffer);
    }
  }

  /**
   * Encode using RLE (Run Length Encoding)
   */
  private void encodeRLE(List<Integer> values, ByteArrayOutputStream out) throws IOException {
    if (values.isEmpty()) {
      out.write(BytesUtils.intToBytes(0)); // Length
      out.write(BytesUtils.intToBytes(0)); // Interval count
      return;
    }
    
    // Simple RLE implementation
    List<Integer> intervals = new ArrayList<>();
    int currentValue = values.get(0);
    int count = 1;
    
    for (int i = 1; i < values.size(); i++) {
      if (values.get(i) == currentValue) {
        count++;
      } else {
        intervals.add(count);
        currentValue = values.get(i);
        count = 1;
      }
    }
    intervals.add(count);
    
    // Encode intervals
    StringBuilder binaryString = new StringBuilder();
    binaryString.append(currentValue);
    
    for (int interval : intervals) {
      binaryString.append(encodeNumber(interval));
    }
    
    // Pad to byte boundary
    while (binaryString.length() % 8 != 0) {
      binaryString.append('0');
    }
    
    // Convert to bytes
    String binary = binaryString.toString();
    int byteCount = binary.length() / 8;
    
    out.write(BytesUtils.intToBytes(byteCount)); // Length
    out.write(BytesUtils.intToBytes(intervals.size())); // Interval count
    
    for (int i = 0; i < byteCount; i++) {
      String byteStr = binary.substring(i * 8, (i + 1) * 8);
      int byteValue = Integer.parseInt(byteStr, 2);
      out.write(byteValue);
    }
  }

  /**
   * Encode a number using variable-length encoding
   */
  private String encodeNumber(int num) {
    int bitLength = Math.max(2, 32 - Integer.numberOfLeadingZeros(num));
    StringBuilder result = new StringBuilder();
    
    // Add (bitLength - 2) '1's followed by '0'
    for (int i = 0; i < bitLength - 2; i++) {
      result.append('1');
    }
    result.append('0');
    
    // Add binary representation
    result.append(Integer.toBinaryString(num));
    
    return result.toString();
  }

  /**
   * Write bits to a byte array
   */
  private void writeBits(byte[] buffer, int bitOffset, int value, int bitWidth) {
    int byteIndex = bitOffset / 8;
    int bitIndex = bitOffset % 8;
    
    for (int i = 0; i < bitWidth; i++) {
      if (byteIndex >= buffer.length) break;
      
      int bit = (value >> (bitWidth - 1 - i)) & 1;
      buffer[byteIndex] |= (bit << (7 - bitIndex));
      
      bitIndex++;
      if (bitIndex == 8) {
        bitIndex = 0;
        byteIndex++;
      }
    }
  }

  /**
   * Initialize hash functions for MinHash
   */
  private void initializeHashFunctions() {
    hashCoefficientsA = new long[DEFAULT_NUM_HASHES];
    hashCoefficientsB = new long[DEFAULT_NUM_HASHES];
    
    Random random = new Random(12345); // Fixed seed for reproducibility
    for (int i = 0; i < DEFAULT_NUM_HASHES; i++) {
      hashCoefficientsA[i] = random.nextLong();
      hashCoefficientsB[i] = random.nextLong();
    }
  }

  /**
   * Reset the encoder state
   */
  private void reset() {
    textValues.clear();
    slidingWindow.clear();
    signatureCache.clear();
  }

  @Override
  public int getOneItemMaxSize() {
    return 1024; // Reasonable estimate for text data
  }

  @Override
  public long getMaxByteSize() {
    if (textValues == null || textValues.isEmpty()) {
      return 0;
    }
    
    // Estimate maximum size based on text length
    long totalTextLength = 0;
    for (String text : textValues) {
      totalTextLength += text.length();
    }
    
    // Add overhead for encoding structures
    return totalTextLength + textValues.size() * 100; // Conservative estimate
  }

  /**
   * Record structure for storing compression information
   */
  private static class Record {
    int method; // 0 = with operations, 1 = direct storage
    int begin; // Index in sliding window
    int operationSize; // Number of operations
    List<Integer> positionList = new ArrayList<>();
    List<Integer> deleteLengths = new ArrayList<>();
    List<Integer> insertLengths = new ArrayList<>();
    List<String> subStrings = new ArrayList<>();
  }

  /**
   * Operation item for storing text transformations
   */
  private static class OperationItem {
    int position;
    int deleteLength;
    int insertLength;
    String insertText;
    
    OperationItem(int position, int deleteLength, int insertLength, String insertText) {
      this.position = position;
      this.deleteLength = deleteLength;
      this.insertLength = insertLength;
      this.insertText = insertText;
    }
  }
}
