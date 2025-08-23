package org.apache.tsfile.encoding.decoder;

import org.apache.tsfile.encoding.encoder.AClusterEncoder;
import org.apache.tsfile.encoding.encoder.Encoder;
import org.apache.tsfile.enums.TSDataType;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

/**
 * Test suite for AClusterEncoder and AClusterDecoder.
 * <p>
 * This test validates the end-to-end encoding and decoding process.
 * Since ACluster algorithm reorders data based on clusters, the validation
 * cannot compare original and decoded lists element by element. Instead, it
 * verifies that the set of unique values and their frequencies are identical
 * before and after the process.
 * </p>
 * <p>
 * The test structure is adapted to the iterator-style Decoder interface
 * (hasNext(buffer), readXXX(buffer)).
 * </p>
 */
public class AClusterEncoderDecoderTest {

    private static final int ROW_NUM = 1000;
    private final Random ran = new Random();

    // =================================================================================
    // Integer Tests
    // =================================================================================

    @Test
    public void testIntBasicClusters() throws IOException {
        List<Integer> data = new ArrayList<>();
        // Three distinct clusters
        for (int i = 0; i < 300; i++) data.add(100 + ran.nextInt(10)); // Cluster around 100
        for (int i = 0; i < 400; i++) data.add(5000 + ran.nextInt(20)); // Cluster around 5000
        for (int i = 0; i < 300; i++) data.add(100000 + ran.nextInt(5)); // Cluster around 100000
        shouldReadAndWrite(data, TSDataType.INT32);
    }


    // =================================================================================
    // Long Tests
    // =================================================================================

    @Test
    public void testLongBasic() throws IOException {
        List<Long> data = new ArrayList<>();
        for (int i = 0; i < ROW_NUM; i++) {
            data.add((long) i * i * i);
        }
        shouldReadAndWrite(data, TSDataType.INT64);
    }

    // =================================================================================
    // Double Tests
    // =================================================================================

    @Test
    public void testDoubleWithPrecision() throws IOException {
        List<Double> data = new ArrayList<>();
        final int precision = 6;

        System.out.println("Testing double with controlled precision (max " + precision + " decimal places)...");

        for (int i = 0; i < ROW_NUM / 2; i++) {
            double randomPart = nextRandomDoubleWithPrecision(ran, precision);
            double rawValue = 123.456 + randomPart;

            double cleanValue = cleanDouble(rawValue, precision + 3);
            data.add(cleanValue);
        }

        for (int i = 0; i < ROW_NUM / 2; i++) {
            double randomPart = nextRandomDoubleWithPrecision(ran, precision);
            double rawValue = 9999.0 + randomPart;

            double cleanValue = cleanDouble(rawValue, precision);
            data.add(cleanValue);
        }

        if (!data.isEmpty()) {
            System.out.println("Sample generated data point (after cleaning): " + data.get(0));
        }

        shouldReadAndWrite(data, TSDataType.DOUBLE);
    }

    private double cleanDouble(double value, int maxPrecision) {
        BigDecimal bd = new BigDecimal(value);
        BigDecimal roundedBd = bd.setScale(maxPrecision, RoundingMode.HALF_UP);
        return roundedBd.doubleValue();
    }
    // =================================================================================
    // Edge Case Tests
    // =================================================================================

    @Test
    public void testSingleValue() throws IOException {
        shouldReadAndWrite(Arrays.asList(123.00000001,123.00000002, 29.0001,29.0002,29.000001), TSDataType.DOUBLE);
    }

    @Test
    public void testAllSameValues() throws IOException {
        List<Integer> data = new ArrayList<>();
        for(int i = 0; i < 100; i++) data.add(777);
        shouldReadAndWrite(data, TSDataType.INT32);
    }

    // =================================================================================
    // Core Test Logic and Helpers
    // =================================================================================

    private double nextRandomDoubleWithPrecision(Random random, int precision) {
        if (precision < 0) {
            throw new IllegalArgumentException("Precision must be non-negative.");
        }
        double factor = Math.pow(10, precision);

        double scaled = random.nextDouble() * factor;
        long rounded = Math.round(scaled);
        return rounded / factor;
    }

    /**
     * Generic helper to write a list of data using the appropriate encoder method.
     */
    private <T extends Number> void writeData(List<T> data, Encoder encoder, ByteArrayOutputStream out) throws IOException {
        if (data.isEmpty()) {
            return;
        }
        // Use instanceof to call the correct overloaded encode method
        if (data.get(0) instanceof Integer) {
            data.forEach(val -> encoder.encode((Integer) val, out));
        } else if (data.get(0) instanceof Long) {
            data.forEach(val -> encoder.encode((Long) val, out));
        } else if (data.get(0) instanceof Float) {
            data.forEach(val -> encoder.encode((Float) val, out));
        } else if (data.get(0) instanceof Double) {
            data.forEach(val -> encoder.encode((Double) val, out));
        }
        encoder.flush(out);
    }

    /**
     * The main validation method. It encodes the given data, then decodes it,
     * and finally compares the frequency maps of the original and decoded data.
     */
    private <T extends Number> void shouldReadAndWrite(List<T> originalData, TSDataType dataType) throws IOException {
        // 1. Prepare for encoding
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Encoder encoder = new AClusterEncoder(dataType);

        // 2. Encode the data
        writeData(originalData, encoder, out);
        ByteBuffer buffer = ByteBuffer.wrap(out.toByteArray());

        // 3. Decode the data using the iterator-style interface
        Decoder decoder = new ClusterDecoder(dataType);
        List<T> decodedData = new ArrayList<>();

        while (decoder.hasNext(buffer)) {
            switch (dataType) {
                case INT32:
                    decodedData.add((T) Integer.valueOf(decoder.readInt(buffer)));
                    break;
                case INT64:
                    decodedData.add((T) Long.valueOf(decoder.readLong(buffer)));
                    break;
                case FLOAT:
                    decodedData.add((T) Float.valueOf(decoder.readFloat(buffer)));
                    break;
                case DOUBLE:
                    decodedData.add((T) Double.valueOf(decoder.readDouble(buffer)));
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported data type for test");
            }
        }

        // 4. Validate the results
        // First, a quick check on the total count
        assertEquals("Decoded data size should match original data size", originalData.size(), decodedData.size());

        // Second, the robust check using frequency maps
        Map<T, Long> originalFrequencies = getFrequencyMap(originalData);
        Map<T, Long> decodedFrequencies = getFrequencyMap(decodedData);

        assertEquals("Frequency maps of original and decoded data should be identical", originalFrequencies, decodedFrequencies);
    }

    /**
     * Helper method to count frequencies of elements in a list.
     */
    private <T extends Number> Map<T, Long> getFrequencyMap(List<T> list) {
        return list.stream()
                .collect(Collectors.groupingBy(Function.identity(), Collectors.counting()));
    }
}