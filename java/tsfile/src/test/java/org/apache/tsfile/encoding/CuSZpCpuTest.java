package org.apache.tsfile.encoding; // 根据你项目包名调整

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import org.junit.Assume;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.zip.DataFormatException;

/**
 * Simplified CPU reimplementation of a cuSZp-like plain 1D compressor for testing.
 *
 * Workflow:
 *  - read a 1D CSV of numbers (double)
 *  - for each CHUNK, compute delta predictor (prev value)
 *  - quantize with absolute error bound eb: q = round((v - pred) / eb)
 *  - zigzag-encode signed q -> unsigned
 *  - group into packs (packSize) and choose minimal bitwidth per pack
 *  - bit-pack into byte[] (store per-block header: packSize, numPacks, bitwidths...)
 *  - decode to verify correctness and measure times
 *
 * This is a practical testing harness, not a byte-for-byte match to cuSZp GPU impl.
 */
public class CuSZpCpuTest {

    private static final Set<String> IGNORE_FILES = Collections.emptySet();
    private static final int CHUNK_SIZE = 8192; // per-iteration chunk size; tune as needed

    @Test
    public void cuSZpCpu1DTest() throws Exception {
        System.out.println("\nCPU cuSZp-like plain-mode Performance Testing...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-pack-size/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-pack-size/output_cuszp_cpu";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        Assume.assumeTrue("Skip test: dataset directory missing: " + directory, dir.exists() && dir.isDirectory());

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (IGNORE_FILES.contains(file.getName()) || file.isDirectory()) continue;
            System.out.println("Processing " + file.getName());

            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size (bits)",
                    "Compression Ratio"
            };
            writer.writeRecord(head);

            // read numbers from CSV into a single double[] (assumes whitespace/commas)
            List<Double> numbers = readCsvToDoubleList(file);

            if (numbers.isEmpty()) {
                System.out.println("Empty file: " + file.getName());
                writer.close();
                continue;
            }

            // parameters
            double eb = 1e-4; // absolute error bound, 可让用户传参 / 读取
            int packSize = 8; // how many values per pack when computing bitwidth
            int repeats = 20; // average times

            long totalEncodeNs = 0;
            long totalDecodeNs = 0;
            long totalCompressedBits = 0;

            for (int r = 0; r < repeats; r++) {
                int index = 0;
                while (index < numbers.size()) {
                    int end = Math.min(index + CHUNK_SIZE, numbers.size());
                    double[] chunk = new double[end - index];
                    for (int i = 0; i < chunk.length; i++) chunk[i] = numbers.get(index + i);

                    long sEnc = System.nanoTime();
                    byte[] cmp = encodePlain1D(chunk, eb, packSize);
                    long eEnc = System.nanoTime();

                    long sDec = System.nanoTime();
                    double[] dec = decodePlain1D(cmp, chunk.length, eb, packSize);
                    long eDec = System.nanoTime();

                    // // optional correctness check (can comment out for speed)
                    // for (int i = 0; i < chunk.length; i++) {
                    //     double err = Math.abs(chunk[i] - dec[i]);
                    //     if (err > eb + 1e-12) {
                    //         throw new AssertionError("Reconstruction error " + err + " > eb for index " + (index + i));
                    //     }
                    // }

                    totalEncodeNs += (eEnc - sEnc);
                    totalDecodeNs += (eDec - sDec);
                    totalCompressedBits += (long) cmp.length * 8L;

                    index = end;
                }
            }

            double avgEncodeNs = (double) totalEncodeNs / repeats;
            double avgDecodeNs = (double) totalDecodeNs / repeats;
            double points = numbers.size();
            long avgCompressedBits = totalCompressedBits / repeats;
            double ratio = avgCompressedBits / (points * 64.0); // compressed bits / raw bits (64 per double)
            // throughput MB/s: points * 8 bytes / time (ns) => (points*8)/avgNs * 1e9 bytes/sec -> /1e6 => MB/s
            double encodeThroughput = (points * 8.0) / (avgEncodeNs) * 1e3; // MB/s (since ns -> sec & bytes->MB)
            double decodeThroughput = (points * 8.0) / (avgDecodeNs) * 1e3;

            String[] record = {
                    file.toString(),
                    "cuSZp-cpu-plain-simplified",
                    String.valueOf(encodeThroughput),
                    String.valueOf(decodeThroughput),
                    String.valueOf((long) points),
                    String.valueOf(avgCompressedBits),
                    String.valueOf(ratio)
            };
            writer.writeRecord(record);
            writer.close();

            System.out.printf("File=%s  encode_throughput=%.2f MB/s decode_throughput=%.2f MB/s ratio=%.4f%n",
                    file.getName(), encodeThroughput, decodeThroughput, ratio);
        }
    }

    @Test
    public void cuSZpCpu1DOptimalV5Test() throws Exception {
        System.out.println("\nCPU cuSZp-like plain-mode (optimal pack V5 per chunk)...");
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-pack-size/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-pack-size/output_cuszp_cpu_optimal_v5";
        File outputDir = new File(outputDirstr);

        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        Assume.assumeTrue("Skip test: dataset directory missing: " + directory, dir.exists() && dir.isDirectory());

        for (File file : Objects.requireNonNull(dir.listFiles())) {
            if (IGNORE_FILES.contains(file.getName()) || file.isDirectory()) continue;
            System.out.println("Processing (V5 pack) " + file.getName());

            String Output = outputDirstr + "/" + file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Decoding Time",
                    "Points",
                    "Compressed Size (bits)",
                    "Compression Ratio"
            };
            writer.writeRecord(head);

            List<Double> numbers = readCsvToDoubleList(file);

            if (numbers.isEmpty()) {
                System.out.println("Empty file: " + file.getName());
                writer.close();
                continue;
            }

            double eb = 1e-4;
            int repeats = 20;

            long totalEncodeNs = 0;
            long totalDecodeNs = 0;
            long totalCompressedBits = 0;

            for (int r = 0; r < repeats; r++) {
                int index = 0;
                while (index < numbers.size()) {
                    int end = Math.min(index + CHUNK_SIZE, numbers.size());
                    double[] chunk = new double[end - index];
                    for (int i = 0; i < chunk.length; i++) chunk[i] = numbers.get(index + i);

                    long sEnc = System.nanoTime();
                    byte[] cmp = encodePlain1DOptimalPackV5(chunk, eb);
                    long eEnc = System.nanoTime();

                    long sDec = System.nanoTime();
                    double[] dec = decodePlain1D(cmp, chunk.length, eb, -1);
                    long eDec = System.nanoTime();

                    totalEncodeNs += (eEnc - sEnc);
                    totalDecodeNs += (eDec - sDec);
                    totalCompressedBits += (long) cmp.length * 8L;

                    index = end;
                }
            }

            double avgEncodeNs = (double) totalEncodeNs / repeats;
            double avgDecodeNs = (double) totalDecodeNs / repeats;
            double points = numbers.size();
            long avgCompressedBits = totalCompressedBits / repeats;
            double ratio = avgCompressedBits / (points * 64.0);
            double encodeThroughput = (points * 8.0) / (avgEncodeNs) * 1e3;
            double decodeThroughput = (points * 8.0) / (avgDecodeNs) * 1e3;

            String[] record = {
                    file.toString(),
                    "cuSZp-cpu+V5pack",
                    String.valueOf(encodeThroughput),
                    String.valueOf(decodeThroughput),
                    String.valueOf((long) points),
                    String.valueOf(avgCompressedBits),
                    String.valueOf(ratio)
            };
            writer.writeRecord(record);
            writer.close();

            System.out.printf("File=%s  encode_throughput=%.2f MB/s decode_throughput=%.2f MB/s ratio=%.4f%n",
                    file.getName(), encodeThroughput, decodeThroughput, ratio);
        }
    }

    /** Brute-force argmin_p of the same surrogate as {@link AllNo8PacksizeOptimal#findOptimalPackSizeCuSZpMeta8Bits(long[])}. */
    static int bruteOptimalPackCuSZpMeta8Bits(long[] u, int n) {
        if (n < 8) {
            return n;
        }
        int[] bitWidths = new int[n];
        for (int i = 0; i < n; i++) {
            bitWidths[i] = 64 - Long.numberOfLeadingZeros(Math.max(1L, u[i]));
        }
        long bestCost = Long.MAX_VALUE;
        int bestP = 1;
        for (int p = 1; p <= n; p++) {
            int m = (n + p - 1) / p;
            long c = 8L * m;
            for (int i = 0; i < m - 1; i++) {
                int start = i * p;
                int end = start + p - 1;
                int mb = 0;
                for (int j = start; j <= end; j++) {
                    mb = Math.max(mb, bitWidths[j]);
                }
                c += (long) p * mb;
            }
            if (m > 0) {
                int lastStart = (m - 1) * p;
                int mb = 0;
                for (int j = lastStart; j < n; j++) {
                    mb = Math.max(mb, bitWidths[j]);
                }
                c += (long) (n - lastStart) * mb;
            }
            if (c < bestCost) {
                bestCost = c;
                bestP = p;
            }
        }
        return bestP;
    }

    @Test
    public void testCuSZpMeta8BitsPrunedMatchesBrute() {
        Random rnd = new Random(7);
        for (int t = 0; t < 80; t++) {
            int n = 8 + rnd.nextInt(120);
            long[] u = new long[n];
            for (int i = 0; i < n; i++) {
                u[i] = rnd.nextInt(2000000);
            }
            int pruned = AllNo8PacksizeOptimal.findOptimalPackSizeCuSZpMeta8Bits(u);
            int brute = bruteOptimalPackCuSZpMeta8Bits(u, n);
            assertEquals("n=" + n + " trial=" + t, brute, pruned);
        }
    }

    @Test
    public void testEncodedByteLengthPlain1DLayoutMatchesEncode() throws Exception {
        Random rnd = new Random(42);
        double eb = 1e-3;
        for (int trial = 0; trial < 30; trial++) {
            int n = 1 + rnd.nextInt(180);
            double[] v = new double[n];
            for (int i = 0; i < n; i++) {
                v[i] = rnd.nextGaussian() * 10.0;
            }
            long[] q = new long[n];
            for (int i = 0; i < n; i++) {
                double pred = (i == 0) ? 0.0 : v[i - 1];
                q[i] = Math.round((v[i] - pred) / eb);
            }
            long[] u = new long[n];
            for (int i = 0; i < n; i++) {
                u[i] = zigzagEncode(q[i]);
            }
            for (int p = 1; p <= n; p++) {
                long calc = encodedByteLengthPlain1DLayout(u, n, p);
                byte[] enc = encodePlain1D(v, eb, p);
                assertEquals("n=" + n + " p=" + p, enc.length, calc);
            }
        }
    }

    // ---------- Encoding / Decoding primitives (simplified plain-mode) ----------

    /**
     * Output size in bytes for {@link #encodePlain1D(double[], double, int)} given precomputed zigzag-unsigned
     * {@code u} (same definition as inside {@code encodePlain1D}) and {@code packSize}. Matches the actual layout:
     * {@code 4+4+4} byte header, {@code numPacks} width bytes, then bit payload with {@link BitWriter} rules and
     * {@code flush()} (partial final byte padded to a full byte).
     */
    static long encodedByteLengthPlain1DLayout(long[] u, int n, int packSize) {
        if (n <= 0) {
            return 12L;
        }
        int numPacks = (n + packSize - 1) / packSize;
        long dataBits = 0;
        for (int pk = 0; pk < numPacks; pk++) {
            int start = pk * packSize;
            int end = Math.min(n, start + packSize);
            long maxv = 0;
            for (int i = start; i < end; i++) {
                if (u[i] > maxv) {
                    maxv = u[i];
                }
            }
            int bw = (maxv == 0) ? 0 : (64 - Long.numberOfLeadingZeros(maxv));
            dataBits += (long) (end - start) * bw;
        }
        long payloadBytes = (dataBits + 7) / 8;
        return 12L + numPacks + payloadBytes;
    }

    /**
     * Pack size {@code p} in {@code 1..n} that minimizes {@link #encodedByteLengthPlain1DLayout(long[], int, int)}
     * for zigzag-unsigned array {@code u} (length {@code n}). Ties favor smaller {@code p}.
     */
    public static int findOptimalPackSizePlain1DExact(long[] u, int n) {
        if (n <= 0) {
            return 8;
        }
        long bestBytes = Long.MAX_VALUE;
        int bestP = 1;
        for (int p = 1; p <= n; p++) {
            long len = encodedByteLengthPlain1DLayout(u, n, p);
            if (len < bestBytes || (len == bestBytes && p < bestP)) {
                bestBytes = len;
                bestP = p;
            }
        }
        return bestP;
    }

    /**
     * Same as {@link #encodePlain1D(double[], double, int)} but chooses {@code packSize} with the same RMQ + prune
     * strategy as {@link AllNo8PacksizeOptimal#findOptimalPackSizeallV5(int[])}, while metadata cost uses {@code 8m}
     * bits (one byte per pack) instead of {@code m*z}: {@link AllNo8PacksizeOptimal#findOptimalPackSizeCuSZpMeta8Bits(long[])}.
     * <p>
     * That objective matches V5's linear data-bit model + 8 bits per pack; it is not identical to minimizing
     * {@link #encodedByteLengthPlain1DLayout(long[], int, int)} (byte alignment / flush). Use
     * {@link #findOptimalPackSizePlain1DExact(long[], int)} for exact byte length.
     */
    public static byte[] encodePlain1DOptimalPackV5(double[] values, double eb) throws IOException {
        long[] q = new long[values.length];
        for (int i = 0; i < values.length; i++) {
            double pred = (i == 0) ? 0.0 : values[i - 1];
            q[i] = Math.round((values[i] - pred) / eb);
        }
        long[] u = new long[q.length];
        for (int i = 0; i < q.length; i++) {
            u[i] = zigzagEncode(q[i]);
        }
        int packSize = AllNo8PacksizeOptimal.findOptimalPackSizeCuSZpMeta8Bits(u);
        return encodePlain1D(values, eb, packSize);
    }

    /**
     * Encode double[] chunk using simple delta predictor + quantization (abs error eb) + pack-size bitpacking.
     * Block format (simplified):
     * [int32:packSize][int32:len][int32:numPacks][for each pack: int8 bitWidth][data bytes...]
     */
    public static byte[] encodePlain1D(double[] values, double eb, int packSize) throws IOException {
        // quantize with delta predictor
        long[] q = new long[values.length];
        double prev = 0.0;
        for (int i = 0; i < values.length; i++) {
            double pred = (i == 0) ? 0.0 : values[i - 1]; // 1D previous-value predictor
            double diff = values[i] - pred;
            long qi = Math.round(diff / eb); // quantized integer (signed)
            q[i] = qi;
        }
        // zigzag encode to unsigned
        long[] u = new long[q.length];
        for (int i = 0; i < q.length; i++) u[i] = zigzagEncode(q[i]);

        // compute per-pack bitwidths
        int numPacks = (q.length + packSize - 1) / packSize;
        int[] bitWidths = new int[numPacks];
        for (int p = 0; p < numPacks; p++) {
            int start = p * packSize;
            int end = Math.min(q.length, start + packSize);
            long maxv = 0;
            for (int i = start; i < end; i++) if (u[i] > maxv) maxv = u[i];
            int bw = (maxv == 0) ? 0 : (64 - Long.numberOfLeadingZeros(maxv));
            bitWidths[p] = bw;
        }

        // bit pack into ByteArrayOutputStream with a simple header
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(baos);

        dos.writeInt(packSize);
        dos.writeInt(values.length);
        dos.writeInt(numPacks);
        // write bitWidths as bytes
        for (int bw : bitWidths) dos.writeByte(bw);

        // now write bitstream per pack
        BitWriter bwriter = new BitWriter(baos);
        for (int p = 0; p < numPacks; p++) {
            int start = p * packSize;
            int end = Math.min(q.length, start + packSize);
            int bwid = bitWidths[p];
            for (int i = start; i < end; i++) {
                if (bwid > 0) bwriter.writeBits(u[i], bwid);
                // if bwid==0, value is zero and nothing is written for this item
            }
            // pad pack to align pack boundary? we simply continue (no per-pack padding)
        }
        bwriter.flush();
        dos.flush();
        return baos.toByteArray();
    }

    /**
     * Decode according to the simple format above.
     */
    public static double[] decodePlain1D(byte[] cmp, int originalLen, double eb, int expectedPackSize) throws IOException, DataFormatException {
        ByteArrayInputStream bais = new ByteArrayInputStream(cmp);
        DataInputStream dis = new DataInputStream(bais);
        int packSize = dis.readInt();
        int len = dis.readInt();
        int numPacks = dis.readInt();
        int[] bitWidths = new int[numPacks];
        for (int i = 0; i < numPacks; i++) bitWidths[i] = dis.readByte() & 0xFF;

        // build a BitReader starting from current pos
        int headerBytes = 4 + 4 + 4 + numPacks; // packSize + len + numPacks + bitwidths
        byte[] bitStream = Arrays.copyOfRange(cmp, headerBytes, cmp.length);
        BitReader breader = new BitReader(bitStream);

        long[] u = new long[len];
        for (int p = 0; p < numPacks; p++) {
            int start = p * packSize;
            int end = Math.min(len, start + packSize);
            int bw = bitWidths[p];
            for (int i = start; i < end; i++) {
                long val = (bw == 0) ? 0L : breader.readBits(bw);
                u[i] = val;
            }
        }
        // zigzag decode and re-apply predictor
        double[] out = new double[len];
        long[] q = new long[len];
        for (int i = 0; i < len; i++) q[i] = zigzagDecode(u[i]);
        for (int i = 0; i < len; i++) {
            double pred = (i == 0) ? 0.0 : out[i - 1];
            out[i] = pred + q[i] * eb;
        }
        return out;
    }

    // ---------- helpers ----------

    private static List<Double> readCsvToDoubleList(File f) throws IOException {
        List<Double> list = new ArrayList<>();
        CsvReader r = new CsvReader(f.getPath(), ',', StandardCharsets.UTF_8);
        while (r.readRecord()) {
            for (String v : r.getValues()) {
                String s = v.trim();
                if (!s.isEmpty()) {
                    try {
                        list.add(Double.parseDouble(s));
                    } catch (NumberFormatException ex) {
                        // skip non-number cells
                    }
                }
            }
        }
        return list;
    }

    // ZigZag: map signed -> unsigned (so small negatives become small unsigned)
    private static long zigzagEncode(long x) {
        return (x << 1) ^ (x >> 63);
    }

    private static long zigzagDecode(long u) {
        return (u >>> 1) ^ -(u & 1);
    }

    // Simple bit writer (append bits, LSB-first within value)
    static class BitWriter {
        private final ByteArrayOutputStream baos;
        private int bitPos = 0; // next bit to write into current byte (0..7)
        private int currentByte = 0;

        BitWriter(ByteArrayOutputStream baos) {
            this.baos = baos;
        }

        // write the low 'bits' bits of value (value treated as unsigned)
        void writeBits(long value, int bits) throws IOException {
            for (int i = bits - 1; i >= 0; i--) {
                int bit = (int) ((value >> i) & 1L);
                currentByte = (currentByte << 1) | bit;
                bitPos++;
                if (bitPos == 8) {
                    baos.write((byte) currentByte);
                    bitPos = 0;
                    currentByte = 0;
                }
            }
        }

        void flush() throws IOException {
            if (bitPos > 0) {
                // pad the remaining bits in the last byte (left-shift to MSB)
                int shiftLeft = 8 - bitPos;
                currentByte = currentByte << shiftLeft;
                baos.write((byte) currentByte);
                bitPos = 0;
                currentByte = 0;
            }
        }
    }

    // Simple bit reader (reads bits produced by BitWriter)
    static class BitReader {
        private final byte[] data;
        private int byteIdx = 0;
        private int bitIdx = 0; // next bit to read inside current byte (0..7, MSB-first as we wrote)

        BitReader(byte[] data) {
            this.data = data;
            this.byteIdx = 0;
            this.bitIdx = 0;
        }

        // read 'bits' bits and return as long (0 if bits==0)
        long readBits(int bits) throws DataFormatException {
            if (bits == 0) return 0L;
            long out = 0L;
            for (int i = 0; i < bits; i++) {
                if (byteIdx >= data.length) throw new DataFormatException("BitReader overflow");
                int b = data[byteIdx] & 0xFF;
                int shift = 7 - bitIdx;
                int bit = (b >> shift) & 1;
                out = (out << 1) | bit;
                bitIdx++;
                if (bitIdx == 8) {
                    bitIdx = 0;
                    byteIdx++;
                }
            }
            return out;
        }
    }
}