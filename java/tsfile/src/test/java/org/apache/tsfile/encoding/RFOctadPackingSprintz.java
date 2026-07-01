package org.apache.iotdb.tsfile.encoding;

import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RFOctadPackingSprintz {

    private static final int CHUNK_SIZE = 1000;

    public static List<int[]> loadDataFromCSV(String filename) {
        List<int[]> sequences = new ArrayList<>();
        // 支持负数、任意空白、多个数字用逗号分隔；外部可能被可选引号包裹
        Pattern pattern = Pattern.compile("\"?\\[\\s*([-]?\\d+(?:\\s*,\\s*[-]?\\d+)*)\\s*\\]\"?");

        try (BufferedReader br = Files.newBufferedReader(Paths.get(filename), StandardCharsets.UTF_8)) {
            String line;
            int lineNo = 0;
            boolean firstLine = true;
            while ((line = br.readLine()) != null) {
                lineNo++;
                if (firstLine) { // 跳过 header（与原始代码行为一致）
                    firstLine = false;
                    continue;
                }
                if (line.trim().isEmpty()) continue; // 跳过空行

                Matcher m = pattern.matcher(line);
                if (m.find()) {
                    String data = m.group(1); // 捕获括号内的 "1, 2, -3" 部分
                    String[] tokens = data.split(",");
                    int[] arr = new int[tokens.length];
                    int idx = 0;
                    for (String t : tokens) {
                        String s = t.trim();
                        if (s.isEmpty()) continue;
                        try {
                            arr[idx++] = Integer.parseInt(s);
                        } catch (NumberFormatException ex) {
                            System.err.printf("Warning: invalid integer '%s' at %s:%d — skipping that token%n",
                                    s, filename, lineNo);
                        }
                    }
                    if (idx > 0) {
                        if (idx < arr.length) arr = Arrays.copyOf(arr, idx);
                        sequences.add(arr);
                    } else {
                        // 如果整行没有合法整数，则忽略（可根据需要改为添加空数组）
                        System.err.printf("Info: no valid integers parsed from brackets at %s:%d%n", filename, lineNo);
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Error opening/reading CSV: " + filename + " -> " + e.getMessage());
        }

        return sequences;
    }

    static String trimStr(String s) {
        if (s == null) return "";
        int a = 0;
        while (a < s.length() && Character.isWhitespace(s.charAt(a))) a++;
        if (a == s.length()) return "";
        int b = s.length() - 1;
        while (b >= 0 && Character.isWhitespace(s.charAt(b))) b--;
        return s.substring(a, b + 1);
    }

    static String stripEnclosingQuotes(String s) {
        if (s == null) return "";
        if (s.length() >= 2) {
            char f = s.charAt(0);
            char l = s.charAt(s.length() - 1);
            if ((f == '"' && l == '"') || (f == '\'' && l == '\'')) {
                return s.substring(1, s.length() - 1);
            }
        }
        return s;
    }

    static long[] scaleNumbers(List<String> numbers, int decimalMax) {
        int n = numbers.size();
        long[] result = new long[n];
        if (n == 0) return result;

        BigDecimal scale = BigDecimal.ONE;
        for (int i = 0; i < decimalMax; ++i) scale = scale.multiply(BigDecimal.TEN);

        BigDecimal[] vals = new BigDecimal[n];
        for (int i = 0; i < n; ++i) {
            String s = trimStr(numbers.get(i));
            s = stripEnclosingQuotes(s);
            if (s.isEmpty()) { vals[i] = BigDecimal.ZERO; continue; }
            s = s.replace(",", ""); // remove thousands sep

            // If scientific notation present, BigDecimal can parse it
            try {
                BigDecimal bd = new BigDecimal(s);
                BigDecimal scaled = bd.multiply(scale);
                // rounding to nearest whole
                BigDecimal rounded = scaled.setScale(0, RoundingMode.HALF_UP);
                vals[i] = rounded;
            } catch (Exception ex) {
                // fallback: parse double
                try {
                    double dv = Double.parseDouble(s);
                    BigDecimal bd = BigDecimal.valueOf(dv).multiply(scale);
                    vals[i] = bd.setScale(0, RoundingMode.HALF_UP);
                } catch (Exception ex2) {
                    System.err.println("Warning: cannot parse token '" + numbers.get(i) + "', set to 0");
                    vals[i] = BigDecimal.ZERO;
                }
            }
        }

        // find min
        BigDecimal minv = vals[0];
        for (int i = 1; i < n; ++i) if (vals[i].compareTo(minv) < 0) minv = vals[i];

        for (int i = 0; i < n; ++i) {
            BigDecimal shifted = vals[i].subtract(minv);
            // clamp to long range
            try {
                BigInteger bi = shifted.toBigIntegerExact();
                if (bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) result[i] = Long.MAX_VALUE;
                else if (bi.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) result[i] = Long.MIN_VALUE;
                else result[i] = bi.longValue();
            } catch (ArithmeticException ae) {
                // not an integer exactly: fallback by converting to long with rounding
                BigDecimal rounded = shifted.setScale(0, RoundingMode.HALF_UP);
                try {
                    BigInteger bi = rounded.toBigIntegerExact();
                    if (bi.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) result[i] = Long.MAX_VALUE;
                    else if (bi.compareTo(BigInteger.valueOf(Long.MIN_VALUE)) < 0) result[i] = Long.MIN_VALUE;
                    else result[i] = bi.longValue();
                } catch (Exception ex) {
                    result[i] = 0;
                }
            }
        }
        return result;
    }

    public static class Stats {
        public final double diffStd;    // 差分的标准差（总体）
        public final double entropy;    // 香农熵（bits）
        public final double repeatFreq; // 相邻重复的频率（0..1）

        public Stats(double diffStd, double entropy, double repeatFreq) {
            this.diffStd = diffStd;
            this.entropy = entropy;
            this.repeatFreq = repeatFreq;
        }
        public double[] toArray() { return new double[]{diffStd, entropy, repeatFreq}; }

        @Override
        public String toString() {
            return String.format("diffStd=%.6f, entropy=%.6f bits, repeatFreq=%.6f",
                    diffStd, entropy, repeatFreq);
        }
    }

    public static Stats computeStats(int[] bitWidths) {
        if (bitWidths == null || bitWidths.length == 0) {
            return new Stats(0.0, 0.0, 0.0);
        }
        int n = bitWidths.length;

        // 1) 差分标准差（d[i] = a[i] - a[i-1], i=1..n-1），总体标准差（除以 m = n-1）
        double diffStd = 0.0;
        if (n >= 2) {
            int m = n - 1;
            // 先算均值
            double sum = 0.0;
            for (int i = 1; i < n; ++i) {
                sum += (bitWidths[i] - bitWidths[i - 1]);
            }
            double mean = sum / m;
            // 算方差
            double sqSum = 0.0;
            for (int i = 1; i < n; ++i) {
                double d = (bitWidths[i] - bitWidths[i - 1]) - mean;
                sqSum += d * d;
            }
            double variance = sqSum / m; // 总体方差（如果想要样本方差可除以 m-1）
            diffStd = Math.sqrt(variance);
        }

        // 2) 香农熵：- sum(p * log2 p)
        double entropy = 0.0;
        if (n > 0) {
            Map<Integer, Integer> cnt = new HashMap<>();
            for (int v : bitWidths) cnt.put(v, cnt.getOrDefault(v, 0) + 1);
            for (Map.Entry<Integer, Integer> e : cnt.entrySet()) {
                double p = e.getValue() / (double) n;
                // p>0 保证
                entropy -= p * (Math.log(p) / Math.log(2.0));
            }
        }

        // 3) 相邻重复频率：count(i where a[i]==a[i-1]) / (n-1)
        double repeatFreq = 0.0;
        if (n >= 2) {
            int repeats = 0;
            for (int i = 1; i < n; ++i) if (bitWidths[i] == bitWidths[i - 1]) ++repeats;
            repeatFreq = repeats / (double) (n - 1);
        }

        return new Stats(diffStd, entropy, repeatFreq);
    }

    public static int computeMinPackingCost(int[] bitWidths) {
        int blocksize= bitWidths.length;
        int minCost = Integer.MAX_VALUE;

        // Try all possible pack sizes from 1 to CHUNK_SIZE
        for (int p = 1; p <= blocksize; p++) {
            int totalCost = 0;
            int numPacks = (int) Math.ceil((double) blocksize / p);

            // Calculate cost for each pack
            for (int pack = 0; pack < numPacks; pack++) {
                int start = pack * p;
                int end = Math.min(start + p, blocksize);

                // Find max bitWidth in current pack
                int maxBitWidth = 0;
                for (int i = start; i < end; i++) {
                    if (bitWidths[i] > maxBitWidth) {
                        maxBitWidth = bitWidths[i];
                    }
                }

                // Add to cost: 8 * p * maxBitWidth
                totalCost += 8 * p * maxBitWidth;
            }

            // Add the chunk cost: 5 * CHUNK_SIZE / p
            totalCost += 5 * blocksize / p;

            // Update minimum cost
            if (totalCost < minCost) {
                minCost = totalCost;
            }
        }
        return minCost;
    }

    public static int findMinCostPack(int[] bitWidths) {
        int blocksize= bitWidths.length;
        int minCost = Integer.MAX_VALUE;
        int optimalP = 1;

        // Try all possible pack sizes from 1 to CHUNK_SIZE
        for (int p = 1; p <= blocksize; p++) {
            int totalCost = 0;
            int numPacks = (int) Math.ceil((double) blocksize / p);

            // Calculate cost for each pack
            for (int pack = 0; pack < numPacks; pack++) {
                int start = pack * p;
                int end = Math.min(start + p, blocksize);

                // Find max bitWidth in current pack
                int maxBitWidth = 0;
                for (int i = start; i < end; i++) {
                    if (bitWidths[i] > maxBitWidth) {
                        maxBitWidth = bitWidths[i] ;
                    }
                }

                // Add to cost: 8 * p * maxBitWidth
                totalCost += 8 * (end-start) * maxBitWidth;
            }

            // Add the chunk cost: 5 * CHUNK_SIZE / p
            totalCost += 5 * blocksize / p;

            // Update minimum cost
            if (totalCost < minCost) {
                minCost = totalCost;
                optimalP = p;
            }
        }
        return optimalP;
    }

    public static int bitPacking(int[] bitWidths, int p){
        int blocksize = bitWidths.length;

        int totalCost = 0;
        int numPacks = (int) Math.ceil((double) blocksize / p);

        // Calculate cost for each pack
        for (int pack = 0; pack < numPacks; pack++) {
            int start = pack * p;
            int end = Math.min(start + p, blocksize);

            // Find max bitWidth in current pack
            int maxBitWidth = 0;
            for (int i = start; i < end; i++) {
                if (bitWidths[i] > maxBitWidth) {
                    maxBitWidth = bitWidths[i] ;
                }
            }

            // Add to cost: 8 * p * maxBitWidth
            totalCost += 8 * (end - start) * maxBitWidth;
        }

        // Add the chunk cost: 5 * CHUNK_SIZE / p
        totalCost += 5 * blocksize / p;

        return totalCost;

    }
    public static class RandomForest {
        private final List<DecisionTree> trees;
        private final Random rng;

        public RandomForest(List<DecisionTree> trees, long seed) {
            this.trees = trees;
            this.rng = new Random(seed);
        }

        public int predict(double[] features) {
            Map<Integer, Integer> votes = new HashMap<>();
            for (DecisionTree t : trees) {
                int p = t.predict(features);
                votes.put(p, votes.getOrDefault(p, 0) + 1);
            }
            // majority vote; tie -> smallest label
            return votes.entrySet().stream()
                    .max((a, b) -> {
                        int c = Integer.compare(a.getValue(), b.getValue());
                        if (c != 0) return c;
                        return Integer.compare(b.getKey(), a.getKey()); // so smaller key wins in tie-break by reversing
                    }).get().getKey();
        }

        public int predict(int[] seq) {
            Stats s = computeStats(seq);
            return predict(s.toArray());
        }

        // Train from raw sequences and an optimalP provider
        public static RandomForest trainFromSequences(List<int[]> sequences,
                                                      int numTrees, int maxDepth, int minSamplesSplit,
                                                      int maxFeatures, long seed) {
            List<double[]> X = new ArrayList<>();
            List<Integer> y = new ArrayList<>();
            for (int[] seq : sequences) {
                Integer label = findMinCostPack(seq);
//                System.out.println(label);
                Stats st = computeStats(seq);
//                System.out.println(Arrays.toString(st.toArray()));
                X.add(st.toArray());
                y.add(label);
            }
            return train(X, y, numTrees, maxDepth, minSamplesSplit, maxFeatures, seed);
        }

        // Train from feature matrix X and labels y
        public static RandomForest train(List<double[]> X, List<Integer> y,
                                         int numTrees, int maxDepth, int minSamplesSplit,
                                         int maxFeatures, long seed) {
            int n = X.size();
            Random rng = new Random(seed);
            List<DecisionTree> trees = new ArrayList<>();
            for (int t = 0; t < numTrees; ++t) {
                // bootstrap sample indices
                int[] indices = new int[n];
                for (int i = 0; i < n; ++i) indices[i] = rng.nextInt(n);
                List<double[]> bx = new ArrayList<>(n);
                List<Integer> by = new ArrayList<>(n);
                for (int i = 0; i < n; ++i) {
                    bx.add(X.get(indices[i]));
                    by.add(y.get(indices[i]));
                }
                DecisionTree tree = DecisionTree.train(bx, by, maxDepth, minSamplesSplit, maxFeatures, rng.nextLong());
                trees.add(tree);
            }
            return new RandomForest(trees, seed);
        }
    }

    // Simple CART decision tree for classification (continuous features)
    public static class DecisionTree {
        private final Node root;
        private DecisionTree(Node root) { this.root = root; }
        public int predict(double[] features) { return root.predict(features); }

        private static class Node {
            // internal
            int featureIndex;
            double threshold;
            Node left, right;
            // leaf
            boolean isLeaf;
            int prediction; // majority label at leaf

            int predict(double[] f) {
                if (isLeaf) return prediction;
                if (f[featureIndex] <= threshold) return left.predict(f);
                else return right.predict(f);
            }
        }

        // Train tree on bx, by
        public static DecisionTree train(List<double[]> X, List<Integer> y,
                                         int maxDepth, int minSamplesSplit, int maxFeatures, long seed) {
            int nFeatures = X.get(0).length;
            Random rng = new Random(seed);
            return new DecisionTree(buildNode(X, y, 0, maxDepth, minSamplesSplit, maxFeatures, nFeatures, rng));
        }

        private static Node buildNode(List<double[]> X, List<Integer> y, int depth,
                                      int maxDepth, int minSamplesSplit, int maxFeatures,
                                      int nFeatures, Random rng) {
            Node node = new Node();
            // compute label counts
            Map<Integer, Integer> counts = new HashMap<>();
            for (int lab : y) counts.put(lab, counts.getOrDefault(lab, 0) + 1);
            int majorityLabel = counts.entrySet().stream().max(Map.Entry.comparingByValue()).get().getKey();

            // stopping
            if (depth >= maxDepth || X.size() < minSamplesSplit || counts.size() == 1) {
                node.isLeaf = true;
                node.prediction = majorityLabel;
                return node;
            }

            double bestGini = Double.POSITIVE_INFINITY;
            int bestFeature = -1;
            double bestThresh = 0.0;
            // pick random subset of features
            int[] featureIndices = randomFeatureSubset(nFeatures, maxFeatures, rng);

            // for each feature, consider thresholds
            for (int fi : featureIndices) {
                // collect pairs (value, label)
                int m = X.size();
                double[] vals = new double[m];
                int[] labs = new int[m];
                for (int i = 0; i < m; ++i) { vals[i] = X.get(i)[fi]; labs[i] = y.get(i); }
                // sort pairs by vals
                Integer[] idx = new Integer[m];
                for (int i = 0; i < m; ++i) idx[i] = i;
                Arrays.sort(idx, Comparator.comparingDouble(i -> vals[i]));
                // generate candidate thresholds at midpoints between distinct values
                for (int i = 1; i < m; ++i) {
                    if (vals[idx[i]] == vals[idx[i - 1]]) continue;
                    double thr = (vals[idx[i]] + vals[idx[i - 1]]) / 2.0;
                    // compute gini of split
                    double gini = giniSplit(vals, labs, idx, i);
                    if (gini < bestGini) {
                        bestGini = gini;
                        bestFeature = fi;
                        bestThresh = thr;
                    }
                }
            }

            if (bestFeature == -1) { // cannot split
                node.isLeaf = true;
                node.prediction = majorityLabel;
                return node;
            }

            // partition data by best split
            List<double[]> leftX = new ArrayList<>(), rightX = new ArrayList<>();
            List<Integer> leftY = new ArrayList<>(), rightY = new ArrayList<>();
            for (int i = 0; i < X.size(); ++i) {
                if (X.get(i)[bestFeature] <= bestThresh) {
                    leftX.add(X.get(i)); leftY.add(y.get(i));
                } else {
                    rightX.add(X.get(i)); rightY.add(y.get(i));
                }
            }
            // if one side empty (rare), make leaf
            if (leftX.isEmpty() || rightX.isEmpty()) {
                node.isLeaf = true;
                node.prediction = majorityLabel;
                return node;
            }

            node.featureIndex = bestFeature;
            node.threshold = bestThresh;
            node.left = buildNode(leftX, leftY, depth + 1, maxDepth, minSamplesSplit, maxFeatures, nFeatures, rng);
            node.right = buildNode(rightX, rightY, depth + 1, maxDepth, minSamplesSplit, maxFeatures, nFeatures, rng);
            node.isLeaf = false;
            return node;
        }

        private static double giniSplit(double[] vals, int[] labs, Integer[] idx, int cutIndex) {
            // left: idx[0..cutIndex-1], right: idx[cutIndex..end]
            Map<Integer, Integer> leftCount = new HashMap<>();
            Map<Integer, Integer> rightCount = new HashMap<>();
            for (int i = 0; i < idx.length; ++i) {
                if (i < cutIndex) leftCount.put(labs[idx[i]], leftCount.getOrDefault(labs[idx[i]], 0) + 1);
                else rightCount.put(labs[idx[i]], rightCount.getOrDefault(labs[idx[i]], 0) + 1);
            }
            int nLeft = cutIndex;
            int nRight = idx.length - cutIndex;
            double gLeft = 1.0;
            for (int c : leftCount.values()) {
                double p = c / (double) nLeft; gLeft -= p * p;
            }
            double gRight = 1.0;
            for (int c : rightCount.values()) {
                double p = c / (double) nRight; gRight -= p * p;
            }
            double total = idx.length;
            return (nLeft / total) * gLeft + (nRight / total) * gRight;
        }

        private static int[] randomFeatureSubset(int nFeatures, int maxFeatures, Random rng) {
            maxFeatures = Math.max(1, Math.min(maxFeatures, nFeatures));
            List<Integer> all = new ArrayList<>();
            for (int i = 0; i < nFeatures; ++i) all.add(i);
            Collections.shuffle(all, rng);
            int[] res = new int[maxFeatures];
            for (int i = 0; i < maxFeatures; ++i) res[i] = all.get(i);
            return res;
        }
    }

    public static long[] sprintz(long[] numbers) {
        int size = numbers.length;
        long[] result = new long[size];

        if (size == 0) return result; // 空数组直接返回

        long first = numbers[0];
        result[0] = first;

        long prev = first;
        for (int i = 1; i < size; i++) {
            long current = numbers[i];
            long diff = current - prev;
            // ZigZag 编码: 正数 -> 偶数, 负数 -> 奇数
            result[i] = (diff << 1) ^ (diff >> 63);
            prev = current;
        }

        return result;
    }

    public static void main(String[] args) throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
//        String csvFilePath = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/processed_data.csv";
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPRF_sprintz";
        File outputDir = new File(outputDirstr);

        String trainCsv = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/processed_data.csv";
        List<int[]> sequences = loadDataFromCSV(trainCsv);
        System.err.println("Loaded " + sequences.size() + " sequences from CSV");
//        for(int[] seq : sequences){
//           int optimalP = findMinCostPack(seq);
//           Stats stats = computeStats(seq);
//        }
        int numTrees = 20;
        int maxDepth = 8;
        int minSamplesSplit = 2;
        int maxFeatures = 2; // each split tries 2 random features out of 3
        long seed = 12345L;

        // Train
        RandomForest rf = RandomForest.trainFromSequences(sequences, numTrees, maxDepth, minSamplesSplit, maxFeatures, seed);



//        RLDecisionModel trainedModel = trainModel(20, csvFilePath);
        if (!outputDir.exists()) outputDir.mkdir();
        File dir = new File(directory);
        for (File file : Objects.requireNonNull(dir.listFiles())) {

            if (file.isDirectory() || !BenchmarkDatasetFilter.includeDatasetFile(file.getName())) continue;
//            if(!file.getName().equals("Stocks-DE.csv")) continue;
            System.out.println(file.getName());
            String Output = outputDirstr+"/"+file.getName();
            CsvWriter writer = new CsvWriter(Output, ',', StandardCharsets.UTF_8);

            String[] head = {
                    "Input Direction",
                    "Encoding Algorithm",
                    "Encoding Time",
                    "Points",
                    "Compressed Size",
                    "Compression Ratio"
            };
            writer.writeRecord(head); // write header to output file
            System.out.println("Processing " + file.getName() + "...");
            List<String> numbers = new ArrayList<>();
            List<Integer> decimalPlaces = new ArrayList<>();
            CsvReader csvReader = new CsvReader(file.getPath(), ',', StandardCharsets.UTF_8);
            while (csvReader.readRecord()) {
                for (String value : csvReader.getValues()) {
                    String numStr = value.trim();
                    if (!numStr.isEmpty()) {
                        numbers.add(numStr);
                        int decimal = 0, sigBits;
                        if (numStr.contains(".")) {
                            String[] parts = numStr.split("\\.");
                            decimal = parts[1].length();
                            sigBits = (int) ((parts[0].length() + decimal) * (Math.log(10) / Math.log(2)));
                        } else {
                            sigBits = (int) (numStr.length() * (Math.log(10) / Math.log(2)));
                        }
                        decimalPlaces.add(decimal);
                    }
                }
            }
            int time_of_repeat = 50;
//            System.out.println(numbers.size());


            // 方法：强化学习
//            long modelStart =  System.nanoTime();
            int modelCost = 0;
            long modelTime = 0;
            for(int j=0;j<time_of_repeat;j++){
                int totalCost = 0;
                for (int i = 0; i < numbers.size(); i += CHUNK_SIZE) {

                    List<String> chunkNumbers = numbers.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()));
                    if(chunkNumbers.size()==1 || chunkNumbers.size()==2)
                        continue;

                    int decimalMax = decimalPlaces.subList(i, Math.min(i + CHUNK_SIZE, numbers.size()))
                            .stream().max(Integer::compare).orElse(0);


                    long[] scaledInt = scaleNumbers(chunkNumbers, decimalMax);

                    long startTime = System.nanoTime();
                    long[] scaledInts = sprintz(scaledInt);
                    int remainder = scaledInts.length % 8;
                    int paddingLength = (remainder == 0) ? 0 : 8 - remainder;

                    // 创建新数组，长度补齐为8的倍数
                    long[] paddedArray = new long[scaledInts.length + paddingLength];
                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                    int actual_length = paddedArray.length;
                    int[] bitWidths = new int[actual_length / 8]; // 存储每8个值的位宽结果

                    for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += 8) {
                        // 1. 找出当前8个元素中的最大值
                        long maxInGroup = 0;
                        for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + 8; scaledInts_j++) {
                            if (paddedArray[scaledInts_j] > maxInGroup) {
                                maxInGroup = paddedArray[scaledInts_j];
                            }
                        }

                        // 2. 计算该最大值的去头零位宽
                        int bitWidth = 64 - Long.numberOfLeadingZeros(maxInGroup);

                        // 3. 存储结果
                        bitWidths[scaledInts_i / 8] = bitWidth;
                    }
                    int optimalP = rf.predict(bitWidths);
//                    System.out.println("bitwidth:"+bitWidths.length);
//                    System.out.println("optimalP:"+optimalP);

                    int cur_cost = bitPacking(bitWidths, optimalP);

//                    PackingResult result = packOctads(bitWidths, model, null); // 禁用决策跟踪
                    long duration = System.nanoTime() - startTime;
                    modelTime += (duration);
                    modelCost +=  cur_cost;
//                    if(i==0)
//                        for (int episode = 0; episode < 10; episode++) {
//                            trainEpisode(scaledInts, episode);
//                        }
//                    List<Integer> optimalK = predictOptimalK(scaledInts);
//                    System.out.println("Optimal k sequence: " + optimalK);
                }

            }
            modelCost /=time_of_repeat;
            modelTime = (modelTime)/time_of_repeat;
            double model_ratio = (double) modelCost / (double) (numbers.size()*64);
            double modelTime_throughput = (double)(numbers.size()*8000)/ (double) (modelTime);
            String[] record = {
                    file.toString(),
                    "BP-FP",
                    String.valueOf(modelTime_throughput),
                    String.valueOf(numbers.size()),
                    String.valueOf(modelCost),
                    String.valueOf(model_ratio)
            };
            writer.writeRecord(record);
            writer.close();
//            break;
        }

    }


}