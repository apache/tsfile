package org.apache.iotdb.tsfile.encoding;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import com.csvreader.CsvReader;
import com.csvreader.CsvWriter;
import java.util.*;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.io.FileReader;
import java.util.ArrayList;
import java.util.List;

public class RLEfficientOctadPacking {

    private static final int CHUNK_SIZE = 1000;



    // 高效的强化学习决策模型
    static class RLDecisionModel {
        // 使用数组代替对象，提高性能
        private final float[] weights = new float[5]; // 5个特征权重
        private float explorationRate = 0.3f;
        private final float learningRate = 0.01f;

        public RLDecisionModel() {
            // 初始化随机权重
            ThreadLocalRandom random = ThreadLocalRandom.current();
            for (int i = 0; i < weights.length; i++) {
                weights[i] = random.nextFloat() * 2 - 1; // [-1, 1]
            }
        }

        // 决策函数 - 避免对象创建
        public boolean shouldMerge(
                int currentPackSize,
                int currentPackMaxB,
                int newOctadB,
                int packCount,
                int currentMaxLog
        ) {
            // 探索
            if (ThreadLocalRandom.current().nextFloat() < explorationRate) {
                return ThreadLocalRandom.current().nextBoolean();
            }

            // 利用：计算特征向量点积
            float score = 0;
            score += weights[0] * currentPackSize / 100.0f;       // 归一化
            score += weights[1] * currentPackMaxB / 32.0f;         // 归一化
            score += weights[2] * newOctadB / 32.0f;               // 归一化
            score += weights[3] * packCount / 100.0f;              // 归一化
            score += weights[4] * currentMaxLog / 10.0f;           // 归一化

            // Sigmoid激活函数
            float probability = 1.0f / (1.0f + (float)Math.exp(-score));
            return probability > 0.5f;
        }

        // 高效训练方法 - 使用REINFORCE算法
        public void train(List<DecisionPoint> decisions, float reward) {
            // 减少探索率
            explorationRate *= 0.99f;
            if (explorationRate < 0.05f) explorationRate = 0.05f;

            // 更新权重
            for (DecisionPoint dp : decisions) {
                float gradient = dp.action ? (1 - dp.probability) : -dp.probability;
                gradient *= reward * learningRate;

                // 更新权重
                weights[0] += gradient * dp.currentPackSize / 100.0f;
                weights[1] += gradient * dp.currentPackMaxB / 32.0f;
                weights[2] += gradient * dp.newOctadB / 32.0f;
                weights[3] += gradient * dp.packCount / 100.0f;
                weights[4] += gradient * dp.currentMaxLog / 10.0f;
            }
        }
    }

    // 决策点记录（轻量级）
    static class DecisionPoint {
        final int currentPackSize;
        final int currentPackMaxB;
        final int newOctadB;
        final int packCount;
        final int currentMaxLog;
        final boolean action;
        final float probability;

        public DecisionPoint(int currentPackSize, int currentPackMaxB,
                             int newOctadB, int packCount,
                             int currentMaxLog, boolean action,
                             float probability) {
            this.currentPackSize = currentPackSize;
            this.currentPackMaxB = currentPackMaxB;
            this.newOctadB = newOctadB;
            this.packCount = packCount;
            this.currentMaxLog = currentMaxLog;
            this.action = action;
            this.probability = probability;
        }
    }


    // 快速生成位宽数据
    public static int[] generateBitWidths(int count) {
        int[] bitWidths = new int[count];
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = 0; i < count; i++) {
            bitWidths[i] = random.nextInt(1, 33); // 1-32位
        }
        return bitWidths;
    }


    private static int zigzagEncode(int n) {
        return (n << 1) ^ (n >> 31);
    }
//    public static int[] scaleNumbers(List<String> numbers, int decimalMax) {
//        int scale = (int) Math.pow(10, decimalMax);
//        int size = numbers.size();
//        int[] result = new int[size];
//
//        if (size == 0) {
//            return result;
//        }
//
//        // 1. Parse all numbers and scale them up
//        int[] scaledValues = new int[size];
//        for (int i = 0; i < size; i++) {
//            String numStr = numbers.get(i);
//            // Parse the number (handling both "123.456" and "123" cases)
//            String[] parts = numStr.split("\\.");
//            int whole = Integer.parseInt(parts[0]);
//
//            // Handle fractional part
//            int fraction = 0;
//            if (parts.length > 1) {
//                String fractionStr = parts[1];
//                // Pad with zeros if necessary to ensure proper scaling
//                if (fractionStr.length() < decimalMax) {
//                    while (fractionStr.length() < decimalMax) {
//                        fractionStr += "0";
//                    }
//                } else if (fractionStr.length() > decimalMax) {
//                    // Truncate if too many decimal places (alternative could be rounding)
//                    fractionStr = fractionStr.substring(0, decimalMax);
//                }
//                fraction = Integer.parseInt(fractionStr);
//            }
//
//            scaledValues[i] = whole * scale + fraction;
//        }
//
//        // 2. Process first element
//        //        result[0] = first;
//
//        // 3. Process subsequent elements with delta + ZigZag encoding
//        int minimum = scaledValues[0];
//        for (int i = 1; i < size; i++) {
//            int current = scaledValues[i];
//            if(current < minimum) minimum=current;
//        }
//        for (int i = 0; i < size; i++){
//            result[i] =  scaledValues[i] - minimum;
//        }
//
//        return result;
//    }
    private static int[] scaleNumbers(List<String> numbers, int decimalMax) {
        // 1. 预先计算缩放因子
        BigDecimal scale = BigDecimal.TEN.pow(decimalMax);
        int size = numbers.size();
        int[] result = new int[size];

        if (size == 0) {
            return result;
        }

        // 2. 单次遍历完成所有转换和最小值查找
        BigDecimal min = null;
        BigDecimal[] scaledValues = new BigDecimal[size];

        for (int i = 0; i < size; i++) {
            BigDecimal val = new BigDecimal(numbers.get(i)).multiply(scale);
            scaledValues[i] = val;
            if (min == null || val.compareTo(min) < 0) {
                min = val;
            }
        }

        // 3. 处理第一个元素
        BigDecimal first = scaledValues[0].subtract(min);
        result[0] = first.toBigInteger().intValue();

        // 4. 处理后续元素（差分+ZigZag）
        for (int i = 1; i < size; i++) {
            BigDecimal current = scaledValues[i].subtract(min);
            result[i]=current.toBigInteger().intValue();
        }

        return result;
    }
//
    public static void performanceTest(ActorNetwork actor) throws IOException {
        // 示例数据（实际应替换为真实时间序列）
        System.out.println("\nPerformance Testing...");
//        String csvFilePath = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/processed_data.csv";
        String directory = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/ElfTestData_camel";
        String outputDirstr = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/output_BPRL";
        File outputDir = new File(outputDirstr);

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


                    int[] scaledInts = scaleNumbers(chunkNumbers, decimalMax);

                    long startTime = System.nanoTime();
                    int remainder = scaledInts.length % 8;
                    int paddingLength = (remainder == 0) ? 0 : 8 - remainder;

                    // 创建新数组，长度补齐为8的倍数
                    int[] paddedArray = new int[scaledInts.length + paddingLength];
                    System.arraycopy(scaledInts, 0, paddedArray, 0, scaledInts.length);
                    int actual_length = paddedArray.length;
                    int[] bitWidths = new int[actual_length / 8]; // 存储每8个值的位宽结果

                    for (int scaledInts_i = 0; scaledInts_i < actual_length; scaledInts_i += 8) {
                        // 1. 找出当前8个元素中的最大值
                        int maxInGroup = 0;
                        for (int scaledInts_j = scaledInts_i; scaledInts_j < scaledInts_i + 8; scaledInts_j++) {
                            if (paddedArray[scaledInts_j] > maxInGroup) {
                                maxInGroup = paddedArray[scaledInts_j];
                            }
                        }

                        // 2. 计算该最大值的去头零位宽
                        int bitWidth = 32 - Integer.numberOfLeadingZeros(maxInGroup);

                        // 3. 存储结果
                        bitWidths[scaledInts_i / 8] = bitWidth;
                    }

                    double actorCost = testPacking(actor, bitWidths);
                    long duration = System.nanoTime() - startTime;
                    modelTime += (duration);
                    modelCost += actorCost;
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
                    "BP-Reinforce-learning",
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




    // 状态类
    static class State {
        int i;       // 已处理octad数量
        int p_m;     // 历史最大包大小
        int p_k;     // 当前包大小
        int k;       // 包数量
        int b_new;   // 下一个octad位宽
        int b_max;   // 当前包最大位宽

        public State(int i, int p_m, int p_k, int k, int b_new, int b_max) {
            this.i = i;
            this.p_m = p_m;
            this.p_k = p_k;
            this.k = k;
            this.b_new = b_new;
            this.b_max = b_max;
        }

        public double[] toFeatureVector() {
            return new double[]{
                    i / 125.0,        // 归一化到[0,1]
                    p_m / 125.0,       // 最大包大小归一化
                    p_k / 125.0,       // 当前包大小归一化
                    k / 125.0,         // 包数量归一化
                    b_new / 64.0,      // 位宽归一化
                    b_max / 64.0       // 位宽归一化
            };
//            return new double[]{i, p_m, p_k, k, b_new, b_max};
        }
    }

    // Actor网络
    static class ActorNetwork implements Serializable{
        double[][] weights; // 权重矩阵 [2][6]
        double[] biases;    // 偏置 [2]

        public ActorNetwork() {
            weights = new double[2][6];
            biases = new double[2];
            initializeWeights();
        }

        private void initializeWeights() {
            Random rand = new Random();
            for (int i = 0; i < 2; i++) {
                for (int j = 0; j < 6; j++) {
                    weights[i][j] = rand.nextGaussian() * 0.1;
                }
//                System.out.println(Arrays.toString(weights[i]));
                biases[i] = rand.nextGaussian() * 0.1;
            }
//            System.out.println(Arrays.toString(biases));
        }

        public double[] getActionProbabilities(State state) {
            double[] x = state.toFeatureVector();
            double[] logits = new double[2];
//            for (int i = 0; i < 2; i++) {
////                for (int j = 0; j < 6; j++) {
////                    weights[i][j] = rand.nextGaussian() * 0.1;
////                }
//                System.out.println(Arrays.toString(weights[i]));
////                biases[i] = rand.nextGaussian() * 0.1;
//            }
//            System.out.println(Arrays.toString(biases));
//            System.out.println(Arrays.toString(x));
//            System.out.println(Arrays.toString(biases));

            for (int a = 0; a < 2; a++) {
                logits[a] = dotProduct(weights[a], x) + biases[a];
            }

            // Softmax
            double exp0 = Math.exp(logits[0]);
            double exp1 = Math.exp(logits[1]);
            double sum = exp0 + exp1;
            return new double[]{exp0/sum, exp1/sum};
        }

        public int chooseAction(State state, double epsilon,int episode,int totalEpisodes) {
            double decayedEpsilon = epsilon * Math.exp(-episode/(totalEpisodes/5.0));

            if (Math.random() < decayedEpsilon) {
                return Math.random() < 0.5 ? 0 : 1;
            }
//            if (Math.random() < epsilon) {
//                return Math.random() < 0.5 ? 0 : 1; // 随机探索
//            }
            double[] probs = getActionProbabilities(state);

            return Math.random() < probs[0] ? 0 : 1;
        }

        public int chooseBestAction(State state) {
//            double[] probs = getActionProbabilities(state);
//            return Math.random() < probs[0] ? 0 : 1; // 仍保持随机性
            double[] probs = getActionProbabilities(state);
//            System.out.println(Arrays.toString(probs));
            return probs[0] > probs[1] ? 0 : 1;
        }

        public void update(State state, int action, double advantage, double learningRate) {
            double[] x = state.toFeatureVector();
            double[] probs = getActionProbabilities(state);


            for (int a = 0; a < 2; a++) {
                double gradientFactor = advantage * ((a == action ? 1 : 0) - probs[a]);

                for (int i = 0; i < 6; i++) {
                    weights[a][i] += learningRate * gradientFactor * x[i];
                }
                biases[a] += learningRate * gradientFactor;

                // 梯度裁剪
                double maxGradNorm = 1.0;
                double gradNorm = 0;
                for (double w : weights[a]) gradNorm += w * w;
                gradNorm = Math.sqrt(gradNorm);

                if (gradNorm > maxGradNorm) {
                    double scale = maxGradNorm / gradNorm;
                    for (int i = 0; i < weights[a].length; i++) {
                        weights[a][i] *= scale;
                    }
                    biases[a] *= scale;
                }
            }

//            System.out.println("advantage:   "+advantage);
        }

        private double dotProduct(double[] a, double[] b) {
            double sum = 0;
            for (int i = 0; i < a.length; i++) {
                sum += a[i] * b[i];
            }
            return sum;
        }

        public void saveModel(String filename) {
            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename))) {
                oos.writeObject(this);
            } catch (IOException e) {
                System.err.println("Error saving model: " + e.getMessage());
            }
        }
        public void printActor(){
            for (int i = 0; i < 2; i++) {
                System.out.println(Arrays.toString(weights[i]));
            }
            System.out.println(Arrays.toString(biases));
        }

        // 从文件加载模型
        public static ActorNetwork loadModel(String filename) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filename))) {
                return (ActorNetwork) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                System.err.println("Error loading model: " + e.getMessage());
                return null;
            }
        }

    }

    // Critic网络
    static class CriticNetwork implements Serializable{
        double[] weights; // 权重向量 [6]
        double bias;

        public CriticNetwork() {
            weights = new double[6];
            bias = 0;
            initializeWeights();
        }

        private void initializeWeights() {
            Random rand = new Random();
            for (int i = 0; i < 6; i++) {
                weights[i] = rand.nextGaussian() * 0.1;
            }
            bias = rand.nextGaussian() * 0.1;
        }

        public double evaluate(State state) {
            double[] x = state.toFeatureVector();
            return dotProduct(weights, x) + bias;
        }

        public void update(State state, double target, double learningRate) {
            double[] x = state.toFeatureVector();
            double V = evaluate(state);
            double error = V - target;

            for (int i = 0; i < 6; i++) {
                weights[i] -= learningRate * 2 * error * x[i];
            }
            bias -= learningRate * 2 * error;
        }

        private double dotProduct(double[] a, double[] b) {
            double sum = 0;
            for (int i = 0; i < a.length; i++) {
                sum += a[i] * b[i];
            }
            return sum;
        }
    }
    public static double computeCostForCompress(int[] B, List<Integer> actions) {
        List<Integer> packSizes = new ArrayList<>();
        List<Integer> packMaxB = new ArrayList<>();
        int start = 0;

        for (int t = 0; t < actions.size(); t++) {
            if (actions.get(t) == 1 || t == actions.size() - 1) {
                int end = t;
                int packSize = end - start + 1;
                int maxB = 0;

                for (int j = start; j <= end; j++) {
                    if (B[j] > maxB) maxB = B[j];
                }

                packSizes.add(packSize);
                packMaxB.add(maxB);
                start = t + 1;
            }
        }

        int l = packSizes.size();
        int maxPackSize = Collections.max(packSizes);
        double cost = 0;

        // 数据存储成本
        for (int i = 0; i < l; i++) {
            cost += 8 * packSizes.get(i) * packMaxB.get(i);
        }

        // 固定开销
        cost += 5 * l;

        // 包大小信息开销
        cost += l * Math.ceil(Math.log(maxPackSize + 1) / Math.log(2));

        return cost;
    }
    // 计算存储成本
    public static double computeCost(int[] B, List<Integer> actions) {
        List<Integer> packSizes = new ArrayList<>();
        List<Integer> packMaxB = new ArrayList<>();
        int start = 0;

        for (int t = 0; t < actions.size(); t++) {
            if (actions.get(t) == 1 || t == actions.size() - 1) {
                int end = t;
                int packSize = end - start + 1;
                int maxB = 0;

                for (int j = start; j <= end; j++) {
                    if (B[j] > maxB) maxB = B[j];
                }

                packSizes.add(packSize);
                packMaxB.add(maxB);
                start = t + 1;
            }
        }

        int l = packSizes.size();
        int maxPackSize = Collections.max(packSizes);
        double cost = 0;

        // 数据存储成本
        for (int i = 0; i < l; i++) {
            cost += 8 * packSizes.get(i) * packMaxB.get(i);
        }

        // 固定开销
        cost += 5 * l;

        // 包大小信息开销
        cost += l * Math.ceil(Math.log(maxPackSize + 1) / Math.log(2));
        double normalizedCost = Math.log(cost + 1) / Math.log(1e6);
        return normalizedCost;
//        return cost;
    }

    // 主训练函数
    public static ActorNetwork actor_critic_train(List<int[]> sequences, int episodes,
                                          double alpha_actor, double alpha_critic, double epsilon,
                                                  String modelPath   ) {

        ActorNetwork actor = new ActorNetwork();
        CriticNetwork critic = new CriticNetwork();
        double gamma = 0.99;
        double learningRateDecay = 0.995;
        double initialEpsilon = epsilon;
        double minEpsilon = 0.1;
        double epsilonDecay = 0.995;

        for (int ep = 0; ep < episodes; ep++) {
            alpha_actor = Math.max(0.001, alpha_actor * learningRateDecay);
            alpha_critic = Math.max(0.001, alpha_critic * learningRateDecay);
            double currentEpsilon = minEpsilon +
                    (initialEpsilon - minEpsilon) *
                            Math.exp(-ep * epsilonDecay);

            for (int[] B : sequences) {
                List<State> states = new ArrayList<>();
                List<Integer> actions = new ArrayList<>();

                // 初始化状态
                State currentState = new State(0, 0, 0, 0, B[0], 0);

                // 收集轨迹
                for (int t = 0; t < B.length; t++) {
                    states.add(currentState);

                    int action;
                    if (t == 0) {
                        action = 1; // 第一个动作必须是新建包
                    } else if (B[t]==B[t-1]){
                        action = 0;
                    }
                    else{
                        action = actor.chooseAction(currentState, currentEpsilon,ep,episodes);
                    }

                    actions.add(action);

                    // 计算下一个状态
                    int next_i = currentState.i + 1;
                    int next_b_new = (t < B.length - 1) ? B[t + 1] : 0;
                    int next_p_m, next_p_k, next_k, next_b_max;

                    if (action == 0) { // 合并
                        next_p_m = Math.max(currentState.p_m, currentState.p_k + 1);
                        next_p_k = currentState.p_k + 1;
                        next_k = currentState.k;
                        next_b_max = Math.max(currentState.b_max, currentState.b_new);
                    } else { // 新建包
                        next_p_m = Math.max(currentState.p_m, currentState.p_k);
                        next_p_k = 1;
                        next_k = currentState.k + 1;
                        next_b_max = currentState.b_new;
                    }

                    currentState = new State(
                            next_i, next_p_m, next_p_k, next_k, next_b_new, next_b_max
                    );
                }

                // 计算最终成本
                double cost = computeCostForCompress(B, actions)/64000;
                double R = 1 - cost;  // -cost; // 奖励 = 负成本
//                System.out.println("奖励："+R);

                // 更新网络
                for (int t = 0; t < B.length - 1; t++) {
                    State state = states.get(t);
                    State nextStates = states.get(t+1);
                    int action = actions.get(t);

                    double V = critic.evaluate(state);
                    double nextV = critic.evaluate(nextStates);
                    double target = R + gamma * nextV;
                    double advantage = target - V;

                    // 更新Critic
                    critic.update(state, R, alpha_critic);

                    // 更新Actor
                    actor.update(state, action, advantage, alpha_actor);
                }
            }
            // 每5轮保存一次模型
            if ((ep + 1) % 5 == 0 && modelPath != null) {
                actor.saveModel(modelPath + "_ep" + (ep+1) + ".model");
            }
        }
        // 保存最终模型
        if (modelPath != null) {
            actor.saveModel(modelPath + "_final.model");
        }

        return actor;
    }

    public static double testPacking(ActorNetwork actor, int[] bitWidths) {
        List<Integer> actions = new ArrayList<>();
        State currentState = new State(0, 0, 0, 0, bitWidths[0], 0);

        // 生成打包策略
        for (int t = 0; t < bitWidths.length; t++) {
            int action;
            if (t == 0) {
                action = 1; // 第一个动作必须是新建包
            } else if (bitWidths[t]==bitWidths[t-1]){
                action = 0;
            }else {
                action = actor.chooseBestAction(currentState);
            }

            actions.add(action);

            // 计算下一个状态
            int next_i = currentState.i + 1;
            int next_b_new = (t < bitWidths.length - 1) ? bitWidths[t + 1] : 0;
            int next_p_m, next_p_k, next_k, next_b_max;

            if (action == 0) { // 合并
                next_p_m = Math.max(currentState.p_m, currentState.p_k + 1);
                next_p_k = currentState.p_k + 1;
                next_k = currentState.k;
                next_b_max = Math.max(currentState.b_max, currentState.b_new);
            } else { // 新建包
                next_p_m = Math.max(currentState.p_m, currentState.p_k);
                next_p_k = 1;
                next_k = currentState.k + 1;
                next_b_max = currentState.b_new;
            }

            currentState = new State(
                    next_i, next_p_m, next_p_k, next_k, next_b_new, next_b_max
            );
        }

        // 计算并返回成本
        return computeCostForCompress(bitWidths, actions);
    }
    // 示例用法
    public static void main(String[] args) throws IOException {
        // 生成训练数据

        String csvFilePath = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/rl_train_data.csv";
//        RLDecisionModel trainedModel = trainModel(20, csvFilePath);
        String modelPath = "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/packing_model";
        System.out.println("Training RL model from CSV data...");
        RLDecisionModel model = new RLDecisionModel();
        List<int[]> sequences = new ArrayList<>();
        CsvReader reader = new CsvReader(csvFilePath);
        reader.readHeaders(); // 跳过表头

        while (reader.readRecord()) {
            int[] arr = new int[reader.getColumnCount()];
            for (int i = 0; i < arr.length; i++) {
                arr[i] = Integer.parseInt(reader.get(i));
            }
            sequences.add(arr);
        }
        reader.close();
        // sequences 现在就是 List<int[]>，每个 int[] 是一行数据
        System.out.println("Loaded " + sequences.size() + " sequences.");
        if (sequences.isEmpty()) {
            System.out.println("No data loaded from CSV. Exiting.");
            return;
//            return model;
        }

        System.out.println("Loaded " + sequences.size() + " sequences from CSV");


        // 训练参数
        int episodes = 500;
        double alpha_actor = 0.01;
        double alpha_critic = 0.01;
        double epsilon = 0.1;

        // 执行训练
        ActorNetwork trainedActor = actor_critic_train(sequences, episodes, alpha_actor, alpha_critic, epsilon,modelPath);
        trainedActor.printActor();


        System.out.println("Training completed!");
        int[] bitWidths = {4, 4, 4, 4, 4, 4, 4, 8, 7, 4, 4, 4, 3, 3, 8};
        List<Integer> actions = new ArrayList<>();
        State currentState = new State(0, 0, 0, 0, bitWidths[0], 0);
        for (int t = 0; t < bitWidths.length; t++) {
            int action;
            if (t == 0) {
                action = 1;
            } else {
                action = trainedActor.chooseBestAction(currentState);
            }
            actions.add(action);

            // 更新状态
            int next_i = currentState.i + 1;
            int next_b_new = (t < bitWidths.length - 1) ? bitWidths[t + 1] : 0;
            int next_p_m, next_p_k, next_k, next_b_max;

            if (action == 0) {
                next_p_m = Math.max(currentState.p_m, currentState.p_k + 1);
                next_p_k = currentState.p_k + 1;
                next_k = currentState.k;
                next_b_max = Math.max(currentState.b_max, currentState.b_new);
            } else {
                next_p_m = Math.max(currentState.p_m, currentState.p_k);
                next_p_k = 1;
                next_k = currentState.k + 1;
                next_b_max = currentState.b_new;
            }

            currentState = new State(
                    next_i, next_p_m, next_p_k, next_k, next_b_new, next_b_max
            );
        }
        printPackingResult(bitWidths,actions);




        performanceTest(trainedActor);
    }
    public static void printPackingResult(int[] bitWidths, List<Integer> actions) {
        System.out.println("Bit-width sequence: " + Arrays.toString(bitWidths));
        System.out.println("Packing actions:    " + actions);

        List<Integer> packSizes = new ArrayList<>();
        List<Integer> packMaxB = new ArrayList<>();
        int start = 0;

        System.out.println("\nPack details:");
        for (int t = 0; t < actions.size(); t++) {
            if (actions.get(t) == 1 || t == actions.size() - 1) {
                int end = t;
                int packSize = end - start + 1;
                int maxB = 0;

                for (int j = start; j <= end; j++) {
                    if (bitWidths[j] > maxB) maxB = bitWidths[j];
                }

                System.out.printf("Pack %2d: size=%2d, max_b=%2d, elements=[",
                        packSizes.size() + 1, packSize, maxB);
                for (int j = start; j <= end; j++) {
                    System.out.print(bitWidths[j]);
                    if (j < end) System.out.print(", ");
                }
                System.out.println("]");

                packSizes.add(packSize);
                packMaxB.add(maxB);
                start = t + 1;
            }
        }

        int maxPackSize = Collections.max(packSizes);
        double cost = computeCostForCompress(bitWidths, actions);

        System.out.println("\nCost breakdown:");
        System.out.printf("Data storage cost: %.2f bits\n",
                packSizes.stream().mapToDouble(s -> 8 * s).sum());
        System.out.printf("Max bit-width cost: %.2f bits\n",
                packMaxB.stream().mapToDouble(b -> 8 * b).sum());
        System.out.printf("Fixed overhead: %d bits\n", 5 * packSizes.size());
        System.out.printf("Size info cost: %.2f bits\n",
                packSizes.size() * Math.ceil(Math.log(maxPackSize + 1) / Math.log(2)));
        System.out.printf("TOTAL COST: %.2f bits\n", cost);
    }
}