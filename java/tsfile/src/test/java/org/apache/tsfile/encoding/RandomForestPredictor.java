package org.apache.iotdb.tsfile.encoding;
import java.io.FileReader;
import java.util.List;
import com.google.gson.Gson;
import com.google.gson.annotations.SerializedName;

public class RandomForestPredictor {
    private final Tree[] trees;
    private final int nFeatures;

    // 树结构定义
    private static class Tree {
        @SerializedName("left_child")
        int[] leftChild;
        @SerializedName("right_child")
        int[] rightChild;
        @SerializedName("features")
        int[] features;
        @SerializedName("thresholds")
        double[] thresholds;
        @SerializedName("values")
        double[] values;
    }

    // 从JSON加载模型
    public RandomForestPredictor(String jsonPath) throws Exception {
        Gson gson = new Gson();
        try (FileReader reader = new FileReader(jsonPath)) {
            ForestData data = gson.fromJson(reader, ForestData.class);
            this.nFeatures = data.nFeatures;
            this.trees = data.trees.toArray(new Tree[0]);
        }
    }

    // 单个树预测
    private double predictTree(Tree tree, double[] features) {
        int node = 0;
        while (tree.leftChild[node] != -1) {
            int feature = tree.features[node];
            if (features[feature] <= tree.thresholds[node]) {
                node = tree.leftChild[node];
            } else {
                node = tree.rightChild[node];
            }
        }
        return tree.values[node];
    }

    // 森林预测
    public int predict(double[] features) {
        if (features.length != nFeatures) {
            throw new IllegalArgumentException("特征数量不匹配");
        }

        double sum = 0.0;
        for (Tree tree : trees) {
            sum += predictTree(tree, features);
        }
        double avgPrediction = sum / trees.length;
        return (int) Math.round(avgPrediction) * 8;
    }

    // JSON结构映射类
    private static class ForestData {
        @SerializedName("n_features")
        int nFeatures;
        @SerializedName("trees")
        List<Tree> trees;
    }

    public static void main(String[] args) throws Exception {
        // 使用示例
        RandomForestPredictor predictor = new RandomForestPredictor("/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/forest_params.json");
        double[] testFeatures = {5.0, 3.2, 100.0, 200.0, 15.0, 12.5};
        int blockSize = predictor.predict(testFeatures);
        System.out.println("预测块大小: " + blockSize);
    }
}