package org.apache.iotdb.tsfile.encoding;
import ai.onnxruntime.*;
//import org.joblib.NDArray;
//import org.joblib.Joblib;
import shaded.parquet.org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.*;


public class ModelVarBlockTest {

    private OrtEnvironment env;
    private OrtSession session;
    private double[] means; // from scaler
    private double[] scales; // from scaler
    public ModelVarBlockTest(String modelPath, String scalerParamsPath) throws OrtException, IOException {
        env = OrtEnvironment.getEnvironment();
        session = env.createSession(modelPath);
        ObjectMapper mapper = new ObjectMapper();
        Map<String, List<Number>> scalerParams = mapper.readValue(new File(scalerParamsPath), Map.class);

        // Convert List<Number> to double[]
        List<Number> meansList = scalerParams.get("mean");
        List<Number> scalesList = scalerParams.get("scale");

        this.means = new double[meansList.size()];
        this.scales = new double[scalesList.size()];

        for (int i = 0; i < meansList.size(); i++) {
            this.means[i] = meansList.get(i).doubleValue();
        }
        for (int i = 0; i < scalesList.size(); i++) {
            this.scales[i] = scalesList.get(i).doubleValue();
        }
    }
    private float[] normalizeFeatures(float[] features) {
        float[] normalizedFeatures = new float[features.length];
        for (int i = 0; i < features.length; i++) {
            normalizedFeatures[i] = (float)((features[i] - means[i]) / scales[i]);
        }
        return normalizedFeatures;
    }

    public float[] predict(float[] features) throws OrtException {
        // Normalize features first
        float[] normalizedFeatures = normalizeFeatures(features);

        // Create tensor and run prediction
        OnnxTensor input = OnnxTensor.createTensor(env,
                new float[][]{normalizedFeatures});

        OrtSession.Result result = session.run(
                Collections.singletonMap("input", input));

        float[][] outputs = (float[][]) result.get(0).getValue();
        return outputs[0];
    }

    public static void main(String[] args) {
        try {
            ModelVarBlockTest predictor = new ModelVarBlockTest(
                    "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/partition_model.onnx",
                    "/Users/xiaojinzhao/Documents/GitHub/encoding-block/elf_resources/scaler_params.json"
            );

            float[] features = {2f, 2.0f, -13f, -6f, 16f, 15.073f};
            float[] predictions = predictor.predict(features);

            List<Integer> partitions = new ArrayList<>();
            for (int i = 0; i < predictions.length; i++) {
                if (predictions[i] > 0.5) {
                    partitions.add(i);
                }
            }

            System.out.println("Predicted partitions: " + partitions);
            predictor.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void close() throws OrtException {
        session.close();
        env.close();
    }
}
