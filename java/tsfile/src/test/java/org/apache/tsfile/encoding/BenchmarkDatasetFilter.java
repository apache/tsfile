package org.apache.iotdb.tsfile.encoding;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Whitelist of CSV basenames under {@code ElfTestData_camel} (LaTeX {@code table:dataset}).
 *
 * <p>Active rows only (10 datasets). Excluded: City-temp (CT). Keep in sync with {@code
 * paper_datasets.py} / plotting scripts {@code dataset_mapping}.
 */
public final class BenchmarkDatasetFilter {

    private static final Set<String> ALLOWED_CSV = new HashSet<>();

    static {
        String[] names =
                new String[] {
                    "Blockchain-tr.csv", // BTR
                    "CS-Sensors.csv", // CS
                    "Cyber-Vehicle.csv", // CV
                    "EPM-Education.csv", // EE
                    "Food-price.csv", // FP
                    "PM10-dust.csv", // PM10
                    "Stocks-UK.csv", // SUK
                    "TH-Climate.csv", // TC
                    "TY-Transport.csv", // TT
                    "USGS-Earthquakes.csv", // UE
                };
        Collections.addAll(ALLOWED_CSV, names);
    }

    private BenchmarkDatasetFilter() {}

    /** @return true if this basename should be processed (exact match, e.g. {@code Foo.csv}). */
    public static boolean includeDatasetFile(String fileName) {
        return fileName != null && ALLOWED_CSV.contains(fileName);
    }
}
