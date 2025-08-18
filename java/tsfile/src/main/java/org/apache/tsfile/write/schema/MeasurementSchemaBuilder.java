package org.apache.tsfile.write.schema;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.util.HashMap;
import java.util.Map;

/**
 * A builder class for constructing {@link MeasurementSchema} instances.
 *
 * <p>This builder provides a fluent API for setting various properties of a measurement schema,
 * including required fields (name and data type) and optional fields (encoding and compression).
 *
 * <p>Example usage:
 *
 * <pre>{@code
 * MeasurementSchema schema = new MeasurementSchemaBuilder("temperature", TSDataType.FLOAT)
 *     .withEncoding(TSEncoding.RLE)
 *     .withCompression(CompressionType.SNAPPY)
 *     .build();
 * }</pre>
 */
public class MeasurementSchemaBuilder {
  private String measurementName;
  private TSDataType dataType;
  private TSEncoding encoding;
  private CompressionType compressionType;
  private Map<String, String> props;

  /**
   * Creates a new builder with the required fields.
   *
   * @param measurementName the name of the measurement (cannot be null or empty)
   * @param dataType the data type of the measurement (cannot be null)
   * @throws IllegalArgumentException if required parameters are null or empty
   */
  public MeasurementSchemaBuilder(String measurementName, TSDataType dataType) {
    if (measurementName == null || measurementName.trim().isEmpty()) {
      throw new IllegalArgumentException("Measurement name cannot be null or empty");
    }
    if (dataType == null) {
      throw new IllegalArgumentException("Data type cannot be null");
    }

    this.measurementName = measurementName;
    this.dataType = dataType;
    // Set default values from TSFile configuration
    this.encoding =
        TSEncoding.valueOf(TSFileDescriptor.getInstance().getConfig().getValueEncoder(dataType));
    this.compressionType = TSFileDescriptor.getInstance().getConfig().getCompressor();
  }

  /**
   * Sets the encoding type for the measurement.
   *
   * @param encoding the encoding type (optional, defaults based on data type)
   * @return this builder instance for method chaining
   */
  public MeasurementSchemaBuilder withEncoding(TSEncoding encoding) {
    this.encoding = encoding;
    return this;
  }

  /**
   * Sets the compression type for the measurement.
   *
   * @param compressionType the compression type (optional, defaults to LZ4)
   * @return this builder instance for method chaining
   */
  public MeasurementSchemaBuilder withCompression(CompressionType compressionType) {
    this.compressionType = compressionType;
    return this;
  }

  /**
   * Adds a property to the measurement schema.
   *
   * @param key the property key (cannot be null)
   * @param value the property value (cannot be null)
   * @return this builder instance for method chaining
   * @throws IllegalArgumentException if key or value is null
   */
  public MeasurementSchemaBuilder withProperty(String key, String value) {
    if (key == null || value == null) {
      throw new IllegalArgumentException("Property key and value cannot be null");
    }
    if (this.props == null) {
      this.props = new HashMap<>();
    }
    this.props.put(key, value);
    return this;
  }

  /**
   * Sets multiple properties for the measurement schema.
   *
   * @param props a map of properties (can be null)
   * @return this builder instance for method chaining
   */
  public MeasurementSchemaBuilder withProperties(Map<String, String> props) {
    this.props = props;
    return this;
  }

  /**
   * Builds the final {@link MeasurementSchema} instance.
   *
   * @return a new MeasurementSchema configured with the builder's settings
   */
  public MeasurementSchema build() {
    return new MeasurementSchema(measurementName, dataType, encoding, compressionType, props);
  }
}
