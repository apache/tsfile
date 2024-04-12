package org.apache.tsfile.compatibility;

import java.io.IOException;
import java.io.InputStream;

public interface StreamDeserializer<T> {
  T deserialize(InputStream inputStream, DeserializeConfig context) throws IOException;
}
