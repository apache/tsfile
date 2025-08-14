package org.apache.tsfile.write.v4;

import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.write.TsFileWriter;

import java.io.File;
import java.io.IOException;

/**
 * Builder class for TsFileTreeWriter to provide a fluent interface for configuration.
 */
public class TsFileTreeWriterBuilder {

    private File file;
    private int memoryThreshold = TSFileDescriptor.getInstance().getConfig().getGroupSizeInByte();

    /**
     * Sets the output file for the TsFileTreeWriter.
     *
     * @param file the output file
     * @return this builder instance
     */
    public TsFileTreeWriterBuilder file(File file) {
        this.file = file;
        return this;
    }

    /**
     * Sets the memory threshold for flushing data.
     *
     * @param memoryThreshold the threshold in bytes
     * @return this builder instance
     */
    public TsFileTreeWriterBuilder memoryThreshold(int memoryThreshold) {
        this.memoryThreshold = memoryThreshold;
        return this;
    }

    /**
     * Builds and returns a new TsFileTreeWriter instance with the configured settings.
     *
     * @return a new TsFileTreeWriter instance
     * @throws IOException if the TsFileWriter cannot be initialized
     */
    public TsFileTreeWriter build() throws IOException {
        if (file == null) {
            throw new IllegalArgumentException("Output file must be specified");
        }

        return new TsFileTreeWriter(file, memoryThreshold);
    }
}