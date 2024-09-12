package com.amazon.ion.benchmark;

import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ionpathextraction.PathExtractor;
import com.amazon.ionpathextraction.PathExtractorBuilder;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;

import static com.amazon.ionelement.api.ElementLoader.loadAllElements;

/**
 * A MeasurableReadTask for reading data in the Ion format (either text or binary).
 */
class IonElementMeasurableReadTask extends MeasurableReadTask {

    private static final int DEFAULT_INCREMENTAL_BUFFER_SIZE = 32 * 1024;
    private static final int DEFAULT_REUSABLE_LOB_BUFFER_SIZE = 1024;
    private final PathExtractor<?> pathExtractor;
    private final IonSystem ionSystem;
    private final byte[] reusableLobBuffer;
    private IonReaderBuilder readerBuilder;
    private SideEffectConsumer sideEffectConsumer = null;

    /**
     * Returns the next power of two greater than or equal to the given value.
     * @param value the start value.
     * @return the next power of two.
     */
    private static int nextPowerOfTwo(int value) {
        return (int) Math.pow(2, Math.ceil(Math.log10(value) / Math.log10(2)));
    }

    /**
     * @param inputPath the Ion data to read.
     * @param options the options to use when reading.
     * @throws IOException if thrown when handling the options.
     */
    IonElementMeasurableReadTask(Path inputPath, ReadOptionsCombination options) throws IOException {
        super(inputPath, options);
        ionSystem = IonUtilities.ionSystemForBenchmark(options);
        pathExtractor = null;
        if (options.useLobChunks) {
            reusableLobBuffer = new byte[DEFAULT_REUSABLE_LOB_BUFFER_SIZE];
        } else {
            reusableLobBuffer = null;
        }
    }

    @Override
    public void setUpTrial() throws IOException {
        super.setUpTrial();
        // Create the reader builder after any file conversion is done so that the buffer configuration can be
        // chosen with knowledge of the actual size of the data.
        readerBuilder = IonUtilities.newReaderBuilderForBenchmark(options).
            withIncrementalReadingEnabled(options.readerType == IonReaderType.INCREMENTAL);
        if (readerBuilder.isIncrementalReadingEnabled()) {
            if (options.initialBufferSize != null) {
                readerBuilder.withBufferConfiguration(
                    IonBufferConfiguration.Builder.standard()
                        .withInitialBufferSize(options.initialBufferSize)
                        .build()
                );
            } else {
                long inputSize = inputFile.length();
                if (inputSize < DEFAULT_INCREMENTAL_BUFFER_SIZE) {
                    readerBuilder.withBufferConfiguration(
                        IonBufferConfiguration.Builder.standard()
                            .withInitialBufferSize(nextPowerOfTwo((int) inputSize))
                            .build()
                    );
                }
            }
        }
    }

    @Override
    public void setUpIteration() {
        // Nothing to do.
    }

    @Override
    public void tearDownIteration() {
        // Nothing to do.
    }

    @Override
    void fullyTraverseFromBuffer(SideEffectConsumer consumer) throws IOException {
        throw new IllegalArgumentException("Not supported.");
    }

    @Override
    public void fullyTraverseFromFile(SideEffectConsumer consumer) throws IOException {
        throw new IllegalArgumentException("Not supported.");
    }

    @Override
    void traverseFromBuffer(List<String> paths, SideEffectConsumer consumer) throws IOException {
        sideEffectConsumer = consumer;
        IonReader reader = readerBuilder.build(buffer);
        pathExtractor.match(reader);
        reader.close();
    }

    @Override
    public void traverseFromFile(List<String> paths, SideEffectConsumer consumer) throws IOException {
        throw new IllegalArgumentException("Not supported.");
    }

    @Override
    public void fullyReadDomFromBuffer(SideEffectConsumer consumer) throws IOException {
        sideEffectConsumer = consumer;
        IonReader reader = readerBuilder.build(buffer);
        sideEffectConsumer.consume(loadAllElements(reader));
        reader.close();
    }

    @Override
    public void fullyReadDomFromFile(SideEffectConsumer consumer) throws IOException {
        sideEffectConsumer = consumer;
        IonReader reader = readerBuilder.build(options.newInputStream(inputFile));
        sideEffectConsumer.consume(loadAllElements(reader));
        reader.close();
    }
}
