package com.amazon.ion.benchmark;

import com.amazon.ion.IonBufferConfiguration;
import com.amazon.ion.IonReader;
import com.amazon.ion.IonSystem;
import com.amazon.ion.IonType;
import com.amazon.ion.system.IonReaderBuilder;
import com.amazon.ionelement.api.IonElementLoaderOptions;
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
class IonMeasurableReadTask extends MeasurableReadTask {

    /*
    Improved IonElement
    Benchmark                         (input)                             (options)  Mode  Cnt      Score     Error   Units
    Bench.run                     scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5      0.025 ±   0.001   ms/op
    Bench.run:Heap usage          scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5    112.387 ± 238.582      MB
    Bench.run:Serialized size     scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5      0.003                MB
    Bench.run:gc.alloc.rate       scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5   1138.215 ±   6.302  MB/sec
    Bench.run:gc.alloc.rate.norm  scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5  30224.016 ±   0.001    B/op
    Bench.run:gc.count            scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5    383.000            counts
    Bench.run:gc.time             scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5    139.000                ms

    ION_VALUE=true
    Benchmark                         (input)                             (options)  Mode  Cnt      Score     Error   Units
    Bench.run                     scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5      0.027 ±   0.001   ms/op
    Bench.run:Heap usage          scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5     48.189 ± 175.625      MB
    Bench.run:Serialized size     scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5      0.003                MB
    Bench.run:gc.alloc.rate       scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5   1062.918 ±  19.110  MB/sec
    Bench.run:gc.alloc.rate.norm  scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5  30224.017 ±   0.001    B/op
    Bench.run:gc.count            scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5    361.000            counts
    Bench.run:gc.time             scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5    132.000                ms

    ION_VALUE=false
    Benchmark                         (input)                             (options)  Mode  Cnt      Score     Error   Units
    Bench.run                     scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5      0.025 ±   0.001   ms/op
    Bench.run:Heap usage          scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5    104.392 ± 163.690      MB
    Bench.run:Serialized size     scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5      0.003                MB
    Bench.run:gc.alloc.rate       scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5   1130.617 ±  24.387  MB/sec
    Bench.run:gc.alloc.rate.norm  scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5  30224.016 ±   0.001    B/op
    Bench.run:gc.count            scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5    378.000            counts
    Bench.run:gc.time             scratch.ion  write::{f:ION_BINARY,t:BUFFER,a:DOM}  avgt    5    139.000                ms


    ION_VALUE=true
    Benchmark                         (input)                                          (options)  Mode  Cnt      Score     Error   Units
    Bench.run                     scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5      0.053 ±   0.001   ms/op
    Bench.run:Heap usage          scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5     54.946 ± 139.239      MB
    Bench.run:Serialized size     scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5      0.003                MB
    Bench.run:gc.alloc.rate       scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5   1469.141 ±  12.745  MB/sec
    Bench.run:gc.alloc.rate.norm  scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5  81128.034 ±   0.004    B/op
    Bench.run:gc.count            scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5    479.000            counts
    Bench.run:gc.time             scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5    188.000                ms

    ION_VALUE=false
    Benchmark                         (input)                                          (options)  Mode  Cnt       Score     Error   Units
    Bench.run                     scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5       0.047 ±   0.001   ms/op
    Bench.run:Heap usage          scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5     105.350 ± 182.961      MB
    Bench.run:Serialized size     scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5       0.003                MB
    Bench.run:gc.alloc.rate       scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5    4404.971 ±  36.098  MB/sec
    Bench.run:gc.alloc.rate.norm  scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5  215416.030 ±   0.004    B/op
    Bench.run:gc.count            scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5     890.000            counts
    Bench.run:gc.time             scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5     465.000                ms

    ION_VALUE=false, no persistent collections
    Benchmark                         (input)                                          (options)  Mode  Cnt      Score     Error   Units
    Bench.run                     scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5      0.021 ±   0.001   ms/op
    Bench.run:Heap usage          scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5     62.110 ± 174.478      MB
    Bench.run:Serialized size     scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5      0.003                MB
    Bench.run:gc.alloc.rate       scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5   1635.635 ±  43.221  MB/sec
    Bench.run:gc.alloc.rate.norm  scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5  35200.013 ±   0.002    B/op
    Bench.run:gc.count            scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5    527.000            counts
    Bench.run:gc.time             scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5    214.000                ms

    ION_VALUE=false, loadElements improvement
    Benchmark                         (input)                                          (options)  Mode  Cnt       Score     Error   Units
    Bench.run                     scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5       0.032 ±   0.001   ms/op
    Bench.run:Heap usage          scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5     153.581 ± 349.585      MB
    Bench.run:Serialized size     scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5       0.003                MB
    Bench.run:gc.alloc.rate       scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5    3911.425 ± 152.536  MB/sec
    Bench.run:gc.alloc.rate.norm  scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5  131720.020 ±   0.003    B/op
    Bench.run:gc.count            scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5     984.000            counts
    Bench.run:gc.time             scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5     460.000                ms

    Benchmark                         (input)                                          (options)  Mode  Cnt       Score     Error   Units
    Bench.run                     scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5       0.028 ±   0.001   ms/op
    Bench.run:Heap usage          scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5     176.234 ± 321.512      MB
    Bench.run:Serialized size     scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5       0.003                MB
    Bench.run:gc.alloc.rate       scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5    4612.285 ± 174.708  MB/sec
    Bench.run:gc.alloc.rate.norm  scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5  133856.018 ±   0.002    B/op
    Bench.run:gc.count            scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5     849.000            counts
    Bench.run:gc.time             scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5     487.000                ms


    LWIL
    Benchmark                         (input)                                          (options)  Mode  Cnt      Score     Error   Units
    Bench.run                     scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5      0.021 ±   0.001   ms/op
    Bench.run:Heap usage          scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5     91.467 ± 260.288      MB
    Bench.run:Serialized size     scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5      0.003                MB
    Bench.run:gc.alloc.rate       scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5   1946.086 ±  15.589  MB/sec
    Bench.run:gc.alloc.rate.norm  scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5  42632.013 ±   0.001    B/op
    Bench.run:gc.count            scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5    616.000            counts
    Bench.run:gc.time             scratch.ion  read::{f:ION_BINARY,t:BUFFER,a:DOM,R:INCREMENTAL}  avgt    5    246.000                ms

scratch.ion (~3kB)
API       | Time                    | gc.alloc.rate.norm
----------|-------------------------|-----------------------------
IonValue  | 0.053 ±   0.001   ms/op |  81128.034 ±   0.004    B/op
Element0  | 0.047 ±   0.001   ms/op | 215416.030 ±   0.004    B/op
Element1  | 0.032 ±   0.001   ms/op | 131720.020 ±   0.003    B/op
Element2  | 0.030 ±   0.001   ms/op |  68432.019 ±   0.002    B/op
IonReader | 0.016 ±   0.001   ms/op |  11704.010 ±   0.001    B/op

service_log_legacy_small.ion (~5kB)

API            | Time                | gc.alloc.rate.norm
---------------|---------------------|------------------------
IonValue       | 0.034 ± 0.001 ms/op |  65816.022 ± 0.003 B/op
IonElement     | 0.027 ± 0.001 ms/op | 131480.017 ± 0.002 B/op
New IonElement | 0.016 ± 0.001 ms/op |  47032.010 ± 0.001 B/op
IonReader      | 0.013 ± 0.001 ms/op |  18184.008 ± 0.001 B/op


service_log_legacy.10n (~20MB)

API            | Time                    | gc.alloc.rate.norm
---------------|-------------------------|------------------------------
IonValue       | 635.450 ±  75.384 ms/op |  625408257.218 ±  49.211 B/op
IonElement     | 630.640 ± 215.099 ms/op | 2348935671.047 ± 146.942 B/op
New IonElement | 316.404 ±  39.124 ms/op |  614521309.423 ±  26.846 B/op
IonReader      | 119.898 ±   0.395 ms/op |   12445498.990 ±   2.225 B/op

tiny.ion
API            | Time                | gc.alloc.rate.norm
---------------|---------------------|------------------------------
IonValue       | 0.003 ± 0.001 ms/op |  7832.002 ± 0.001 B/op
IonElement     | 0.002 ± 0.001 ms/op | 11176.002 ± 0.001 B/op
New IonElement | 0.002 ± 0.001 ms/op |  5144.001 ± 0.001 B/op
IonReader      | 0.xxx ± 0.001 ms/op |     x.990 ± 2.xxx B/op

json_isl.ion
API            | Time                | gc.alloc.rate.norm
---------------|---------------------|------------------------------
IonValue       | 0.006 ± 0.001 ms/op | 12848.004 ± 0.001 B/op
IonElement     | 0.006 ± 0.001 ms/op | 25176.004 ± 0.001 B/op
New IonElement | 0.004 ± 0.001 ms/op |  8672.002 ± 0.001 B/op
IonReader      | x.xxx ± 0.001 ms/op |     x.990 ± 2.225 B/op


----









     */

    private static final boolean ION_VALUE = false;

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
     * Callback function for path extractor matches. Fully consumes the current value.
     * @param reader the reader positioned at the match.
     * @return 0, meaning that the reader should not step out of the current container after a match.
     */
    private int pathExtractorCallback(IonReader reader) {
        consumeCurrentValue(reader, reader.isInStruct());
        return 0;
    }

    /**
     * @param inputPath the Ion data to read.
     * @param options the options to use when reading.
     * @throws IOException if thrown when handling the options.
     */
    IonMeasurableReadTask(Path inputPath, ReadOptionsCombination options) throws IOException {
        super(inputPath, options);
        ionSystem = IonUtilities.ionSystemForBenchmark(options);
        if (options.paths != null) {
            PathExtractorBuilder<?> pathExtractorBuilder = PathExtractorBuilder.standard();
            for (String path : options.paths) {
                pathExtractorBuilder.withSearchPath(path, this::pathExtractorCallback);
            }
            pathExtractor = pathExtractorBuilder.build();
        } else {
            pathExtractor = null;
        }
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

    private void consumeCurrentValue(IonReader reader, boolean isInStruct) {
        if (isInStruct) {
            if (options.useSymbolTokens) {
                sideEffectConsumer.consume(reader.getFieldNameSymbol());
            } else {
                sideEffectConsumer.consume(reader.getFieldName());
            }
        }
        if (options.useSymbolTokens) {
            sideEffectConsumer.consume(reader.getTypeAnnotationSymbols());
        } else {
            Iterator<String> annotationsIterator = reader.iterateTypeAnnotations();
            while (annotationsIterator.hasNext()) {
                sideEffectConsumer.consume(annotationsIterator.next());
            }
        }
        IonType type = reader.getType();
        if (!reader.isNullValue()) {
            switch (type) {
                case BOOL:
                    sideEffectConsumer.consume(reader.booleanValue());
                    break;
                case INT:
                    switch (reader.getIntegerSize()) {
                        case INT:
                            sideEffectConsumer.consume(reader.intValue());
                            break;
                        case LONG:
                            sideEffectConsumer.consume(reader.longValue());
                            break;
                        case BIG_INTEGER:
                            sideEffectConsumer.consume(reader.bigIntegerValue());
                            break;
                    }
                    break;
                case FLOAT:
                    sideEffectConsumer.consume(reader.doubleValue());
                    break;
                case DECIMAL:
                    if (options.ionUseBigDecimals) {
                        sideEffectConsumer.consume(reader.bigDecimalValue());
                    } else {
                        sideEffectConsumer.consume(reader.decimalValue());
                    }
                    break;
                case TIMESTAMP:
                    sideEffectConsumer.consume(reader.timestampValue());
                    break;
                case SYMBOL:
                    if (options.useSymbolTokens) {
                        sideEffectConsumer.consume(reader.symbolValue());
                    } else {
                        sideEffectConsumer.consume(reader.stringValue());
                    }
                    break;
                case STRING:
                    sideEffectConsumer.consume(reader.stringValue());
                    break;
                case CLOB:
                case BLOB:
                    if (options.useLobChunks) {
                        int bytesRemaining = reader.byteSize();
                        while (bytesRemaining > 0) {
                            bytesRemaining -= reader.getBytes(
                                reusableLobBuffer,
                                0,
                                Math.min(bytesRemaining, reusableLobBuffer.length)
                            );
                        }
                        sideEffectConsumer.consume(reusableLobBuffer[0]);
                    } else {
                        sideEffectConsumer.consume(reader.newBytes());
                    }
                    break;
                case LIST:
                case SEXP:
                    reader.stepIn();
                    fullyTraverse(reader, false);
                    reader.stepOut();
                    break;
                case STRUCT:
                    reader.stepIn();
                    fullyTraverse(reader, true);
                    reader.stepOut();
                    break;
                default:
                    break;
            }
        }
    }

    private void fullyTraverse(IonReader reader, boolean isInStruct) {
        while (reader.next() != null) {
            consumeCurrentValue(reader, isInStruct);
        }
    }


    @Override
    void fullyTraverseFromBuffer(SideEffectConsumer consumer) throws IOException {
        sideEffectConsumer = consumer;
        IonReader reader = readerBuilder.build(buffer);
        fullyTraverse(reader, false);
        reader.close();
    }

    @Override
    public void fullyTraverseFromFile(SideEffectConsumer consumer) throws IOException {
        sideEffectConsumer = consumer;
        IonReader reader = readerBuilder.build(options.newInputStream(inputFile));
        fullyTraverse(reader, false);
        reader.close();
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
        sideEffectConsumer = consumer;
        IonReader reader = readerBuilder.build(options.newInputStream(inputFile));
        pathExtractor.match(reader);
        reader.close();
    }

    @Override
    public void fullyReadDomFromBuffer(SideEffectConsumer consumer) throws IOException {
        sideEffectConsumer = consumer;
        IonReader reader = readerBuilder.build(buffer);
        if (ION_VALUE) {
            ionSystem.newLoader().load(reader);
        } else {
            loadAllElements(reader, new IonElementLoaderOptions(false));
        }
        reader.close();
    }

    @Override
    public void fullyReadDomFromFile(SideEffectConsumer consumer) throws IOException {
        sideEffectConsumer = consumer;
        IonReader reader = readerBuilder.build(options.newInputStream(inputFile));
        if (ION_VALUE) {
            ionSystem.newLoader().load(reader);
        } else {
            loadAllElements(reader, new IonElementLoaderOptions(false));
        }
        reader.close();
    }
}
