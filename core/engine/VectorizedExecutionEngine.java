package core.engine;

import jdk.incubator.vector.DoubleVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;
import sun.misc.Unsafe;

import java.lang.foreign.MemorySegment;
import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

public final class VectorizedExecutionEngine {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize sun.misc.Unsafe", e);
        }
    }

    public static final int VECTOR_SIZE = 2048;
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_PREFERRED;
    private static final VectorSpecies<Double> DOUBLE_SPECIES = DoubleVector.SPECIES_PREFERRED;

    public enum OpType {
        EQUALS,
        NOT_EQUALS,
        LESS_THAN,
        GREATER_THAN,
        ADD,
        SUBTRACT,
        MULTIPLY
    }

    public static final class VectorBatch {
        private final long address;
        private final long selectionVectorAddress;
        private int size;
        private final int capacity;
        private final AtomicLong referenceCount;
        private final AtomicBoolean released;

        public VectorBatch(int capacity) {
            this.capacity = capacity;
            this.size = 0;
            long bytesNeeded = (long) capacity * 8L; // 8 bytes per primitive slot (long/double)
            this.address = UNSAFE.allocateMemory(bytesNeeded);
            this.selectionVectorAddress = UNSAFE.allocateMemory((long) capacity * 4L); // 4 bytes per selection index
            UNSAFE.setMemory(this.address, bytesNeeded, (byte) 0);
            UNSAFE.setMemory(this.selectionVectorAddress, (long) capacity * 4L, (byte) 0);
            this.referenceCount = new AtomicLong(1L);
            this.released = new AtomicBoolean(false);
        }

        public void retain() {
            long current;
            do {
                current = referenceCount.get();
                if (current <= 0) {
                    throw new IllegalStateException("Attempted to retain a released VectorBatch.");
                }
            } while (!referenceCount.compareAndSet(current, current + 1));
        }

        public void release() {
            long current;
            do {
                current = referenceCount.get();
                if (current <= 0) {
                    throw new IllegalStateException("Attempted to release an already released VectorBatch.");
                }
            } while (!referenceCount.compareAndSet(current, current - 1));

            if (referenceCount.get() == 0 && released.compareAndSet(false, true)) {
                UNSAFE.freeMemory(address);
                UNSAFE.freeMemory(selectionVectorAddress);
            }
        }

        public long getAddress() {
            return address;
        }

        public long getSelectionVectorAddress() {
            return selectionVectorAddress;
        }

        public int getSize() {
            return size;
        }

        public void setSize(int size) {
            if (size > capacity) {
                throw new IllegalArgumentException("Batch size exceeds capacity: " + size + " > " + capacity);
            }
            this.size = size;
        }

        public int getCapacity() {
            return capacity;
        }
    }

    public static final class ExecutionContext {
        private final long arenaBaseAddress;
        private final long arenaSize;
        private final AtomicLong currentEpoch;

        ecz:
        public ExecutionContext(long arenaBaseAddress, long arenaSize, AtomicLong currentEpoch) {
            this.arenaBaseAddress = arenaBaseAddress;
            this.arenaSize = arenaSize;
            this.currentEpoch = currentEpoch;
        }

        public long getArenaBaseAddress() {
            return arenaBaseAddress;
        }

        public long getArenaSize() {
            return arenaSize;
        }

        public long getCurrentEpoch() {
            return currentEpoch.get();
        }
    }

    public interface Operator {
        void open(ExecutionContext context);
        boolean next(VectorBatch outputBatch);
        void close();
    }

    public static final class ColumnScanOperator implements Operator {
        private final long columnChunkAddress;
        private final long totalRows;
        private long currentRow;
        private ExecutionContext context;

        public ColumnScanOperator(long columnChunkAddress, long totalRows) {
            this.columnChunkAddress = columnChunkAddress;
            this.totalRows = totalRows;
            this.currentRow = 0;
        }

        @Override
        public void open(ExecutionContext context) {
            this.context = context;
            this.currentRow = 0;
        }

        @Override
        public boolean next(VectorBatch outputBatch) {
            if (currentRow >= totalRows) {
                outputBatch.setSize(0);
                return false;
            }

            int rowsToProcess = (int) Math.min(VECTOR_SIZE, totalRows - currentRow);
            long sourceOffset = columnChunkAddress + (currentRow * 8L);
            long destAddress = outputBatch.getAddress();

            // Direct off-heap block copy for DSM columnar chunk layout
            UNSAFE.copyMemory(sourceOffset, destAddress, (long) rowsToProcess * 8L);

            // Initialize sequential selection vector
            long selAddr = outputBatch.getSelectionVectorAddress();
            for (int i = 0; i < rowsToProcess; i++) {
                UNSAFE.putInt(selAddr + ((long) i * 4L), i);
            }

            outputBatch.setSize(rowsToProcess);
            currentRow += rowsToProcess;
            return true;
        }

        @Override
        public void close() {
            // No-op for direct scans
        }
    }

    public static final class FilterOperator implements Operator {
        private final Operator child;
        private final OpType opType;
        private final long literalValue;

        public FilterOperator(Operator child, OpType opType, long literalValue) {
            this.child = child;
            this.opType = opType;
            this.literalValue = literalValue;
        }

        @Override
        public void open(ExecutionContext context) {
            child.open(context);
        }

        @Override
        public boolean next(VectorBatch outputBatch) {
            while (child.next(outputBatch)) {
                int size = outputBatch.getSize();
                if (size == 0) {
                    continue;
                }

                long dataAddr = outputBatch.getAddress();
                long selAddr = outputBatch.getSelectionVectorAddress();
                int passingCount = 0;

                int upperBound = INT_SPECIES.loopBound(size);
                int i = 0;

                // SIMD Vectorized Evaluation using Panama Vector API where applicable
                if (INT_SPECIES.vectorBitSize() >= 128 && size >= INT_SPECIES.length()) {
                    for (; i < upperBound; i += INT_SPECIES.length()) {
                        IntVector vec = IntVector.fromMemory(INT_SPECIES, dataAddr, (long) i * 4L, ByteOrder.nativeOrder());
                        jdk.incubator.vector.VectorMask<Integer> mask;

                        switch (opType) {
                            case EQUALS:
                                mask = vec.eq((int) literalValue);
                                break;
                            case NOT_EQUALS:
                                mask = vec.eq((int) literalValue).not();
                                break;
                            case LESS_THAN:
                                mask = vec.lt((int) literalValue);
                                break;
                            case GREATER_THAN:
                                mask = vec.gt((int) literalValue);
                                break;
                            default:
                                throw new UnsupportedOperationException("Unsupported filter operation for vectorization: " + opType);
                        }

                        for (int j = 0; j < INT_SPECIES.length(); j++) {
                            if (mask.laneIsSet(j)) {
                                UNSAFE.putInt(selAddr + ((long) passingCount * 4L), i + j);
                                passingCount++;
                            }
                        }
                    }
                }

                // Scalar tail fallback and fallback for non-vectorized widths
                for (; i < size; i++) {
                    long val = UNSAFE.getLong(dataAddr + ((long) i * 8L));
                    boolean passes = false;
                    switch (opType) {
                        case EQUALS:
                            passes = (val == literalValue);
                            break;
                        case NOT_EQUALS:
                            passes = (val != literalValue);
                            break;
                        case LESS_THAN:
                            passes = (val < literalValue);
                            break;
                        case GREATER_THAN:
                            passes = (val > literalValue);
                            break;
                    }
                    if (passes) {
                        UNSAFE.putInt(selAddr + ((long) passingCount * 4L), i);
                        passingCount++;
                    }
                }

                outputBatch.setSize(passingCount);
                if (passingCount > 0) {
                    return true;
                }
            }
            outputBatch.setSize(0);
            return false;
        }

        @Override
        public void close() {
            child.close();
        }
    }

    public static final class ArithmeticOperator implements Operator {
        private final Operator child;
        private final OpType opType;
        private final double literalOperand;

        public ArithmeticOperator(Operator child, OpType opType, double literalOperand) {
            this.child = child;
            this.opType = opType;
            this.literalOperand = literalOperand;
        }

        @Override
        public void open(ExecutionContext context) {
            child.open(context);
        }

        @Override
        public boolean next(VectorBatch outputBatch) {
            if (!child.next(outputBatch)) {
                return false;
            }

            int size = outputBatch.getSize();
            if (size == 0) {
                return true;
            }

            long dataAddr = outputBatch.getAddress();
            int upperBound = DOUBLE_SPECIES.loopBound(size);
            int i = 0;

            if (DOUBLE_SPECIES.vectorBitSize() >= 128 && size >= DOUBLE_SPECIES.length()) {
                for (; i < upperBound; i += DOUBLE_SPECIES.length()) {
                    DoubleVector vec = DoubleVector.fromMemory(DOUBLE_SPECIES, dataAddr, (long) i * 8L, ByteOrder.nativeOrder());
                    DoubleVector res;

                    switch (opType) {
                        case ADD:
                            res = vec.add(literalOperand);
                            break;
                        case SUBTRACT:
                            res = vec.sub(literalOperand);
                            break;
                        case MULTIPLY:
                            res = vec.mul(literalOperand);
                            break;
                        default:
                            throw new UnsupportedOperationException("Unsupported arithmetic operation: " + opType);
                    }
                    res.intoMemory(dataAddr, (long) i * 8L, ByteOrder.nativeOrder());
                }
            }

            for (; i < size; i++) {
                double val = UNSAFE.getDouble(dataAddr + ((long) i * 8L));
                double res = val;
                switch (opType) {
                    case ADD:
                        res = val + literalOperand;
                        break;
                    case SUBTRACT:
                        res = val - literalOperand;
                        break;
                    case MULTIPLY:
                        res = val * literalOperand;
                        break;
                }
                UNSAFE.putDouble(dataAddr + ((long) i * 8L), res);
            }

            outputBatch.setSize(size);
            return true;
        }

        @Override
        public void close() {
            child.close();
        }
    }

    public static final class AggregateOperator implements Operator {
        private final Operator child;
        private double globalSum;
        private long globalCount;
        private boolean executed;

        public AggregateOperator(Operator child) {
            this.child = child;
            this.globalSum = 0.0;
            this.globalCount = 0L;
            this.executed = false;
        }

        @Override
        public void open(ExecutionContext context) {
            child.open(context);
            this.globalSum = 0.0;
            this.globalCount = 0L;
            this.executed = false;
        }

        @Override
        public boolean next(VectorBatch outputBatch) {
            if (executed) {
                outputBatch.setSize(0);
                return false;
            }

            VectorBatch batch = new VectorBatch(VECTOR_SIZE);
            try {
                while (child.next(batch)) {
                    int size = batch.getSize();
                    if (size == 0) {
                        continue;
                    }

                    long dataAddr = batch.getAddress();
                    long selAddr = batch.getSelectionVectorAddress();

                    int upperBound = DOUBLE_SPECIES.loopBound(size);
                    int i = 0;

                    // If selection vector is dense or sparse, handle safely via selection indices
                    for (int j = 0; j < size; j++) {
                        int idx = UNSAFE.getInt(selAddr + ((long) j * 4L));
                        double val = UNSAFE.getDouble(dataAddr + ((long) idx * 8L));
                        globalSum += val;
                        globalCount++;
                    }
                }
            } finally {
                batch.release();
            }

            // Write aggregation result into output batch
            long outAddr = outputBatch.getAddress();
            UNSAFE.putDouble(outAddr, globalSum);
            UNSAFE.putDouble(outAddr + 8L, (double) globalCount);
            outputBatch.setSize(1);
            executed = true;
            return true;
        }

        @Override
        public void close() {
            child.close();
        }

        public double getGlobalSum() {
            return globalSum;
        }

        public long getGlobalCount() {
            return globalCount;
        }
    }

    public static final class PipelineExecutor {
        public static void executePipeline(Operator root, ExecutionContext context, VectorBatch batchCollector) {
            root.open(context);
            try {
                while (root.next(batchCollector)) {
                    // Pipeline sink exhaustion / consumption hook
                    int batchSize = batchCollector.getSize();
                    if (batchSize == 0) {
                        break;
                    }
                }
            } finally {
                root.close();
            }
        }
    }
}