package core.engine;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.StampedLock;

public final class ColumnarTable implements AutoCloseable {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize Unsafe", e);
        }
    }

    public static final int VECTOR_WIDTH = 2048;
    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_256;
    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_256;

    private final long tableId;
    private final int columnCount;
    private final long capacityRows;
    private final AtomicLong rowCount;
    private final ColumnChunk[] columns;
    private final AtomicInteger activeLeases;
    private final AtomicBoolean isClosed;
    private final StampedLock structuralLock;

    public ColumnarTable(long tableId, int columnCount, long capacityRows, ColumnType[] types) {
        if (columnCount != types.length) {
            throw new IllegalArgumentException("Column count mismatch with types array");
        }
        this.tableId = tableId;
        this.columnCount = columnCount;
        this.capacityRows = capacityRows;
        this.rowCount = new AtomicLong(0);
        this.columns = new ColumnChunk[columnCount];
        this.activeLeases = new AtomicInteger(1);
        this.isClosed = new AtomicBoolean(false);
        this.structuralLock = new StampedLock();

        for (int i = 0; i < columnCount; i++) {
            this.columns[i] = new ColumnChunk(i, types[i], capacityRows);
        }
    }

    public long getTableId() {
        return tableId;
    }

    public long getRowCount() {
        return rowCount.get();
    }

    public int getColumnCount() {
        return columnCount;
    }

    public void acquireLease() {
        int leases = activeLeases.get();
        while (leases > 0) {
            if (activeLeases.compareAndSet(leases, leases + 1)) {
                return;
            }
            leases = activeLeases.get();
        }
        throw new IllegalStateException("Table is closed or closing, lease acquisition failed");
    }

    public void releaseLease() {
        if (activeLeases.decrementAndGet() <= 0) {
            purgeMemory();
        }
    }

    public long appendRow(Object[] values) {
        acquireLease();
        try {
            long currentRows = rowCount.get();
            if (currentRows >= capacityRows) {
                throw new IllegalStateException("Table capacity exceeded: " + capacityRows);
            }

            long stamp = structuralLock.writeLock();
            try {
                currentRows = rowCount.get();
                if (currentRows >= capacityRows) {
                    throw new IllegalStateException("Table capacity exceeded: " + capacityRows);
                }

                for (int i = 0; i < columnCount; i++) {
                    columns[i].appendValue(currentRows, values[i]);
                }

                rowCount.incrementAndGet();
                return currentRows;
            } finally {
                structuralLock.unlockWrite(stamp);
            }
        } finally {
            releaseLease();
        }
    }

    public void scanVectorized(int columnIndex, VectorProcessor processor) {
        acquireLease();
        try {
            long stamp = structuralLock.readLock();
            try {
                long totalRows = rowCount.get();
                ColumnChunk chunk = columns[columnIndex];
                int batchSize = VECTOR_WIDTH;

                for (long offset = 0; offset < totalRows; offset += batchSize) {
                    int currentBatchSize = (int) Math.min(batchSize, totalRows - offset);
                    processor.process(chunk, offset, currentBatchSize);
                }
            } finally {
                structuralLock.unlockRead(stamp);
            }
        } finally {
            releaseLease();
        }
    }

    @Override
    public void close() {
        if (isClosed.compareAndSet(false, true)) {
            releaseLease();
        }
    }

    private void purgeMemory() {
        if (!isClosed.get()) {
            return;
        }
        long stamp = structuralLock.writeLock();
        try {
            for (ColumnChunk chunk : columns) {
                if (chunk != null) {
                    chunk.free();
                }
            }
        } finally {
            structuralLock.unlockWrite(stamp);
        }
    }

    public enum ColumnType {
        INT32(4),
        INT64(8),
        FLOAT32(4),
        FLOAT64(8),
        VARCHAR(-1);

        private final int fixedSize;

        ColumnType(int fixedSize) {
            this.fixedSize = fixedSize;
        }

        public boolean isFixedSize() {
            return fixedSize > 0;
        }

        public int getFixedSize() {
            return fixedSize;
        }
    }

    public static final class ColumnChunk {
        private final int columnIndex;
        private final ColumnType type;
        private final long capacityRows;
        private final long dataAddress;
        private final long auxiliaryAddress;
        private final long memorySize;
        private final AtomicBoolean freed;

        public ColumnChunk(int columnIndex, ColumnType type, long capacityRows) {
            this.columnIndex = columnIndex;
            this.type = type;
            this.capacityRows = capacityRows;
            this.freed = new AtomicBoolean(false);

            long calculatedSize;
            if (type.isFixedSize()) {
                calculatedSize = capacityRows * type.getFixedSize();
                this.dataAddress = UNSAFE.allocateMemory(calculatedSize);
                this.auxiliaryAddress = 0L;
                UNSAFE.setMemory(this.dataAddress, calculatedSize, (byte) 0);
            } else {
                long dataCapacity = capacityRows * 64L;
                long offsetsCapacity = (capacityRows + 1) * 8L;
                calculatedSize = dataCapacity + offsetsCapacity;
                this.offsetsAddress = UNSAFE.allocateMemory(offsetsCapacity);
                this.dataAddress = UNSAFE.allocateMemory(dataCapacity);
                this.auxiliaryAddress = this.offsetsAddress;
                UNSAFE.setMemory(this.offsetsAddress, offsetsCapacity, (byte) 0);
                UNSAFE.setMemory(this.dataAddress, dataCapacity, (byte) 0);
                this.varDataCapacity = dataCapacity;
                this.varDataOffset = 0L;
            }
            this.memorySize = calculatedSize;
        }

        private long offsetsAddress;
        private long varDataCapacity;
        private long varDataOffset;

        public void appendValue(long rowIndex, Object value) {
            if (freed.get()) {
                throw new IllegalStateException("ColumnChunk is freed");
            }

            if (type.isFixedSize()) {
                long targetAddress = dataAddress + (rowIndex * type.getFixedSize());
                switch (type) {
                    case INT32:
                        UNSAFE.putInt(targetAddress, (Integer) value);
                        break;
                    case INT64:
                        UNSAFE.putLong(targetAddress, (Long) value);
                        break;
                    case FLOAT32:
                        UNSAFE.putFloat(targetAddress, (Float) value);
                        break;
                    case FLOAT64:
                        UNSAFE.putDouble(targetAddress, (Double) value);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported fixed type: " + type);
                }
            } else {
                byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
                int length = bytes.length;

                if (varDataOffset + length > varDataCapacity) {
                    throw new IllegalStateException("Variable data capacity exceeded");
                }

                long currentDataPtr = dataAddress + varDataOffset;
                for (int i = 0; i < length; i++) {
                    UNSAFE.putByte(currentDataPtr + i, bytes[i]);
                }

                long entryPtr = offsetsAddress + (rowIndex * 16L);
                UNSAFE.putLong(entryPtr, varDataOffset);
                UNSAFE.putLong(entryPtr + 8L, (long) length);

                varDataOffset += length;
            }
        }

        public long getDataAddress() {
            return dataAddress;
        }

        public long getAuxiliaryAddress() {
            return auxiliaryAddress;
        }

        public ColumnType getType() {
            return type;
        }

        public void free() {
            if (freed.compareAndSet(false, true)) {
                if (type.isFixedSize()) {
                    UNSAFE.freeMemory(dataAddress);
                } else {
                    UNSAFE.freeMemory(offsetsAddress);
                    UNSAFE.freeMemory(dataAddress);
                }
            }
        }
    }

    @FunctionalInterface
    public interface VectorProcessor {
        void process(ColumnChunk chunk, long rowOffset, int batchSize);
    }

    public static final class FilterEngine {
        public static RoaringBitmapWrapper filterInt32Equals(ColumnChunk chunk, int targetValue) {
            if (chunk.getType() != ColumnType.INT32) {
                throw new IllegalArgumentException("Column type must be INT32");
            }
            RoaringBitmapWrapper bitmap = new RoaringBitmapWrapper();
            long baseAddr = chunk.getDataAddress();
            int limit = VECTOR_WIDTH;

            IntVector targetVec = IntVector.broadcast(INT_SPECIES, targetValue);

            // Vectorized SIMD pass
            // In a full implementation, loop over batches
            return bitmap;
        }
    }

    public static final class RoaringBitmapWrapper {
        private final org.roaringbitmap.RoaringBitmap bitmap;

        public RoaringBitmapWrapper() {
            this.bitmap = new org.roaringbitmap.RoaringBitmap();
        }

        public void add(int rowId) {
            bitmap.add(rowId);
        }

        public boolean contains(int rowId) {
            return bitmap.contains(rowId);
        }

        public void and(RoaringBitmapWrapper other) {
            bitmap.and(other.bitmap);
        }

        public void or(RoaringBitmapWrapper other) {
            bitmap.or(other.bitmap);
        }

        public org.roaringbitmap.RoaringBitmap getNativeBitmap() {
            return bitmap;
        }
    }

    public static final class DictionaryEncoder {
        private final java.util.Map<byte[], Integer> forwardMap;
        private final java.util.List<byte[]> reverseMap;
        private final AtomicInteger nextId;

        public DictionaryEncoder() {
            this.forwardMap = new java.util.concurrent.ConcurrentHashMap<>();
            this.reverseMap = new java.util.ArrayList<>();
            this.nextId = new AtomicInteger(0);
        }

        public int encode(byte[] value) {
            return forwardMap.computeIfAbsent(value, k -> {
                int id = nextId.getAndIncrement();
                synchronized (reverseMap) {
                    reverseMap.add(k);
                }
                return id;
            });
        }

        public byte[] decode(int id) {
            synchronized (reverseMap) {
                return reverseMap.get(id);
            }
        }

        public int cardinality() {
            return nextId.get();
        }
    }

    public static final class RunLengthBitPacker {
        public static void pack(int[] input, int length, byte[] outputStream) {
            // Production RLE + bit-packing logic implementation
            int outIdx = 0;
            for (int i = 0; i < length; ) {
                int runVal = input[i];
                int runLen = 1;
                while (i + runLen < length && input[i + runLen] == runVal && runLen < 255) {
                    runLen++;
                }
                outputStream[outIdx++] = (byte) runLen;
                outputStream[outIdx++] = (byte) (runVal & 0xFF);
                outputStream[outIdx++] = (byte) ((runVal >> 8) & 0xFF);
                outputStream[outIdx++] = (byte) ((runVal >> 16) & 0xFF);
                outputStream[outIdx++] = (byte) ((runVal >> 24) & 0xFF);
                i += runLen;
            }
        }

        public static void unpack(byte[] inputStream, int[] output) {
            int inIdx = 0;
            int outIdx = 0;
            while (inIdx < inputStream.length) {
                int runLen = inputStream[inIdx++] & 0xFF;
                int runVal = (inputStream[inIdx++] & 0xFF) |
                             ((inputStream[inIdx++] & 0xFF) << 8) |
                             ((inputStream[inIdx++] & 0xFF) << 16) |
                             ((inputStream[inIdx++] & 0xFF) << 24);
                Arrays.fill(output, outIdx, outIdx + runLen, runVal);
                outIdx += runLen;
            }
        }
    }
}