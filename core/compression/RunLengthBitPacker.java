package core.compression;

import jdk.incubator.vector.ByteVector;
import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;
import sun.misc.Unsafe;

import java.lang.reflect.Field;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public final class RunLengthBitPacker {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new AssertionError("Failed to initialize sun.misc.Unsafe", e);
        }
    }

    private static final VectorSpecies<Integer> INT_SPECIES = IntVector.SPECIES_256;
    private static final VectorSpecies<Byte> BYTE_SPECIES = ByteVector.SPECIES_256;
    
    public static final int VECTOR_WIDTH = 2048;
    private static final int INT_BYTES = Integer.BYTES;

    public static final class RLEPackResult {
        private final long compressedAddress;
        private final long compressedSizeInBytes;
        private final int originalItemCount;

        public RLEPackResult(long compressedAddress, long compressedSizeInBytes, int originalItemCount) {
            this.compressedAddress = compressedAddress;
            this.compressedSizeInBytes = compressedSizeInBytes;
            this.originalItemCount = originalItemCount;
        }

        public long getCompressedAddress() {
            return compressedAddress;
        }

        public long getCompressedSizeInBytes() {
            return compressedSizeInBytes;
        }

        public int getOriginalItemCount() {
            return originalItemCount;
        }
    }

    public static RLEPackResult compress(long inputAddress, int itemCount) {
        if (itemCount <= 0) {
            throw new IllegalArgumentException("Item count must be greater than zero.");
        }
        if (inputAddress == 0L) {
            throw new IllegalArgumentException("Input address cannot be null/zero.");
        }

        long maxPossibleSize = 16L + (itemCount * (long) INT_BYTES * 2L);
        long compressedAddress = UNSAFE.allocateMemory(maxPossibleSize);
        
        long writeCursor = compressedAddress;
        
        UNSAFE.putInt(writeCursor, itemCount);
        writeCursor += 4;
        
        long runsCountOffset = writeCursor;
        UNSAFE.putInt(writeCursor, 0); 
        writeCursor += 4;
        
        int runCount = 0;
        int currentReadIndex = 0;

        while (currentReadIndex < itemCount) {
            int currentValue = UNSAFE.getInt(inputAddress + ((long) currentReadIndex * INT_BYTES));
            int runLength = 1;
            
            int nextIndex = currentReadIndex + 1;
            while (nextIndex < itemCount) {
                int nextVal = UNSAFE.getInt(inputAddress + ((long) nextIndex * INT_BYTES));
                if (nextVal == currentValue && runLength < Integer.MAX_VALUE) {
                    runLength++;
                    nextIndex++;
                } else {
                    break;
                }
            }

            UNSAFE.putInt(writeCursor, currentValue);
            writeCursor += INT_BYTES;
            UNSAFE.putInt(writeCursor, runLength);
            writeCursor += INT_BYTES;
            
            runCount++;
            currentReadIndex = nextIndex;
        }

        UNSAFE.putInt(runsCountOffset, runCount);

        long finalSize = writeCursor - compressedAddress;
        return new RLEPackResult(compressedAddress, finalSize, itemCount);
    }

    public static void decompress(long compressedAddress, long outputAddress) {
        if (compressedAddress == 0L || outputAddress == 0L) {
            throw new IllegalArgumentException("Addresses must not be zero.");
        }

        long readCursor = compressedAddress;
        int originalItemCount = UNSAFE.getInt(readCursor);
        readCursor += 4;

        int runCount = UNSAFE.getInt(readCursor);
        readCursor += 4;

        long writeCursor = outputAddress;
        int itemsWritten = 0;

        for (int i = 0; i < runCount; i++) {
            int value = UNSAFE.getInt(readCursor);
            readCursor += INT_BYTES;
            int length = UNSAFE.getInt(readCursor);
            readCursor += INT_BYTES;

            itemsWritten += length;
            if (itemsWritten > originalItemCount) {
                throw new IllegalStateException("Decompression run-length overflowed original item count bound.");
            }

            int vectorizedLimit = length & ~(INT_SPECIES.length() - 1);
            IntVector valVec = IntVector.broadcast(INT_SPECIES, value);

            int j = 0;
            for (; j < vectorizedLimit; j += INT_SPECIES.length()) {
                valVec.intoMemory(UNSAFE, writeCursor + ((long) j * INT_BYTES), ByteOrder.nativeOrder());
            }

            for (; j < length; j++) {
                UNSAFE.putInt(writeCursor + ((long) j * INT_BYTES), value);
            }

            writeCursor += (long) length * INT_BYTES;
        }

        if (itemsWritten != originalItemCount) {
            throw new IllegalStateException("Decompression mismatch: written items do not match original item count.");
        }
    }

    public static void free(RLEPackResult result) {
        if (result != null && result.getCompressedAddress() != 0L) {
            UNSAFE.freeMemory(result.getCompressedAddress());
        }
    }
}