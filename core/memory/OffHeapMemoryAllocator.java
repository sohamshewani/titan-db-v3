package titan.core.memory;

import sun.misc.Unsafe;

import java.lang.ref.Cleaner;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * OffHeapMemoryAllocator coordinates off-heap memory allocations for titan-db.
 * Direct memory is managed via {@link sun.misc.Unsafe} with 64-byte cache line alignment
 * for high-throughput SIMD operations (AVX-512 / ARM Neon). Memory handles are tracked
 * with atomic arena leasing, strict lifecycle validation, double-free prevention,
 * and fatal leak-detection traps.
 */
public final class OffHeapMemoryAllocator implements AutoCloseable {

    private static final Unsafe UNSAFE;
    private static final int DEFAULT_ALIGNMENT = 64; // Cache-line alignment for AVX-512
    private static final long ADDRESS_SIZE_MASK = Long.BYTES - 1L;

    static {
        try {
            Field field = Unsafe.class.getDeclaredField("theUnsafe");
            field.setAccessible(true);
            UNSAFE = (Unsafe) field.get(null);
        } catch (Exception ex) {
            throw new ExceptionInInitializerError(
                new IllegalStateException("CRITICAL: Failed to access sun.misc.Unsafe for off-heap operations", ex)
            );
        }
    }

    private final long maxDirectMemory;
    private final boolean leakDetectionEnabled;
    private final AtomicLong allocatedBytes = new AtomicLong(0L);
    private final AtomicLong peakAllocatedBytes = new AtomicLong(0L);
    private final AtomicLong allocationCount = new AtomicLong(0L);
    private final ConcurrentHashMap<Long, AllocationMetadata> activeAllocations = new ConcurrentHashMap<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    private final Cleaner cleaner = Cleaner.create();

    /**
     * Initializes the allocator with a memory ceiling and leak detection configuration.
     *
     * @param maxDirectMemory      Maximum off-heap bytes allowed for this allocator.
     * @param leakDetectionEnabled If true, captures allocation-site stack traces.
     */
    public OffHeapMemoryAllocator(long maxDirectMemory, boolean leakDetectionEnabled) {
        if (maxDirectMemory <= 0) {
            throw new IllegalArgumentException("Max direct memory budget must be strictly positive: " + maxDirectMemory);
        }
        this.maxDirectMemory = maxDirectMemory;
        this.leakDetectionEnabled = leakDetectionEnabled;
    }

    public static Unsafe getUnsafe() {
        return UNSAFE;
    }

    public static int getDefaultAlignment() {
        return DEFAULT_ALIGNMENT;
    }

    /**
     * Allocates a contiguous off-heap memory region with default 64-byte SIMD alignment.
     *
     * @param bytes Number of payload bytes requested.
     * @return Aligned memory address pointer.
     */
    public long allocate(long bytes) {
        return allocate(bytes, DEFAULT_ALIGNMENT);
    }

    /**
     * Allocates a contiguous off-heap memory region aligned to the specified byte boundary.
     *
     * @param bytes     Number of payload bytes requested.
     * @param alignment Alignment boundary; must be a positive power of two.
     * @return Aligned memory address pointer.
     */
    public long allocate(long bytes, int alignment) {
        ensureOpen();
        validateAllocationParams(bytes, alignment);

        long currentAllocated = allocatedBytes.addAndGet(bytes);
        if (currentAllocated > maxDirectMemory) {
            allocatedBytes.addAndGet(-bytes);
            throw new OffHeapOutOfMemoryException(
                "Direct memory allocation exceeds budget. Requested: " + bytes +
                " bytes, Current: " + currentAllocated + " bytes, Limit: " + maxDirectMemory + " bytes"
            );
        }

        updatePeakMemory(currentAllocated);

        long rawAddress = 0L;
        long alignedAddress = 0L;
        try {
            // Allocate extra space: requested bytes + alignment padding + storage for raw pointer
            long totalAllocSize = bytes + alignment + Long.BYTES;
            rawAddress = UNSAFE.allocateMemory(totalAllocSize);

            // Clear allocated memory to avoid indeterminate data
            UNSAFE.setMemory(rawAddress, totalAllocSize, (byte) 0);

            long baseAfterHeader = rawAddress + Long.BYTES;
            alignedAddress = (baseAfterHeader + (alignment - 1L)) & ~(alignment - 1L);

            // Store raw pointer immediately before the aligned memory boundary
            UNSAFE.putLong(alignedAddress - Long.BYTES, rawAddress);

            StackTraceElement[] allocationTrace = leakDetectionEnabled
                ? Thread.currentThread().getStackTrace()
                : null;

            AllocationMetadata metadata = new AllocationMetadata(
                rawAddress,
                alignedAddress,
                bytes,
                totalAllocSize,
                alignment,
                allocationTrace
            );

            activeAllocations.put(alignedAddress, metadata);
            allocationCount.incrementAndGet();

            return alignedAddress;
        } catch (Throwable t) {
            if (rawAddress != 0L) {
                UNSAFE.freeMemory(rawAddress);
            }
            allocatedBytes.addAndGet(-bytes);
            throw new AllocationFailureException("Fatal hardware or OS memory allocation error for " + bytes + " bytes", t);
        }
    }

    /**
     * Reallocates an aligned off-heap memory buffer to a new capacity.
     *
     * @param alignedAddress Current aligned memory pointer.
     * @param newBytes       New payload capacity.
     * @return New aligned memory pointer.
     */
    public long reallocate(long alignedAddress, long newBytes) {
        ensureOpen();
        if (alignedAddress == 0L) {
            return allocate(newBytes);
        }

        AllocationMetadata metadata = activeAllocations.get(alignedAddress);
        if (metadata == null) {
            throw new InvalidMemoryAddressException("Cannot reallocate untracked address: 0x" + Long.toHexString(alignedAddress));
        }

        metadata.validateActiveState();

        int alignment = metadata.alignment;
        long oldBytes = metadata.requestedBytes;
        long newAddress = allocate(newBytes, alignment);

        long copyLength = Math.min(oldBytes, newBytes);
        UNSAFE.copyMemory(alignedAddress, newAddress, copyLength);

        free(alignedAddress);
        return newAddress;
    }

    /**
     * Decrements the lease reference count of the allocation. If count reaches zero, memory is freed.
     *
     * @param alignedAddress Aligned memory address pointer.
     */
    public void free(long alignedAddress) {
        if (alignedAddress == 0L) {
            return;
        }

        AllocationMetadata metadata = activeAllocations.get(alignedAddress);
        if (metadata == null) {
            throw new UseAfterFreeException("Memory double-free or invalid pointer release: 0x" + Long.toHexString(alignedAddress));
        }

        if (metadata.releaseLease()) {
            activeAllocations.remove(alignedAddress);
            UNSAFE.setMemory(metadata.rawAddress, metadata.totalAllocatedBytes, (byte) 0xDE); // Poison freed area
            UNSAFE.freeMemory(metadata.rawAddress);
            allocatedBytes.addAndGet(-metadata.requestedBytes);
        }
    }

    /**
     * Acquires an explicit arena lease, incrementing its atomic reference counter.
     *
     * @param alignedAddress Aligned memory address pointer.
     */
    public void acquireLease(long alignedAddress) {
        if (alignedAddress == 0L) {
            throw new IllegalArgumentException("Cannot acquire lease on NULL address");
        }
        AllocationMetadata metadata = activeAllocations.get(alignedAddress);
        if (metadata == null) {
            throw new UseAfterFreeException("Attempted to lease untracked/freed address: 0x" + Long.toHexString(alignedAddress));
        }
        metadata.acquireLease();
    }

    /**
     * Creates an RAII-governed memory arena wrapping an aligned off-heap allocation.
     *
     * @param bytes Requested byte size.
     * @return Managed ArenaLease instance.
     */
    public ArenaLease allocateArena(long bytes) {
        return allocateArena(bytes, DEFAULT_ALIGNMENT);
    }

    /**
     * Creates an RAII-governed memory arena wrapping an aligned off-heap allocation