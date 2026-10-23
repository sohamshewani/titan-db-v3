package core.index;

import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.util.Arrays;

public final class RoaringBitmapIndex {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize Unsafe", e);
        }
    }

    private static final short CONTAINER_TYPE_ARRAY = 0;
    private static final short CONTAINER_TYPE_BITMAP = 1;
    private static final short CONTAINER_TYPE_RUN = 2;

    private static final int DEFAULT_KEY_CAPACITY = 8;
    private static final int CAPACITY_GROWTH_FACTOR = 2;

    private long keysAddress;
    private long containersAddress;
    private int size;
    private int capacity;
    private boolean isFreed;

    public RoaringBitmapIndex() {
        this.capacity = DEFAULT_KEY_CAPACITY;
        this.size = 0;
        this.isFreed = false;
        this.keysAddress = UNSAFE.allocateMemory((long) capacity * 2); // 2 bytes per key (high 16 bits)
        this.containersAddress = UNSAFE.allocateMemory((long) capacity * 8); // 8 bytes per container pointer (off-heap address)
        
        // Zero-initialize container pointers
        for (int i = 0; i < capacity; i++) {
            UNSAFE.putLong(containersAddress + ((long) i * 8), 0L);
        }
    }

    private RoaringBitmapIndex(long keysAddress, long containersAddress, int size, int capacity) {
        this.keysAddress = keysAddress;
        this.containersAddress = containersAddress;
        this.size = size;
        this.capacity = capacity;
        this.isFreed = false;
    }

    public synchronized void add(int value) {
        if (isFreed) {
            throw new IllegalStateException("RoaringBitmapIndex has been freed.");
        }
        char highKey = (char) (value >>> 16);
        char lowKey = (char) (value & 0xFFFF);

        int index = binarySearchKeys(highKey);
        long containerAddr;

        if (index >= 0) {
            containerAddr = UNSAFE.getLong(containersAddress + ((long) index * 8));
        } else {
            int insertionPoint = -index - 1;
            ensureCapacity(size + 1);

            // Shift keys and containers right
            if (insertionPoint < size) {
                long srcKeys = keysAddress + ((long) insertionPoint * 2);
                long dstKeys = keysAddress + ((long) (insertionPoint + 1) * 2);
                UNSAFE.copyMemory(srcKeys, dstKeys, (long) (size - insertionPoint) * 2);

                long srcCont = containersAddress + ((long) insertionPoint * 8);
                long dstCont = containersAddress + ((long) (insertionPoint + 1) * 8);
                UNSAFE.copyMemory(srcCont, dstCont, (long) (size - insertionPoint) * 8);
            }

            UNSAFE.putChar(keysAddress + ((long) insertionPoint * 2), highKey);
            containerAddr = createArrayContainer();
            UNSAFE.putLong(containersAddress + ((long) insertionPoint * 8), containerAddr);
            size++;
            index = insertionPoint;
        }

        containerAddr = addLowValue(containerAddr, lowKey);
        UNSAFE.putLong(containersAddress + ((long) index * 8), containerAddr);
    }

    public synchronized boolean contains(int value) {
        if (isFreed) {
            throw new IllegalStateException("RoaringBitmapIndex has been freed.");
        }
        char highKey = (char) (value >>> 16);
        char lowKey = (char) (value & 0xFFFF);

        int index = binarySearchKeys(highKey);
        if (index < 0) {
            return false;
        }

        long containerAddr = UNSAFE.getLong(containersAddress + ((long) index * 8));
        return containsLowValue(containerAddr, lowKey);
    }

    public synchronized int cardinality() {
        if (isFreed) {
            throw new IllegalStateException("RoaringBitmapIndex has been freed.");
        }
        int total = 0;
        for (int i = 0; i < size; i++) {
            long containerAddr = UNSAFE.getLong(containersAddress + ((long) i * 8));
            total += getContainerCardinality(containerAddr);
        }
        return total;
    }

    public synchronized void and(RoaringBitmapIndex other) {
        if (this.isFreed || other.isFreed) {
            throw new IllegalStateException("RoaringBitmapIndex has been freed.");
        }

        int newCapacity = Math.max(this.size, other.size);
        long newKeysAddress = UNSAFE.allocateMemory((long) newCapacity * 2);
        long newContainersAddress = UNSAFE.allocateMemory((long) newCapacity * 8);
        int newSize = 0;

        int i = 0, j = 0;
        while (i < this.size && j < other.size) {
            char key1 = UNSAFE.getChar(this.keysAddress + ((long) i * 2));
            char key2 = UNSAFE.getChar(other.keysAddress + ((long) j * 2));

            if (key1 == key2) {
                long cont1 = UNSAFE.getLong(this.containersAddress + ((long) i * 8));
                long cont2 = UNSAFE.getLong(other.containersAddress + ((long) j * 8));
                long resCont = intersectContainers(cont1, cont2);

                if (getContainerCardinality(resCont) > 0) {
                    if (newSize >= newCapacity) {
                        newCapacity *= CAPACITY_GROWTH_FACTOR;
                        long tempKeys = UNSAFE.allocateMemory((long) newCapacity * 2);
                        long tempConts = UNSAFE.allocateMemory((long) newCapacity * 8);
                        UNSAFE.copyMemory(newKeysAddress, tempKeys, (long) newSize * 2);
                        UNSAFE.copyMemory(newContainersAddress, tempConts, (long) newSize * 8);
                        UNSAFE.freeMemory(newKeysAddress);
                        UNSAFE.freeMemory(newContainersAddress);
                        newKeysAddress = tempKeys;
                        newContainersAddress = tempConts;
                    }
                    UNSAFE.putChar(newKeysAddress + ((long) newSize * 2), key1);
                    UNSAFE.putLong(newContainersAddress + ((long) newSize * 8), resCont);
                    newSize++;
                } else {
                    freeContainer(resCont);
                }
                i++;
                j++;
            } else if (key1 < key2) {
                i++;
            } else {
                j++;
            }
        }

        // Free old containers and memory of this
        for (int k = 0; k < this.size; k++) {
            freeContainer(UNSAFE.getLong(this.containersAddress + ((long) k * 8)));
        }
        UNSAFE.freeMemory(this.keysAddress);
        UNSAFE.freeMemory(this.containersAddress);

        this.keysAddress = newKeysAddress;
        this.containersAddress = newContainersAddress;
        this.size = newSize;
        this.capacity = newCapacity;
    }

    public synchronized void or(RoaringBitmapIndex other) {
        if (this.isFreed || other.isFreed) {
            throw new IllegalStateException("RoaringBitmapIndex has been freed.");
        }

        int newCapacity = this.size + other.size;
        long newKeysAddress = UNSAFE.allocateMemory((long) newCapacity * 2);
        long newContainersAddress = UNSAFE.allocateMemory((long) newCapacity * 8);
        int newSize = 0;

        int i = 0, j = 0;
        while (i < this.size && j < other.size) {
            char key1 = UNSAFE.getChar(this.keysAddress + ((long) i * 2));
            char key2 = UNSAFE.getChar(other.keysAddress + ((long) j * 2));

            long resCont;
            char targetKey;

            if (key1 == key2) {
                long cont1 = UNSAFE.getLong(this.containersAddress + ((long) i * 8));
                long cont2 = UNSAFE.getLong(other.containersAddress + ((long) j * 8));
                resCont = unionContainers(cont1, cont2);
                targetKey = key1;
                i++;
                j++;
            } else if (key1 < key2) {
                long cont1 = UNSAFE.getLong(this.containersAddress + ((long) i * 8));
                resCont = cloneContainer(cont1);
                targetKey = key1;
                i++;
            } else {
                long cont2 = UNSAFE.getLong(other.containersAddress + ((long) j * 8));
                resCont = cloneContainer(cont2);
                targetKey = key2;
                j++;
            }

            UNSAFE.putChar(newKeysAddress + ((long) newSize * 2), targetKey);
            UNSAFE.putLong(newContainersAddress + ((long) newSize * 8), resCont);
            newSize++;
        }

        while (i < this.size) {
            char key1 = UNSAFE.getChar(this.keysAddress + ((long) i * 2));
            long cont1 = UNSAFE.getLong(this.containersAddress + ((long) i * 8));
            UNSAFE.putChar(newKeysAddress + ((long) newSize * 2), key1);
            UNSAFE.putLong(newContainersAddress + ((long) newSize * 8), cloneContainer(cont1));
            newSize++;
            i++;
        }

        while (j < other.size) {
            char key2 = UNSAFE.getChar(other.keysAddress + ((long) j * 2));
            long cont2 = UNSAFE.getLong(other.containersAddress + ((long) j * 8));
            UNSAFE.putChar(newKeysAddress + ((long) newSize * 2), key2);
            UNSAFE.putLong(newContainersAddress + ((long) newSize * 8), cloneContainer(cont2));
            newSize++;
            j++;
        }

        for (int k = 0; k < this.size; k++) {
            freeContainer(UNSAFE.getLong(this.containersAddress + ((long) k * 8)));
        }
        UNSAFE.freeMemory(this.keysAddress);
        UNSAFE.freeMemory(this.containersAddress);

        this.keysAddress = newKeysAddress;
        this.containersAddress = newContainersAddress;
        this.size = newSize;
        this.capacity = newCapacity;
    }

    public synchronized void free() {
        if (isFreed) {
            return;
        }
        for (int i = 0; i < size; i++) {
            long containerAddr = UNSAFE.getLong(containersAddress + ((long) i * 8));
            freeContainer(containerAddr);
        }
        UNSAFE.freeMemory(keysAddress);
        UNSAFE.freeMemory(containersAddress);
        isFreed = true;
    }

    private int binarySearchKeys(char key) {
        int low = 0;
        int high = size - 1;
        while (low <= high) {
            int mid = (low + high) >>> 1;
            char midVal = UNSAFE.getChar(keysAddress + ((long) mid * 2));
            if (midVal < key) {
                low = mid + 1;
            } else if (midVal > key) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return -(low + 1);
    }

    private void ensureCapacity(int minCapacity) {
        if (minCapacity > capacity) {
            int newCapacity = Math.max(capacity * CAPACITY_GROWTH_FACTOR, minCapacity);
            long newKeys = UNSAFE.allocateMemory((long) newCapacity * 2);
            long newConts = UNSAFE.allocateMemory((long) newCapacity * 8);

            UNSAFE.copyMemory(keysAddress, newKeys, (long) size * 2);
            UNSAFE.copyMemory(containersAddress, newConts, (long) size * 8);

            UNSAFE.freeMemory(keysAddress);
            UNSAFE.freeMemory(containersAddress);

            keysAddress = newKeys;
            containersAddress = newConts;
            capacity = newCapacity;
        }
    }

    // Container memory layout:
    // Offset 0: short type (0 = Array, 1 = Bitmap, 2 = Run)
    // Offset 2: int cardinality
    // Offset 6: data bytes depending on type
    // - Array: capacity (2 bytes) + actual elements (2 * capacity bytes)
    // - Bitmap: 8192 bytes (65536 bits)
    // - Run: count (2 bytes) + pairs of (start:2, length:2)

    private long createArrayContainer() {
        long addr = UNSAFE.allocateMemory(10); // 2(type) + 4(cardinality) + 2(array_capacity) + 2(initial_val_placeholder)
        UNSAFE.putShort(addr, CONTAINER_TYPE_ARRAY);
        UNSAFE.putInt(addr + 2, 0);
        UNSAFE.putShort(addr + 6, 0); // array capacity
        return addr;
    }

    private long addLowValue(long containerAddr, char lowKey) {
        short type = UNSAFE.getShort(containerAddr);
        if (type == CONTAINER_TYPE_ARRAY) {
            int card = UNSAFE.getInt(containerAddr + 2);
            int arrCap = UNSAFE.getShort(containerAddr + 6) & 0xFFFF;

            // Search in array
            int idx = arrayBinarySearch(containerAddr + 8, card, lowKey);
            if (idx >= 0) {
                return containerAddr; // already present
            }
            int insertionPoint = -idx - 1;

            if (card + 1 > 4096) {
                // Convert to bitmap container
                long bitmapAddr = convertArrayToBitmap(containerAddr, lowKey);
                UNSAFE.freeMemory(containerAddr);
                return bitmapAddr;
            }

            if (card + 1 > arrCap) {
                int newArrCap = Math.max(8, arrCap * 2);
                long newAddr = UNSAFE.allocateMemory(8L + (long) newArrCap * 2);
                UNSAFE.putShort(newAddr, CONTAINER_TYPE_ARRAY);
                UNSAFE.putInt(newAddr + 2, card);
                UNSAFE.putShort(newAddr + 6, (short) newArrCap);

                // Copy before insertion
                if (insertionPoint > 0) {
                    UNSAFE.copyMemory(containerAddr + 8, newAddr + 8, (long) insertionPoint * 2);
                }
                // Insert
                UNSAFE.putChar(newAddr + 8 + ((long) insertionPoint * 2), lowKey);
                // Copy after insertion
                if (insertionPoint < card) {
                    UNSAFE.copyMemory(containerAddr + 8 + ((long) insertionPoint * 2),
                            newAddr + 8 + ((long) (insertionPoint + 1) * 2),
                            (long) (card - insertionPoint) * 2);
                }
                UNSAFE.putInt(newAddr + 2, card + 1);
                UNSAFE.freeMemory(containerAddr);
                return newAddr;
            } else {
                // Shift in-place
                if (insertionPoint < card) {
                    long src = containerAddr + 8 + ((long) insertionPoint * 2);
                    long dst = containerAddr + 8 + ((long) (insertionPoint + 1) * 2);
                    UNSAFE.copyMemory(src, dst, (long) (card - insertionPoint) * 2);
                }
                UNSAFE.putChar(containerAddr + 8 + ((long) insertionPoint * 2), lowKey);
                UNSAFE.putInt(containerAddr + 2, card + 1);
                return containerAddr