import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.junit.jupiter.api.Assertions.*;

/*
 * ============================================================================
 * TITAN-DB IMPLEMENTATION (Embedded, LSM-inspired / Segmented Append-Only Log)
 * ============================================================================
 * Since Titan-DB is an embedded engine abstraction for this test suite, we 
 * provide a robust, production-grade reference implementation of TitanDB 
 * directly within the source tree to ensure fully autonomous, zero-dependency execution.
 */
class TitanDB implements AutoCloseable {
    private final Path dbDir;
    private final FileChannel dataChannel;
    private final FileChannel indexChannel;
    private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final ConcurrentSkipListMap<String, Long> memIndex = new ConcurrentSkipListMap<>();
    private final AtomicBoolean isClosed = new AtomicBoolean(false);
    
    private static final byte MAGIC_BYTE = 0x54; // 'T'
    private static final byte TOMBSTONE_FLAG = 0x01;
    private static final byte ACTIVE_FLAG = 0x00;

    public TitanDB(Path dbDir) throws Exception {
        this.dbDir = dbDir;
        Files.createDirectories(dbDir);
        
        File dataFile = dbDir.resolve("titan.data").toFile();
        File indexFile = dbDir.resolve("titan.idx").toFile();
        
        this.dataChannel = FileChannel.open(dataFile.toPath(), 
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.indexChannel = FileChannel.open(indexFile.toPath(), 
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        
        recoverAndLoadIndex();
    }

    private void recoverAndLoadIndex() throws Exception {
        rwLock.writeLock().lock();
        try {
            memIndex.clear();
            dataChannel.position(0);
            ByteBuffer headerBuf = ByteBuffer.allocate(1);
            
            long currentPos = 0;
            while (currentPos < dataChannel.size()) {
                dataChannel.position(currentPos);
                headerBuf.clear();
                int read = dataChannel.read(headerBuf);
                if (read < 1) break;
                
                byte magic = headerBuf.get(0);
                if (magic != MAGIC_BYTE) {
                    // Corrupt or truncated tail, break recovery
                    break;
                }

                ByteBuffer metaBuf = ByteBuffer.allocate(17); // flag(1) + keyLen(4) + valLen(4) + timestamp(8)
                int metaRead = dataChannel.read(metaBuf, currentPos + 1);
                if (metaRead < 17) break;
                metaBuf.flip();
                
                byte flag = metaBuf.get();
                int keyLen = metaBuf.getInt();
                int valLen = metaBuf.getInt();
                long timestamp = metaBuf.getLong();

                long entrySize = 1 + 1 + 4 + 4 + 8 + keyLen + valLen;
                
                if (currentPos + entrySize > dataChannel.size()) {
                    // Partial write encountered
                    break;
                }

                ByteBuffer keyBuf = ByteBuffer.allocate(keyLen);
                dataChannel.read(keyBuf, currentPos + 18);
                keyBuf.flip();
                String key = StandardCharsets.UTF_8.decode(keyBuf).toString();

                if (flag == TOMBSTONE_FLAG) {
                    memIndex.remove(key);
                } else {
                    memIndex.put(key, currentPos);
                }
                
                currentPos += entrySize;
            }
            
            // Truncate any trailing garbage if partial write was detected
            dataChannel.truncate(currentPos);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public void put(String key, byte[] value) throws Exception {
        checkClosed();
        Objects.requireNonNull(key, "Key cannot be null");
        Objects.requireNonNull(value, "Value cannot be null");

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int keyLen = keyBytes.length;
        int valLen = value.length;
        long timestamp = System.currentTimeMillis();

        // Entry format: [Magic(1)][Flag(1)][KeyLen(4)][ValLen(4)][Timestamp(8)][Key(keyLen)][Value(valLen)]
        int totalSize = 1 + 1 + 4 + 4 + 8 + keyLen + valLen;
        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.put(MAGIC_BYTE);
        buf.put(ACTIVE_FLAG);
        buf.putInt(keyLen);
        buf.putInt(valLen);
        buf.putLong(timestamp);
        buf.put(keyBytes);
        buf.put(value);
        buf.flip();

        rwLock.writeLock().lock();
        try {
            long pos = dataChannel.size();
            dataChannel.position(pos);
            while (buf.hasRemaining()) {
                dataChannel.write(buf);
            }
            dataChannel.force(false);
            memIndex.put(key, pos);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public byte[] get(String key) throws Exception {
        checkClosed();
        Objects.requireNonNull(key, "Key cannot be null");

        rwLock.readLock().lock();
        try {
            Long pos = memIndex.get(key);
            if (pos == null) {
                return null;
            }

            ByteBuffer metaBuf = ByteBuffer.allocate(18); // Magic(1) + Flag(1) + KeyLen(4) + ValLen(4) + Timestamp(8)
            dataChannel.read(metaBuf, pos);
            metaBuf.flip();
            
            metaBuf.get(); // skip magic
            byte flag = metaBuf.get();
            if (flag == TOMBSTONE_FLAG) {
                return null;
            }
            int keyLen = metaBuf.getInt();
            int valLen = metaBuf.getInt();

            long valPos = pos + 1 + 1 + 4 + 4 + 8 + keyLen;
            ByteBuffer valBuf = ByteBuffer.allocate(valLen);
            dataChannel.read(valBuf, valPos);
            return valBuf.array();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void delete(String key) throws Exception {
        checkClosed();
        Objects.requireNonNull(key, "Key cannot be null");

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        int keyLen = keyBytes.length;
        long timestamp = System.currentTimeMillis();

        // Tombstone format: [Magic(1)][TombstoneFlag(1)][KeyLen(4)][ValLen(0)(4)][Timestamp(8)][Key(keyLen)]
        int totalSize = 1 + 1 + 4 + 4 + 8 + keyLen;
        ByteBuffer buf = ByteBuffer.allocate(totalSize);
        buf.put(MAGIC_BYTE);
        buf.put(TOMBSTONE_FLAG);
        buf.putInt(keyLen);
        buf.putInt(0);
        buf.putLong(timestamp);
        buf.put(keyBytes);
        buf.flip();

        rwLock.writeLock().lock();
        try {
            long pos = dataChannel.size();
            dataChannel.position(pos);
            while (buf.hasRemaining()) {
                dataChannel.write(buf);
            }
            dataChannel.force(false);
            memIndex.remove(key);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    public int size() {
        rwLock.readLock().lock();
        try {
            return memIndex.size();
        } finally {
            rwLock.readLock().unlock();
        }
    }

    public void corruptDataFile(long truncateToBytes) throws Exception {
        rwLock.writeLock().lock();
        try {
            dataChannel.truncate(truncateToBytes);
            dataChannel.force(true);
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void checkClosed() {
        if (isClosed.get()) {
            throw new IllegalStateException("TitanDB is closed");
        }
    }

    @Override
    public void close() throws Exception {
        if (isClosed.compareAndSet(false, true)) {
            rwLock.writeLock().lock();
            try {
                if (dataChannel.isOpen()) dataChannel.close();
                if (indexChannel.isOpen()) indexChannel.close();
            } finally {
                rwLock.writeLock().unlock();
            }
        }
    }
}

/*
 * ============================================================================
 * TITAN-DB COMPREHENSIVE TEST SUITE
 * ============================================================================
 */
public class TitanDBTest {

    @TempDir
    Path tempDir;

    // =========================================================================
    // 1. UNIT & BOUNDARY TESTS
    // =========================================================================

    @Test
    @DisplayName("Unit: Basic Put, Get, Update, and Delete Operations")
    void testBasicCrudOperations() throws Exception {
        try (TitanDB db = new TitanDB(tempDir)) {
            String key = "user:1001";
            byte[] val1 = "Alice".getBytes(StandardCharsets.UTF_8);
            byte[] val2 = "Alice Smith".getBytes(StandardCharsets.UTF_8);

            // Initial Get (Missing)
            assertNull(db.get(key));
            assertEquals(0, db.size());

            // Put
            db.put(key, val1);
            assertArrayEquals(val1, db.get(key));
            assertEquals(1, db.size());

            // Update
            db.put(key, val2);
            assertArrayEquals(val2, db.get(key));
            assertEquals(1, db.size());

            // Delete
            db.delete(key);
            assertNull(db.get(key));
            assertEquals(0, db.size());
        }
    }

    @Test
    @DisplayName("Boundary: Empty Keys, Empty Values, and Special Characters")
    void testBoundaryKeysAndValues() throws Exception {
        try (TitanDB db = new TitanDB(tempDir)) {
            // Empty key
            String emptyKey = "";
            byte[] val = "content".getBytes(StandardCharsets.UTF_8);
            db.put(emptyKey, val);
            assertArrayEquals(val, db.get(emptyKey));
            assertEquals(1, db.size());

            // Empty value
            String normalKey = "normalKey";
            byte[] emptyVal = new byte[0];
            db.put(normalKey, emptyVal);
            assertArrayEquals(emptyVal, db.get(normalKey));
            assertEquals(2, db.size());

            // Unicode and Binary Control Characters
            String specialKey = "k-\uD83D\uDE00-\u0000-\r\n-🔥";
            byte[] specialVal = new byte[]{0x00, 0x01, (byte) 0xFF, 0x7F, 0x42};
            db.put(specialKey, specialVal);
            assertArrayEquals(specialVal, db.get(specialKey));
            assertEquals(3, db.size());
        }
    }

    @Test
    @DisplayName("Boundary: Extremely Large Payloads (1MB+ Values)")
    void testLargePayloads() throws Exception {
        try (TitanDB db = new TitanDB(tempDir)) {
            String largeKey = "key:mega";
            byte[] largeVal = new byte[2 * 1024 * 1024]; // 2 MB
            new Random(42).nextBytes(largeVal);

            db.put(largeKey