package core.concurrency;

import sun.misc.Unsafe;
import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.LockSupport;

public final class MVCCManager {

    private static final Unsafe UNSAFE;
    static {
        try {
            Field f = Unsafe.class.getDeclaredField("theUnsafe");
            f.setAccessible(true);
            UNSAFE = (Unsafe) f.get(null);
        } catch (Exception e) {
            throw new RuntimeException("Unable to initialize sun.misc.Unsafe", e);
        }
    }

    private static final int MAX_THREADS = 128;
    private static final int EPOCH_SHIFT = 3; 
    private static final long HEARTBEAT_INTERVAL_NS = 10_000_000L; // 10ms

    private final AtomicLong globalEpoch = new AtomicLong(1L);
    private final AtomicLong transactionIdSequence = new AtomicLong(1L);
    
    // Hazard pointer / thread local epoch tracking slots
    private final AtomicReferenceArray<ThreadEpochSlot> threadSlots;
    private final AtomicBoolean isShutdown = new AtomicBoolean(false);
    private final Thread backgroundReclamationThread;

    public static final class ThreadEpochSlot {
        public final AtomicLong currentEpoch = new AtomicLong(0L);
        public final AtomicLong hazardPointer = new AtomicLong(0L);
        public final AtomicBoolean active = new AtomicBoolean(false);
    }

    public static final class EpochProtectedObject {
        private final long creationEpoch;
        private final long memoryAddress;
        private final long sizeBytes;
        private final Runnable destructor;

        public EpochProtectedObject(long creationEpoch, long memoryAddress, long sizeBytes, Runnable destructor) {
            this.creationEpoch = creationEpoch;
            this.memoryAddress = memoryAddress;
            this.sizeBytes = sizeBytes;
            this.destructor = destructor;
        }

        public long getCreationEpoch() {
            return creationEpoch;
        }

        public long getMemoryAddress() {
            return memoryAddress;
        }

        public long getSizeBytes() {
            return sizeBytes;
        }

        public void reclaim() {
            if (destructor != null) {
                destructor.run();
            }
        }
    }

    private static final class ReclamationNode {
        EpochProtectedObject payload;
        ReclamationNode next;

        ReclamationNode(EpochProtectedObject payload) {
            this.payload = payload;
        }
    }

    private static final class EpochBatch {
        long epoch;
        ReclamationNode head;
        ReclamationNode tail;
        EpochBatch next;

        EpochBatch(long epoch) {
            this.epoch = epoch;
        }
    }

    private final EpochBatch activeBatchesHead = new EpochBatch(0L);

    public MVCCManager() {
        this.threadSlots = new AtomicReferenceArray<>(MAX_THREADS);
        for (int i = 0; i < MAX_THREADS; i++) {
            threadSlots.set(i, new ThreadEpochSlot());
        }

        this.backgroundReclamationThread = new Thread(this::reclamationLoop, "Titan-MVCC-Reclaimer");
        this.backgroundReclamationThread.setDaemon(true);
        this.backgroundReclamationThread.start();
    }

    public long beginReadSnapshot() {
        long epoch = globalEpoch.get();
        ThreadEpochSlot slot = getMyThreadSlot();
        slot.currentEpoch.set(epoch);
        slot.active.set(true);
        // Ensure visibility fence
        UNSAFE.storeFence();
        return epoch;
    }

    public void registerHazardPointer(long address) {
        ThreadEpochSlot slot = getMyThreadSlot();
        slot.hazardPointer.set(address);
        UNSAFE.storeFence();
    }

    public void clearHazardPointer() {
        ThreadEpochSlot slot = getMyThreadSlot();
        slot.hazardPointer.set(0L);
        UNSAFE.storeFence();
    }

    public void endReadSnapshot() {
        ThreadEpochSlot slot = getMyThreadSlot();
        slot.hazardPointer.set(0L);
        slot.currentEpoch.set(0L);
        slot.active.set(false);
        UNSAFE.storeFence();
    }

    public long acquireTransactionId() {
        return transactionIdSequence.incrementAndGet();
    }

    public void retire(EpochProtectedObject obj) {
        long currentEpochVal = globalEpoch.get();
        ReclamationNode node = new ReclamationNode(obj);

        synchronized (activeBatchesHead) {
            EpochBatch batch = activeBatchesHead;
            while (batch.next != null && batch.next.epoch <= currentEpochVal) {
                batch = batch.next;
            }
            if (batch.epoch != currentEpochVal) {
                EpochBatch newBatch = new EpochBatch(currentEpochVal);
                newBatch.next = batch.next;
                batch.next = newBatch;
                batch = newBatch;
            }
            if (batch.tail == null) {
                batch.head = node;
                batch.tail = node;
            } else {
                batch.tail.next = node;
                batch.tail = node;
            }
        }
    }

    public void advanceEpoch() {
        globalEpoch.incrementAndGet();
    }

    private ThreadEpochSlot getMyThreadSlot() {
        int index = (int) (Thread.currentThread().getId() % MAX_THREADS);
        ThreadEpochSlot slot = threadSlots.get(index);
        if (slot == null) {
            slot = new ThreadEpochSlot();
            threadSlots.set(index, slot);
        }
        return slot;
    }

    private void reclamationLoop() {
        while (!isShutdown.get()) {
            try {
                Thread.sleep(HEARTBEAT_INTERVAL_NS / 1_000_000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupted();
                break;
            }

            advanceEpoch();
            reclaimStaleBatches();
        }
        // Force flush remaining batches on shutdown
        forceReclaimAll();
    }

    private void reclaimStaleBatches() {
        long minActiveEpoch = computeMinActiveEpoch();

        EpochBatch batchToReclaimHead = null;
        synchronized (activeBatchesHead) {
            EpochBatch prev = activeBatchesHead;
            EpochBatch curr = activeBatchesHead.next;

            while (curr != null && curr.epoch < minActiveEpoch) {
                prev.next = curr.next;
                curr.next = null;

                if (batchToReclaimHead == null) {
                    batchToReclaimHead = curr;
                } else {
                    EpochBatch temp = batchToReclaimHead;
                    while (temp.next != null) {
                        temp = temp.next;
                    }
                    temp.next = curr;
                }
                curr = prev.next;
            }
        }

        executeReclamationBatch(batchToReclaimHead);
    }

    private long computeMinActiveEpoch() {
        long minEpoch = globalEpoch.get();
        for (int i = 0; i < MAX_THREADS; i++) {
            ThreadEpochSlot slot = threadSlots.get(i);
            if (slot != null && slot.active.get()) {
                long ep = slot.currentEpoch.get();
                if (ep != 0 && ep < minEpoch) {
                    minEpoch = ep;
                }
            }
        }
        return minEpoch;
    }

    private boolean isProtectedByHazardPointer(long address) {
        if (address == 0L) {
            return false;
        }
        for (int i = 0; i < MAX_THREADS; i++) {
            ThreadEpochSlot slot = threadSlots.get(i);
            if (slot != null && slot.hazardPointer.get() == address) {
                return true;
            }
        }
        return false;
    }

    private void executeReclamationBatch(EpochBatch batchHead) {
        EpochBatch currentBatch = batchHead;
        while (currentBatch != null) {
            ReclamationNode node = currentBatch.head;
            ReclamationNode deferredHead = null;
            ReclamationNode deferredTail = null;

            while (node != null) {
                ReclamationNode nextNode = node.next;
                node.next = null;

                if (isProtectedByHazardPointer(node.payload.getMemoryAddress())) {
                    // Defer reclamation if still trapped by a hazard pointer
                    if (deferredHead == null) {
                        deferredHead = node;
                        deferredTail = node;
                    } else {
                        deferredTail.next = node;
                        deferredTail = node;
                    }
                } else {
                    node.payload.reclaim();
                }
                node = nextNode;
            }

            if (deferredHead != null) {
                // Re-queue deferred objects into current active epoch batch
                synchronized (activeBatchesHead) {
                    EpochBatch target = activeBatchesHead;
                    while (target.next != null) {
                        target = target.next;
                    }
                    if (target.tail == null) {
                        target.head = deferredHead;
                        target.tail = deferredTail;
                    } else {
                        target.tail.next = deferredHead;
                        target.tail = deferredTail;
                    }
                }
            }

            currentBatch = currentBatch.next;
        }
    }

    private void forceReclaimAll() {
        EpochBatch batchToReclaimHead;
        synchronized (activeBatchesHead) {
            batchToReclaimHead = activeBatchesHead.next;
            activeBatchesHead.next = null;
        }

        EpochBatch current = batchToReclaimHead;
        while (current != null) {
            ReclamationNode node = current.head;
            while (node != null) {
                node.payload.reclaim();
                node = node.next;
            }
            current = current.next;
        }
    }

    public void shutdown() {
        if (isShutdown.compareAndSet(false, true)) {
            LockSupport.unpark(backgroundReclamationThread);
            try {
                backgroundReclamationThread.join(2000L);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }
}