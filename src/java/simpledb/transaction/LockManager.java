package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.List;
public class LockManager {
    public ConcurrentHashMap<PageId, List<Lock>> lockMap;
    public LockManager(){
        lockMap = new ConcurrentHashMap<>();
    }
    public synchronized boolean tryAcquireLock(PageId pid, TransactionId tid, int lockType, int timeout){
        final long now = System.currentTimeMillis();
        while(true){
            if(System.currentTimeMillis() - now >= timeout){
                return false;
            }
            if(acquireLock(pid, tid, lockType)) return true;
        }
    }

    private final Object lockCondition = new Object();  // Used for wait and notify

    // Use constants for readability
    public static final int READ_LOCK = 0;
    public static final int WRITE_LOCK = 1;

    public boolean acquireLock(PageId pid, TransactionId tid, int lockType) {
        synchronized (lockCondition) {
            if (!lockMap.containsKey(pid)) {
                List<Lock> locks = new ArrayList<>();
                Lock cur = new Lock(tid, lockType);
                locks.add(cur);
                lockMap.put(pid, locks);
                return true;
            }

            List<Lock> locks = lockMap.get(pid);
            for (final Lock lock : locks) {
                if (lock.getTid().equals(tid)) {
                    if (lock.getLockType() == lockType) return true;
                    if (lock.getLockType() == WRITE_LOCK) return true;
                    if (lock.getLockType() == READ_LOCK && locks.size() == 1) {
                        lock.setLockType(WRITE_LOCK);
                        return true;
                    }
                }
            }

            if (lockType == READ_LOCK) {
                Lock lock = new Lock(tid, READ_LOCK);
                locks.add(lock);
                return true;
            } else if (lockType == WRITE_LOCK) {
                while (locks.size() > 0) {
                    try {
                        lockCondition.wait();  // Wait until all locks on this page are released
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();  // Preserve interrupt status
                        return false;  // On interrupt, return false
                    }
                }
                Lock lock = new Lock(tid, WRITE_LOCK);
                locks.add(lock);
                return true;
            }
            return false;
        }
    }

    public synchronized boolean releaseLock(PageId pid, TransactionId tid){
        if(!lockMap.containsKey(pid)) {
            return false;
        };
        List<Lock> locks = lockMap.get(pid);
        for(Lock lock: locks){
            if(lock.getTid().equals(tid)){
                locks.remove(lock);
                lockMap.put(pid, locks);
                if(locks.size() == 0){
                    lockMap.remove(pid);
                }
                lockCondition.notifyAll();
                return true;
            }
        }
        return false;
    }

    public synchronized void releaseLockByTid(TransactionId tid){
        Iterator<Map.Entry<PageId, List<Lock>>> entryIterator = lockMap.entrySet().iterator();
        while (entryIterator.hasNext()) {
            Map.Entry<PageId, List<Lock>> entry = entryIterator.next();
            List<Lock> locks = entry.getValue();
            Iterator<Lock> lockIterator = locks.iterator();
            while (lockIterator.hasNext()) {
                Lock lock = lockIterator.next();
                if (lock.getTid().equals(tid)) {
                    lockIterator.remove();
                }
            }
            // Remove the entry from lockMap if there are no more locks.
            if (locks.isEmpty()) {
                entryIterator.remove();
            }

        }
    }
}
