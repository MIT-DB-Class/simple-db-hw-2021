package simpledb.transaction;

import simpledb.storage.PageId;

import java.util.ArrayList;
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

    synchronized boolean acquireLock(PageId pid, TransactionId tid, int lockType){
        // If there is no lock for tid, add a lock to lockMap for tid;
        if(!lockMap.containsKey(pid)){
            List<Lock> locks = new ArrayList<>();
            Lock cur = new Lock(tid, lockType);
            locks.add(cur);
            lockMap.put(pid, locks);
            return true;
        }
        List<Lock> locks = lockMap.get(pid);
        for(final Lock lock: locks){
            if(lock.getTid().equals(tid)){
                // if there is a lock for tid and the lock is same type with requesting type return true
                if(lock.getLockType() == lockType) return true;
                // if there existing write lock for tid, no matter which type of lock is being requested, return true
                if(lock.getLockType() == 1 ) return true;
                if(lock.getLockType() == 0 && locks.size() == 1){
                    lock.setLockType(1);
                    return true;
                }
            }
        }
        if(locks.size() > 0 && locks.get(0).getLockType() == 1){
            return false;
        }
        if(lockType == 0){
            final Lock lock = new Lock(tid, lockType);
            locks.add(lock);
            return true;
        }
        return false;
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
                return true;
            }
        }
        return false;
    }
}
