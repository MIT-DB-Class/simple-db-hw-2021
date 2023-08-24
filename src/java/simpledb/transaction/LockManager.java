package simpledb.transaction;
import simpledb.common.Permissions;
import simpledb.storage.PageId;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
public class LockManager {
    public static class PageLock {
        public final PageId pid;
        public Permissions perm;
        public int holdNum;

        public PageLock(PageId pid, Permissions perm) {
            this.pid = pid;
            this.perm = perm;
            holdNum = 1;
        }
        @Override
        public boolean equals(Object o) {
            if (o == null || o.getClass() != this.getClass()) {
                return false;
            }

            if (this == o) {
                return true;
            }

            return this.pid.equals(((PageLock) o).pid);
        }

        @Override
        public int hashCode() {
            return pid.hashCode();
        }
    }

    static class DirectedWaitGraph {
        final ConcurrentHashMap<TransactionId, HashSet<TransactionId>> g;
        public DirectedWaitGraph(){
            g = new ConcurrentHashMap<>();
        }
        public void addVertex(TransactionId tid){
            if(g.containsKey(tid)) return;
            g.put(tid, new HashSet<>());
        }
        public void addEdge(TransactionId s, TransactionId e){
            addVertex(s);
            addVertex(e);
            g.get(s).add(e);
        }
        public void removeEdge(TransactionId s, TransactionId e){
            if(g.containsKey(s)){
                g.get(s).remove(e);
            }
        }
        public void removeVertex(TransactionId node){
            for(Map.Entry<TransactionId, HashSet<TransactionId>> entry: g.entrySet()){
                entry.getValue().remove(node);
            }
            g.remove(node);
        }

        private boolean isCyclicUtil(TransactionId tid, Map<TransactionId, Boolean> visited, Map<TransactionId, Boolean> recStack) {
            if (recStack.getOrDefault(tid, false)) {
                return true;
            }

            if (visited.getOrDefault(tid, false)) {
                return false;
            }

            visited.put(tid, true);
            recStack.put(tid, true);

            HashSet<TransactionId> children = g.get(tid);
            for (TransactionId child : children) {
                if (isCyclicUtil(child, visited, recStack)) {
                    return true;
                }
            }

            recStack.put(tid, false);
            return false;
        }

        public boolean isCyclic() {
            Map<TransactionId, Boolean> visited = new HashMap<>();
            Map<TransactionId, Boolean> recStack = new HashMap<>();

            for (TransactionId tid : g.keySet()) {
                if (isCyclicUtil(tid, visited, recStack)) {
                    return true;
                }
            }
            return false;
        }
    }

    static class Digraph {
        final ConcurrentHashMap<TransactionId, HashSet<TransactionId>> waitList;

        public Digraph() {
            waitList = new ConcurrentHashMap<>();
        }

        void print() {
            for (Map.Entry e: waitList.entrySet()) {
                System.out.print("" + ((TransactionId) e.getKey()).getId() + ": ");
                HashSet<TransactionId> s = (HashSet<TransactionId>) e.getValue();
                for (TransactionId i: s) {
                    System.out.print("" + i.getId() + ", ");
                }
                System.out.println();
            }
        }

        public void addVertex(TransactionId tid) {
            if (waitList.containsKey(tid)) {
                return;
            }
            waitList.put(tid, new HashSet<>());
        }

        public void addEdge(TransactionId from, TransactionId to) {
            addVertex(from);
            addVertex(to);
            waitList.get(from).add(to);
        }

        public void removeEdge(TransactionId from, TransactionId to) {
            if (waitList.containsKey(from) && waitList.containsKey(to)) {
                waitList.get(from).remove(to);
            }
        }

        public void removeVertex(TransactionId tid) {
            for (Map.Entry e: waitList.entrySet()) {
                HashSet<TransactionId> s = (HashSet<TransactionId>) e.getValue();
                s.remove(tid);
            }
            waitList.remove(tid);
        }

        private boolean isCyclicHelper(TransactionId id, ConcurrentHashMap<TransactionId, Boolean> visited,
                                       ConcurrentHashMap<TransactionId, Boolean> traceStack) {
            if (traceStack.getOrDefault(id, false)) {
                return true;
            }

            if (visited.getOrDefault(id, false)) {
                return false;
            }
            visited.put(id, true);
            traceStack.put(id, true);
            Set<TransactionId> s = waitList.get(id);

            for (TransactionId t : s)
                if (isCyclicHelper(t, visited, traceStack)) {
                    return true;
                }
            traceStack.put(id, false);
            return false;
        }

        public boolean isCyclic() {
            int v = waitList.size();
            ConcurrentHashMap<TransactionId, Boolean> visited = new ConcurrentHashMap<>();
            ConcurrentHashMap<TransactionId, Boolean> traceStack = new ConcurrentHashMap<>();
            for (TransactionId id : waitList.keySet())
                if (isCyclicHelper(id, visited, traceStack)) {
                    return true;
                }
            return false;
        }
    }

    final ConcurrentHashMap<TransactionId, Set<PageLock>> txn2LocksMap;
    final ConcurrentHashMap<PageId, Set<TransactionId>> pageId2TxnsMap;
    final DirectedWaitGraph graph;
    volatile boolean hasWriter = false;
    volatile PageId writerPage;
    public LockManager(){
        txn2LocksMap = new ConcurrentHashMap<>();
        pageId2TxnsMap = new ConcurrentHashMap();
        graph = new DirectedWaitGraph();
    }

    public synchronized void grantLock(TransactionId tid, PageId pid, Permissions permType) throws TransactionAbortedException{
        Set<TransactionId> txns = pageId2TxnsMap.get(pid);
        Set<PageLock> locks = txn2LocksMap.get(tid);
        if(txns == null){
            PageLock lock = new PageLock(pid, permType);
            txns = new HashSet<>();
            txns.add(tid);
            pageId2TxnsMap.put(pid, txns);
            if(locks == null){
                locks = new HashSet<>();
                locks.add(lock);
                txn2LocksMap.put(tid, locks);
            }else{
                locks.add(lock);
            }
        }else{
            if(permType.equals(Permissions.READ_ONLY)){
                if(locks != null && locks.contains(new PageLock(pid, Permissions.READ_ONLY))){
                    // if we already get some lock for corrent page, and we are asking for READ only lock
                    // so just grant it.
                    return;
                }
                if(locks != null && hasWriter && writerPage.equals(pid)){

                    //            if(locks != null && locks.size() > 2 && hasWriter && writerPage.equals(pid)){
                    // though we are trying to grant READ ONLY here, due to the concurrent nature
                    // This could happen, that is, try to read a page with writer, we should abort
                    // rather than waiting writer finished. Since the data inconsistency
                    throw new TransactionAbortedException();
                }
                // here we
                // 1. there is no other lock with same transaction and pageid as current
                // 2. current page is not being writing
                PageLock plk = null;
                for (TransactionId txnId : txns) {
                    Set<PageLock> currentLocks = txn2LocksMap.get(txnId);
                    if (currentLocks != null) {
                        for (PageLock p : currentLocks) {
                            if (p.equals(new PageLock(pid, permType))) {
                                plk = p;
                                break;
                            }
                        }
                    }
                    if (plk != null) {
                        break;
                    }
                }
                if(plk != null && plk.perm == Permissions.READ_WRITE){
                    // trying to acquire write lock for a page locked by another txn's read lock, need to add wait edge
                    // from tid to other txnId
                    for(TransactionId txnId: txns){
                        if(!txnId.equals(tid)){
                            graph.addEdge(tid, txnId);
                        }
                    }
                    if (graph.isCyclic()) {
                        /* deadlock will occur */
                        for (TransactionId id : txns) {
                            graph.removeEdge(tid, id);
                        }
                        graph.removeVertex(tid);
                        throw new TransactionAbortedException();
                    }
                    // The most interesting part, if adding current edge will not leads to cyclic, then hold plk utill release
                    while(plk.holdNum != 0){
                        try{
                            this.wait();
                        }catch (InterruptedException e){

                        }
                    }
                }
                // here we know, at least for now, no other txn is waiting for current READ ONLY lock.
                graph.removeVertex(tid);
                // and, one more time, lock is at page granularity, so for same page lock, number of transaction to hold
                // this lock should increment by 1.
                updateMap(plk, permType, tid, locks);
                txns.add(tid);
            }else{
                boolean holds = false;
                PageLock plk = null;

                for (TransactionId txnId : txns) {
                    Set<PageLock> currentLocks = txn2LocksMap.get(txnId);
                    if (currentLocks != null) {
                        for (PageLock p : currentLocks) {
                            if (p.equals(new PageLock(pid, permType))) {
                                plk = p;
                                break;
                            }
                        }
                    }
                    if (plk != null) {
                        break;
                    }
                }
                if(txns.contains(tid)){
                    if(txns.size() == 1){
                        // txns = page2TxnIdmap.get(pid), there is only one transaction for current page
                        // then there will be only one pagelock for current page
                        if(plk != null && plk.perm.equals(Permissions.READ_ONLY)){
                            plk.perm = Permissions.READ_WRITE;
                            plk.holdNum = 1;
                        }
                        // else do nothing.
                        return;
                    }else{
                        holds = true;
                    }
                }
                // for all other txns in the same page, since now trying to grant write lock, other lock should be released
                // before writing
                for(TransactionId txnId: txns){
                    if(!txnId.equals(tid)){
                        graph.addEdge(tid, txnId);
                    }
                }
                if (graph.isCyclic()) {
                    /* deadlock will occur */
                    for (TransactionId id : txns) {
                        graph.removeEdge(tid, id);
                    }
                    graph.removeVertex(tid);
                    throw new TransactionAbortedException();
                }
                hasWriter = true;
                writerPage = pid;
                // The most interesting part, if adding current edge will not leads to cyclic, then hold plk utill release
                while(plk.holdNum != 0 && (plk.holdNum != 1 || !holds)){
                    try{
                        this.wait();
                    }catch (InterruptedException e){

                    }
                }
                hasWriter = false;
                writerPage = null;
                graph.removeVertex(tid);
                updateMap(plk, permType, tid, locks);
                txns.add(tid);
            }
        }
    }

    public synchronized void updateMap(PageLock plk, Permissions perm, TransactionId tid, Set<PageLock> locks){
        plk.holdNum++;
        plk.perm = perm;
        if(locks == null){
            locks = new HashSet<>();
            locks.add(plk);
            txn2LocksMap.put(tid, locks);
        }else{
            locks.add(plk);
        }
    }

    public synchronized boolean holdLock(TransactionId tid, PageId pid) {
        /* compare based on PageId, permission doesn't matter here */
        return pageId2TxnsMap.containsKey(pid) && pageId2TxnsMap.get(pid) != null
                && pageId2TxnsMap.get(pid).contains(tid);
    }
    public synchronized void releaseLock(TransactionId tid, PageId pid) {
        if (holdLock(tid, pid)) {
            Set<PageLock> lockSet = txn2LocksMap.get(tid);
            Set<TransactionId> txSet = pageId2TxnsMap.get(pid);
            PageLock lock = null;
            /* compare based on PageId, permission doesn't matter here */
            PageLock targetLock = new PageLock(pid, Permissions.READ_WRITE);
            for (PageLock l : lockSet) {
                if (l.equals(targetLock)) {
                    lock = l;
                }
            }
            lock.holdNum--;
            /* remove lock from two maps */
            txSet.remove(tid);
            if (txSet.size() == 0) {
                pageId2TxnsMap.remove(pid);
            }
            lockSet.remove(lock);
            if (lockSet.size() == 0) {
                txn2LocksMap.remove(tid);
            }
            this.notifyAll();
        }
    }

    public synchronized Set<PageLock> getPages(TransactionId tid) {
        /* return a copy of the set to support modification when iterating */
        return new HashSet<>(txn2LocksMap.getOrDefault(tid, Collections.emptySet()));
    }
}