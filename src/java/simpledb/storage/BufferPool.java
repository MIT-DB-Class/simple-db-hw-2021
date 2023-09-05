package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.transaction.LockManager;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 *
 * @Threadsafe, all fields are final
 */
public class BufferPool {
    /**
     * Bytes per page, including header.
     */
    private static final int DEFAULT_PAGE_SIZE = 4096;

    private static int pageSize = DEFAULT_PAGE_SIZE;

    private final ConcurrentHashMap<PageId, Page> pages;

    private final ConcurrentLinkedDeque<PageId> pageOrd;

    private final int numPages;

    private final LockManager lm;

    /**
     * Default number of pages passed to the constructor. This is used by
     * other classes. BufferPool should use the numPages argument to the
     * constructor instead.
     */
    public static final int DEFAULT_PAGES = 50;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // some code goes here
        pages = new ConcurrentHashMap<>();
        pageOrd = new ConcurrentLinkedDeque<>();
        this.numPages = numPages;
        lm = new LockManager();
    }

    public static int getPageSize() {
        return pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void setPageSize(int pageSize) {
        BufferPool.pageSize = pageSize;
    }

    // THIS FUNCTION SHOULD ONLY BE USED FOR TESTING!!
    public static void resetPageSize() {
        BufferPool.pageSize = DEFAULT_PAGE_SIZE;
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, a page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid  the ID of the transaction requesting the page
     * @param pid  the ID of the requested page
     * @param perm the requested permissions on the page
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
            throws TransactionAbortedException, DbException {
        // some code goes here
        lm.grantLock(tid, pid, perm);
        if (!pages.containsKey(pid)) {
            if (pages.size() >= numPages)
                evictPage(tid);
            pages.put(pid, Database.getCatalog().getDatabaseFile(pid.getTableId()).readPage(pid));
            pageOrd.add(pid);
        } else {
            pageOrd.remove(pid);
            pageOrd.add(pid);
        }
        return pages.get(pid);
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for lab1|lab2
        lm.releaseLock(tid, pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid){
        // some code goes here
        // not necessary for lab1|lab2
        transactionComplete(tid, true);
    }

    /**
     * Return true if the specified transaction has a lock on the specified page
     */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for lab1|lab2
        return lm.holdLock(tid, p);
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid    the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit) {
        // some code goes here
        // not necessary for lab1|lab2
        Set<LockManager.PageLock> s = lm.getPages(tid);
        if (commit) {
            try{
                flushPages(tid);
            } catch (Exception e){
                e.printStackTrace();
            }

        } else {
            for (LockManager.PageLock l: s) {
                discardPage(l.pid);
            }
        }

        for (LockManager.PageLock l: s) {
            releasePage(tid, l.pid);
        }
    }

    /**
     * Add a tuple to the specified table on behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to and any other
     * pages that are updated (Lock acquisition is not needed for lab2).
     * May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid     the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t       the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        DbFile db = Database.getCatalog().getDatabaseFile(tableId);
        List<Page> modifiedPages = db.insertTuple(tid, t);

        for (Page p : modifiedPages) {
            p.markDirty(true, tid);
            PageId pid = p.getId();
            // null when not existent
            if (pages.replace(pid, p) == null) {
                if (pages.size() >= numPages)
                    evictPage(tid);
                else
                    pageOrd.add(pid);
                pages.put(pid, p);
            } else {
                pageOrd.remove(pid);
                pageOrd.add(pid);
            }
        }

    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from and any
     * other pages that are updated. May block if the lock(s) cannot be acquired.
     * <p>
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and adds versions of any pages that have
     * been dirtied to the cache (replacing any existing versions of those pages) so
     * that future requests see up-to-date pages.
     *
     * @param tid the transaction deleting the tuple.
     * @param t   the tuple to delete
     */
    public void deleteTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        List<Page> modifiedPages = Database.getCatalog().getDatabaseFile(
                t.getRecordId().getPageId().getTableId()).deleteTuple(tid, t);

        for (Page p : modifiedPages) {
            p.markDirty(true, tid);
            PageId pid = p.getId();
            // null when not existent
            if (pages.replace(pid, p) == null) {
                if (pages.size() >= numPages)
                    evictPage(tid);
                else
                    pageOrd.add(pid);
                pages.put(pid, p);
            } else {
                pageOrd.remove(pid);
                pageOrd.add(pid);
            }
        }
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     * break simpledb if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for lab1
        for (PageId pid : pages.keySet()) {
            flushPage(pid);
        }
    }

    /**
     * Remove the specific page id from the buffer pool.
     * Needed by the recovery manager to ensure that the
     * buffer pool doesn't keep a rolled back page in its
     * cache.
     * <p>
     * Also used by B+ tree files to ensure that deleted pages
     * are removed from the cache so they can be reused safely
     */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
        // not necessary for lab1
        if (pid != null) {
            pages.remove(pid);
            pageOrd.remove(pid);
        }
    }

    /**
     * Flushes a certain page to disk
     *
     * @param pid an ID indicating the page to flush
     */
    private synchronized void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for lab1
        if (pid != null) {
            Page p = pages.get(pid);
            TransactionId dirtier = p.isDirty();
            if (dirtier != null){
                Database.getLogFile().logWrite(dirtier, p.getBeforeImage(), p);
                Database.getLogFile().force();
            }
            Database.getCatalog().getDatabaseFile(pid.getTableId()).writePage(p);
            p.markDirty(false, null);
            pages.remove(pid);
            pageOrd.remove(pid);
        }
    }

    /**
     * Write all pages of the specified transaction to disk.
     */
    public synchronized void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for lab1|lab2
        Set<LockManager.PageLock> s = lm.getPages(tid);
        for (LockManager.PageLock l: s) {
            /* this transaction finishes, we can only flush those with read lock
            and there is no other transaction holding this page, or current transaction
            holds a write lock on this page
            */
            if (((l.perm.equals(Permissions.READ_ONLY) && l.holdNum == 1)
                    || l.perm.equals(Permissions.READ_WRITE))
                    && pages.containsKey(l.pid)) {
                flushPage(l.pid);
            }
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized void evictPage(TransactionId tid) throws DbException {
        PageId pid = null;
        boolean found = false;
        for (PageId id: pageOrd) {
            if (pages.get(id).isDirty() == null) {
                pid = id;
                found = true;
                break;
            }
        }
        if (found) {
            try {
                flushPage(pid);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            throw new DbException("No clean page to evict!");
        }

    }

}
