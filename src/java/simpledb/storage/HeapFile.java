package simpledb.storage;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Debug;
import simpledb.common.Permissions;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere to ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return file.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        try {
            RandomAccessFile raf = new RandomAccessFile(file, "r");
            raf.seek(pid.getPageNumber() * BufferPool.getPageSize());
            byte[] buffer = new byte[BufferPool.getPageSize()];
            raf.readFully(buffer);
            raf.close();
            return new HeapPage(new HeapPageId(getId(), pid.getPageNumber()), buffer);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        RandomAccessFile raf = new RandomAccessFile(file, "rws");
        raf.seek(page.getId().getPageNumber() * BufferPool.getPageSize());
        raf.write(page.getPageData(), 0, BufferPool.getPageSize());
        raf.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(file.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        ArrayList<Page> modified = new ArrayList<>();
        HeapPage page = null;

        for (int i = 0; i < numPages(); i++) {
            HeapPageId id = new HeapPageId(getId(), i);
            page = (HeapPage) Database.getBufferPool().getPage(tid, id, Permissions.READ_WRITE);
            if (page.getNumEmptySlots() > 0) {
                break;
            }
            page = null;
        }

        if (page == null) {
            page = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
        }
        page.insertTuple(t);
        if (page.getId().getPageNumber() == numPages()) {
            writePage(page);
        }
        modified.add(page);
        return modified;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_WRITE);
        page.deleteTuple(t);
        ArrayList<Page> modified = new ArrayList<>();
        modified.add(page);
        return modified;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {

        // some code goes here
        return new DbFileIterator() {
            int tableId = getId();
            int pgNo = 0;
            Iterator<Tuple> iterator = null;
            boolean opened = false;

            private void readPage(HeapPageId pid) throws DbException, TransactionAbortedException {
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                iterator = page.iterator();
            }

            @Override
            public void open() throws DbException, TransactionAbortedException {
                if (opened) {
                    return;
                }

                pgNo = 0;
                readPage(new HeapPageId(tableId, pgNo));
                opened = true;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!opened) {
                    return false;
                }
                while (pgNo < numPages() && !iterator.hasNext()) {
                    pgNo++;
                    if (pgNo < numPages()) {
                        readPage(new HeapPageId(tableId, pgNo));
                    }
                }
                return pgNo < numPages() && iterator.hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!opened) {
                    throw new NoSuchElementException();
                }
                return iterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }

            @Override
            public void close() {
                pgNo = 0;
                iterator = null;
                opened = false;
            }
        };
    }

}

