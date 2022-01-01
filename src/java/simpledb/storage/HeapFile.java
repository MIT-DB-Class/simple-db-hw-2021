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

    private File f;
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
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return this.f;
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
        return f.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // some code goes here
        int tableId = pid.getTableId();
        int pgNo = pid.getPageNumber();
        byte[] data = HeapPage.createEmptyPageData();
        try(RandomAccessFile file = new RandomAccessFile(f, "r")) {
            file.seek(pgNo * BufferPool.getPageSize());
            file.read(data,0,BufferPool.getPageSize());
            return new HeapPage(new HeapPageId(tableId, pgNo), data);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int) Math.ceil(f.length() / BufferPool.getPageSize());
    }

    // see DbFile.java for javadocs
    public List<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }
    class HeapFileIterator implements DbFileIterator{

        private TransactionId tid;

        private int curPage;

        private Iterator<Tuple> it;

        public HeapFileIterator(TransactionId tid){
            this.tid = tid;
        }

        public Iterator<Tuple> getTupleItertor(int curPage) throws TransactionAbortedException, DbException {
            if(curPage >= 0 && curPage < numPages()){
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid,new HeapPageId(getId(), curPage),Permissions.READ_ONLY);
                return page.iterator();
            }
            throw new DbException("没有curPage 对应的 iterator ***** from HeapFileIterator.getTupleIterator()");
        }

        @Override
        public void open() throws DbException, TransactionAbortedException {
            curPage = 0;
            it = getTupleItertor(curPage);
        }

        @Override
        public boolean hasNext() throws DbException, TransactionAbortedException {
            if(it == null) return false;
            if(!it.hasNext()){
                if(curPage < numPages() - 1){
                    curPage ++;
                    it = getTupleItertor(curPage);
                    return hasNext();
                }else{
                    return false;
                }
            }else {
                return true;
            }
        }

        @Override
        public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
            if(it != null && it.hasNext()){
                return it.next();
            }
            throw new NoSuchElementException("没有下一个tuple了 ***** from HeapFileIterator.next()");
        }

        @Override
        public void rewind() throws DbException, TransactionAbortedException {
            open();
        }

        @Override
        public void close() {
            it = null;
        }
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new HeapFileIterator(tid);
    }

}

