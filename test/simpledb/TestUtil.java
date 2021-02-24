package simpledb;

import simpledb.common.*;
import simpledb.execution.OpIterator;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

import java.io.*;
import java.util.*;

import static org.junit.Assert.*;

public class TestUtil {
    /**
     * @return an IntField with value n
     */
    public static Field getField(int n) {
        return new IntField(n);
    }

    /**
     * @return a OpIterator over a list of tuples constructed over the data
     *   provided in the constructor. This iterator is already open.
     * @param width the number of fields in each tuple
     * @param tupdata an array such that the ith element the jth tuple lives
     *   in slot j * width + i
     * @require tupdata.length % width == 0
     */
    public static TupleIterator createTupleList(int width, int[] tupdata) {
        int i = 0;
        List<Tuple> tuplist = new ArrayList<>();
        while (i < tupdata.length) {
            Tuple tup = new Tuple(Utility.getTupleDesc(width));
            for (int j = 0; j < width; ++j)
                tup.setField(j, getField(tupdata[i++]));
            tuplist.add(tup);
        }

        TupleIterator result = new TupleIterator(Utility.getTupleDesc(width), tuplist);
        result.open();
        return result;
    }

    /**
     * @return a OpIterator over a list of tuples constructed over the data
     *   provided in the constructor. This iterator is already open.
     * @param width the number of fields in each tuple
     * @param tupdata an array such that the ith element the jth tuple lives
     *   in slot j * width + i.  Objects can be strings or ints;  tuples must all be of same type.
     * @require tupdata.length % width == 0
     */
    public static TupleIterator createTupleList(int width, Object[] tupdata) {
        List<Tuple> tuplist = new ArrayList<>();
        TupleDesc td;
        Type[] types = new Type[width];
        int i= 0;
        for (int j = 0; j < width; j++) {
            if (tupdata[j] instanceof String) {
                types[j] = Type.STRING_TYPE;
            }
            if (tupdata[j] instanceof Integer) {
                types[j] = Type.INT_TYPE;
            }
        }
        td = new TupleDesc(types);

        while (i < tupdata.length) {
            Tuple tup = new Tuple(td);
            for (int j = 0; j < width; j++) {
                Field f;
                Object t = tupdata[i++];
                if (t instanceof String)
                    f = new StringField((String)t, Type.STRING_LEN); 
                else
                    f = new IntField((Integer)t);

                tup.setField(j, f);
            }
            tuplist.add(tup);
        }

        TupleIterator result = new TupleIterator(td, tuplist);
        result.open();
        return result;
    }

    /**
     * @return true iff the tuples have the same number of fields and
     *   corresponding fields in the two Tuples are all equal.
     */
    public static boolean compareTuples(Tuple t1, Tuple t2) {
        if (t1.getTupleDesc().numFields() != t2.getTupleDesc().numFields())
            return false;

        for (int i = 0; i < t1.getTupleDesc().numFields(); ++i) {
            if (!(t1.getTupleDesc().getFieldType(i).equals(t2.getTupleDesc().getFieldType(i))))
                return false;
            if (!(t1.getField(i).equals(t2.getField(i))))
                return false;
        }

        return true;
    }

    /**
     * Check to see if the DbIterators have the same number of tuples and
     *   each tuple pair in parallel iteration satisfies compareTuples .
     * If not, throw an assertion.
     */
    public static void compareDbIterators(OpIterator expected, OpIterator actual)
            throws DbException, TransactionAbortedException {
        while (expected.hasNext()) {
            assertTrue(actual.hasNext());

            Tuple expectedTup = expected.next();
            Tuple actualTup = actual.next();
            assertTrue(compareTuples(expectedTup, actualTup));
        }
        // Both must now be exhausted
        assertFalse(expected.hasNext());
        assertFalse(actual.hasNext());
    }

    /**
     * Check to see if every tuple in expected matches <b>some</b> tuple
     *   in actual via compareTuples. Note that actual may be a superset.
     * If not, throw an assertion.
     */
    public static void matchAllTuples(OpIterator expected, OpIterator actual) throws
            DbException, TransactionAbortedException {
        // TODO(ghuo): this n^2 set comparison is kind of dumb, but we haven't
        // implemented hashCode or equals for tuples.
        boolean matched = false;
        while (expected.hasNext()) {
            Tuple expectedTup = expected.next();
            matched = false;
            actual.rewind();

            while (actual.hasNext()) {
                Tuple next = actual.next();
                if (compareTuples(expectedTup, next)) {
                    matched = true;
                    break;
                }
            }

            if (!matched) {
                throw new RuntimeException("expected tuple not found: " + expectedTup);
            }
        }
    }

    /**
     * Verifies that the OpIterator has been exhausted of all elements.
     */
    public static boolean checkExhausted(OpIterator it)
        throws TransactionAbortedException, DbException {

        if (it.hasNext()) return false;

        try {
            Tuple t = it.next();
            System.out.println("Got unexpected tuple: " + t);
            return false;
        } catch (NoSuchElementException e) {
            return true;
        }
    }

    /**
     * @return a byte array containing the contents of the file 'path'
     */
    public static byte[] readFileBytes(String path) throws IOException {
        File f = new File(path);
        InputStream is = new FileInputStream(f);
        byte[] buf = new byte[(int) f.length()];

        int offset = 0;
        int count = 0;
        while (offset < buf.length
               && (count = is.read(buf, offset, buf.length - offset)) >= 0) {
            offset += count;
        }

        // check that we grabbed the entire file
        if (offset < buf.length)
            throw new IOException("failed to read test data");

        // Close the input stream and return bytes
        is.close();
        return buf;
    }

    /**
     * Stub DbFile class for unit testing.
     */
    public static class SkeletonFile implements DbFile {
        private final int tableid;
        private final TupleDesc td;

        public SkeletonFile(int tableid, TupleDesc td) {
            this.tableid = tableid;
            this.td = td;
        }

        public Page readPage(PageId id) throws NoSuchElementException {
            throw new RuntimeException("not implemented");
        }

        public int numPages() {
            throw new RuntimeException("not implemented");
        }

        public void writePage(Page p) {
            throw new RuntimeException("not implemented");
        }

        public List<Page> insertTuple(TransactionId tid, Tuple t) {
            throw new RuntimeException("not implemented");
        }

        public List<Page> deleteTuple(TransactionId tid, Tuple t) {
            throw new RuntimeException("not implemented");
        }

        public int bytesPerPage() {
            throw new RuntimeException("not implemented");
        }

        public int getId() {
            return tableid;
        }

        public DbFileIterator iterator(TransactionId tid) {
            throw new RuntimeException("not implemented");
        }

		public TupleDesc getTupleDesc() {			
			return td;
		}
    }

    /**
     * Mock SeqScan class for unit testing.
     */
    public static class MockScan implements OpIterator {
        private int cur;
        private final int low;
        private final int high;
        private final int width;

        /**
         * Creates a fake SeqScan that returns tuples sequentially with 'width'
         * fields, each with the same value, that increases from low (inclusive)
         * and high (exclusive) over getNext calls.
         */
        public MockScan(int low, int high, int width) {
            this.low = low;
            this.high = high;
            this.width = width;
            this.cur = low;
        }

        public void open() {
            cur = low;
        }

        public void close() {
        }

        public void rewind() {
            cur = low;
        }

        public TupleDesc getTupleDesc() {
            return Utility.getTupleDesc(width);
        }

        protected Tuple readNext() {
            if (cur >= high) return null;

            Tuple tup = new Tuple(getTupleDesc());
            for (int i = 0; i < width; ++i)
                tup.setField(i, new IntField(cur));
            cur++;
            return tup;
        }

		public boolean hasNext() {
            return cur < high;
        }

		public Tuple next() throws NoSuchElementException {
			if(cur >= high) throw new NoSuchElementException();
            Tuple tup = new Tuple(getTupleDesc());
            for (int i = 0; i < width; ++i)
                tup.setField(i, new IntField(cur));
            cur++;
            return tup;
		}
    }

    /**
     * Helper class that attempts to acquire a lock on a given page in a new
     * thread.
     *
     * @return a handle to the Thread that will attempt lock acquisition after it
     *   has been started
     */
    static class LockGrabber extends Thread {

        final TransactionId tid;
        final PageId pid;
        final Permissions perm;
        boolean acquired;
        Exception error;
        final Object alock;
        final Object elock;

        /**
         * @param tid the transaction on whose behalf we want to acquire the lock
         * @param pid the page over which we want to acquire the lock
         * @param perm the desired lock permissions
         */
        public LockGrabber(TransactionId tid, PageId pid, Permissions perm) {
            this.tid = tid;
            this.pid = pid;
            this.perm = perm;
            this.acquired = false;
            this.error = null;
            this.alock = new Object();
            this.elock = new Object();
        }

        public void run() {
            try {
                Database.getBufferPool().getPage(tid, pid, perm);
                synchronized(alock) {
                    acquired = true;
                }
            } catch (Exception e) {
                e.printStackTrace();
                synchronized(elock) {
                    error = e;
                }

                Database.getBufferPool().transactionComplete(tid, false);
            }
        }

        /**
         * @return true if we successfully acquired the specified lock
         */
        public boolean acquired() {
            synchronized(alock) {
                return acquired;
            }
        }

        /**
         * @return an Exception instance if one occured during lock acquisition;
         *   null otherwise
         */
        public Exception getError() {
            synchronized(elock) {
                return error;
            }
        }
    }

    /** JUnit fixture that creates a heap file and cleans it up afterward. */
    public static abstract class CreateHeapFile {
        protected CreateHeapFile() {
            try{
                emptyFile = File.createTempFile("empty", ".dat");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            emptyFile.deleteOnExit();
        }

        protected void setUp() throws Exception {
            try{
            	Database.reset();
                empty = Utility.createEmptyHeapFile(emptyFile.getAbsolutePath(), 2);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        protected HeapFile empty;
        private final File emptyFile;
    }
}
