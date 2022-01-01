package simpledb.systemtest;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Utility;
import simpledb.execution.SeqScan;
import simpledb.storage.*;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;

import org.junit.Test;

import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Dumps the contents of a table.
 * args[1] is the number of columns.  E.g., if it's 5, then ScanTest will end
 * up dumping the contents of f4.0.txt.
 */
public class ScanTest extends SimpleDbTestBase {
    private final static Random r = new Random();

    /** Tests the scan operator for a table with the specified dimensions. */
    private void validateScan(int[] columnSizes, int[] rowSizes)
            throws IOException, DbException, TransactionAbortedException {
        for (int columns : columnSizes) {
            for (int rows : rowSizes) {
                List<List<Integer>> tuples = new ArrayList<>();
                HeapFile f = SystemTestUtil.createRandomHeapFile(columns, rows, null, tuples);
                SystemTestUtil.matchTuples(f, tuples);
                Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
            }
        }
    }

    /** Scan 1-4 columns. */
    @Test public void testSmall() throws IOException, DbException, TransactionAbortedException {
        int[] columnSizes = new int[]{1, 2, 3, 4};
        int[] rowSizes =
                new int[]{0, 1, 2, 511, 512, 513, 1023, 1024, 1025, 4096 + r.nextInt(4096)};
        validateScan(columnSizes, rowSizes);
        System.out.println();
    }

    /** Test that rewinding a SeqScan iterator works. */
    @Test public void testRewind() throws IOException, DbException, TransactionAbortedException {
        List<List<Integer>> tuples = new ArrayList<>();
        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 1000, null, tuples);

        TransactionId tid = new TransactionId();
        SeqScan scan = new SeqScan(tid, f.getId(), "table");
        scan.open();
        for (int i = 0; i < 100; ++i) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(tuples.get(i), SystemTestUtil.tupleToList(t));
        }

        scan.rewind();
        for (int i = 0; i < 100; ++i) {
            assertTrue(scan.hasNext());
            Tuple t = scan.next();
            assertEquals(tuples.get(i), SystemTestUtil.tupleToList(t));
        }
        scan.close();
        Database.getBufferPool().transactionComplete(tid);
    }

    /** Verifies that the buffer pool is actually caching data.
     * @throws TransactionAbortedException
     * @throws DbException */
    @Test public void testCache() throws IOException, DbException, TransactionAbortedException {
        /* Counts the number of readPage operations. */
        class InstrumentedHeapFile extends HeapFile {
            public InstrumentedHeapFile(File f, TupleDesc td) {
                super(f, td);
            }

            @Override
            public Page readPage(PageId pid) throws NoSuchElementException {
                readCount += 1;
                return super.readPage(pid);
            }

            public int readCount = 0;
        }

        // Create the table
        final int PAGES = 30;
        List<List<Integer>> tuples = new ArrayList<>();
        File f = SystemTestUtil.createRandomHeapFileUnopened(1, 992*PAGES, 1000, null, tuples);
        TupleDesc td = Utility.getTupleDesc(1);
        InstrumentedHeapFile table = new InstrumentedHeapFile(f, td);
        Database.getCatalog().addTable(table, SystemTestUtil.getUUID());

        // Scan the table once
        SystemTestUtil.matchTuples(table, tuples);
        assertEquals(PAGES, table.readCount);
        table.readCount = 0;

        // Scan the table again: all pages should be cached
        SystemTestUtil.matchTuples(table, tuples);
        assertEquals(0, table.readCount);
    }

    /** Verifies SeqScan's getTupleDesc prefixes the table name + "." to the field names
     * @throws IOException
     */
    @Test public void testTupleDesc() throws IOException {
        List<List<Integer>> tuples = new ArrayList<>();
        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 1000, null, tuples, "test");

        TransactionId tid = new TransactionId();
        String prefix = "table_alias";
        SeqScan scan = new SeqScan(tid, f.getId(), prefix);

        TupleDesc original = f.getTupleDesc();
        TupleDesc prefixed = scan.getTupleDesc();
        assertEquals(prefix, scan.getAlias());

        // Sanity check the number of fields
        assertEquals(original.numFields(), prefixed.numFields());

        // Check each field for the appropriate tableAlias. prefix
        for (int i = 0; i < original.numFields(); i++) {
           assertEquals(prefix + "." + original.getFieldName(i), prefixed.getFieldName(i));
        }
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(ScanTest.class);
    }
}
