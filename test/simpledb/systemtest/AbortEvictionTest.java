package simpledb.systemtest;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Collections;

import org.junit.Test;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Utility;
import simpledb.execution.Insert;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.Transaction;
import simpledb.transaction.TransactionAbortedException;

public class AbortEvictionTest extends SimpleDbTestBase {
    // Note: This is a direct copy of the EvictTest method,
    // but the autograder won't necessarily have EvictTest when it runs
    // AbortEvictionTest, so we can't just cal the EvictTest method
    public static void insertRow(HeapFile f, Transaction t) throws DbException,
            TransactionAbortedException {
        // Create a row to insert
        TupleDesc twoIntColumns = Utility.getTupleDesc(2);
        Tuple value = new Tuple(twoIntColumns);
        value.setField(0, new IntField(-42));
        value.setField(1, new IntField(-43));
        TupleIterator insertRow = new TupleIterator(Utility.getTupleDesc(2), Collections.singletonList(value));

        // Insert the row
        Insert insert = new Insert(t.getId(), insertRow, f.getId());
        insert.open();
        Tuple result = insert.next();
        assertEquals(SystemTestUtil.SINGLE_INT_DESCRIPTOR, result.getTupleDesc());
        assertEquals(1, ((IntField)result.getField(0)).getValue());
        assertFalse(insert.hasNext());
        insert.close();
    }

    // Note: This is a direct copy of the EvictTest method,
    // but the autograder won't necessarily have EvictTest when it runs
    // AbortEvictionTest, so we can't just cal the EvictTest method
    public static boolean findMagicTuple(HeapFile f, Transaction t)
            throws DbException, TransactionAbortedException {
        SeqScan ss = new SeqScan(t.getId(), f.getId(), "");
        boolean found = false;
        ss.open();
        while (ss.hasNext()) {
            Tuple v = ss.next();
            int v0 = ((IntField)v.getField(0)).getValue();
            int v1 = ((IntField)v.getField(1)).getValue();
            if (v0 == -42 && v1 == -43) {
                assertFalse(found);
                found = true;
            }
        }
        ss.close();
        return found;
    }

    /** Aborts a transaction and ensures that its effects were actually undone.
     * This requires dirty pages to <em>not</em> get flushed to disk.
     */
    @Test public void testDoNotEvictDirtyPages()
            throws IOException, DbException, TransactionAbortedException {
        // Allocate a file with ~10 pages of data
        HeapFile f = SystemTestUtil.createRandomHeapFile(2, 512*10, null, null);
        Database.resetBufferPool(2);

        // BEGIN TRANSACTION
        Transaction t = new Transaction();
        t.start();

        // Insert a new row
        AbortEvictionTest.insertRow(f, t);

        // The tuple must exist in the table
        boolean found = AbortEvictionTest.findMagicTuple(f, t);
        assertTrue(found);
        // ABORT
        t.transactionComplete(true);

        // A second transaction must not find the tuple
        t = new Transaction();
        t.start();
        found = AbortEvictionTest.findMagicTuple(f, t);
        assertFalse(found);
        t.commit();
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(AbortEvictionTest.class);
    }
}
