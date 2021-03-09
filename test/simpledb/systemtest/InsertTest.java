package simpledb.systemtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;
import org.junit.Test;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.execution.Insert;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class InsertTest extends SimpleDbTestBase {
    private void validateInsert(int columns, int sourceRows, int destinationRows)
                throws DbException, IOException, TransactionAbortedException {
        // Create the two tables
        List<List<Integer>> sourceTuples = new ArrayList<>();
        HeapFile source = SystemTestUtil.createRandomHeapFile(
                columns, sourceRows, null, sourceTuples);
        assert sourceTuples.size() == sourceRows;
        List<List<Integer>> destinationTuples = new ArrayList<>();
        HeapFile destination = SystemTestUtil.createRandomHeapFile(
                columns, destinationRows, null, destinationTuples);
        assert destinationTuples.size() == destinationRows;

        // Insert source into destination
        TransactionId tid = new TransactionId();
        SeqScan ss = new SeqScan(tid, source.getId(), "");
        Insert insOp = new Insert(tid, ss, destination.getId());

//        Query q = new Query(insOp, tid);
        insOp.open();
        boolean hasResult = false;
        while (insOp.hasNext()) {
            Tuple tup = insOp.next();
            assertFalse(hasResult);
            hasResult = true;
            assertEquals(SystemTestUtil.SINGLE_INT_DESCRIPTOR, tup.getTupleDesc());
            assertEquals(sourceRows, ((IntField) tup.getField(0)).getValue());
        }
        assertTrue(hasResult);
        insOp.close();

        // As part of the same transaction, scan the table
        sourceTuples.addAll(destinationTuples);
        SystemTestUtil.matchTuples(destination, tid, sourceTuples);

        // As part of a different transaction, scan the table
        Database.getBufferPool().transactionComplete(tid);
        Database.getBufferPool().flushAllPages();
        SystemTestUtil.matchTuples(destination, sourceTuples);
    }

    @Test public void testEmptyToEmpty()
            throws IOException, DbException, TransactionAbortedException {
        validateInsert(3, 0, 0);
    }

    @Test public void testEmptyToOne()
            throws IOException, DbException, TransactionAbortedException {
        validateInsert(8, 0, 1);
    }

    @Test public void testOneToEmpty()
            throws IOException, DbException, TransactionAbortedException {
        validateInsert(3, 1, 0);
    }

    @Test public void testOneToOne()
            throws IOException, DbException, TransactionAbortedException {
        validateInsert(1, 1, 1);
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(InsertTest.class);
    }
}
