package simpledb.systemtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.execution.Join;
import simpledb.execution.JoinPredicate;
import simpledb.execution.Predicate;
import simpledb.execution.SeqScan;
import simpledb.storage.HeapFile;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class JoinTest extends SimpleDbTestBase {
    private static final int COLUMNS = 2;
    public void validateJoin(int table1ColumnValue, int table1Rows, int table2ColumnValue,
            int table2Rows)
            throws IOException, DbException, TransactionAbortedException {
        // Create the two tables
        Map<Integer, Integer> columnSpecification = new HashMap<>();
        columnSpecification.put(0, table1ColumnValue);
        List<List<Integer>> t1Tuples = new ArrayList<>();
        HeapFile table1 = SystemTestUtil.createRandomHeapFile(
                COLUMNS, table1Rows, columnSpecification, t1Tuples);
        assert t1Tuples.size() == table1Rows;

        columnSpecification.put(0, table2ColumnValue);
        List<List<Integer>> t2Tuples = new ArrayList<>();
        HeapFile table2 = SystemTestUtil.createRandomHeapFile(
                COLUMNS, table2Rows, columnSpecification, t2Tuples);
        assert t2Tuples.size() == table2Rows;

        // Generate the expected results
        List<List<Integer>> expectedResults = new ArrayList<>();
        for (List<Integer> t1 : t1Tuples) {
            for (List<Integer> t2 : t2Tuples) {
                // If the columns match, join the tuples
                if (t1.get(0).equals(t2.get(0))) {
                    List<Integer> out = new ArrayList<>(t1);
                    out.addAll(t2);
                    expectedResults.add(out);
                }
            }
        }

        // Begin the join
        TransactionId tid = new TransactionId();
        SeqScan ss1 = new SeqScan(tid, table1.getId(), "");
        SeqScan ss2 = new SeqScan(tid, table2.getId(), "");
        JoinPredicate p = new JoinPredicate(0, Predicate.Op.EQUALS, 0);
        Join joinOp = new Join(p, ss1, ss2);

        // test the join results
        SystemTestUtil.matchTuples(joinOp, expectedResults);

        joinOp.close();
        Database.getBufferPool().transactionComplete(tid);
    }

    @Test public void testSingleMatch()
            throws IOException, DbException, TransactionAbortedException {
        validateJoin(1, 1, 1, 1);
    }

    @Test public void testNoMatch()
            throws IOException, DbException, TransactionAbortedException {
        validateJoin(1, 2, 2, 10);
    }

    @Test public void testMultipleMatch()
            throws IOException, DbException, TransactionAbortedException {
        validateJoin(1, 3, 1, 3);
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(JoinTest.class);
    }
}
