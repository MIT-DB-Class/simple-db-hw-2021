package simpledb.systemtest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.execution.Aggregate;
import simpledb.execution.Aggregator;
import simpledb.execution.SeqScan;
import simpledb.storage.DbFile;
import simpledb.storage.HeapFile;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class AggregateTest extends SimpleDbTestBase {
    public void validateAggregate(DbFile table, Aggregator.Op operation, int aggregateColumn, int groupColumn, List<List<Integer>> expectedResult)
            throws DbException, TransactionAbortedException {
        TransactionId tid = new TransactionId();
        SeqScan ss = new SeqScan(tid, table.getId(), "");
        Aggregate ag = new Aggregate(ss, aggregateColumn, groupColumn, operation);

        SystemTestUtil.matchTuples(ag, expectedResult);
        Database.getBufferPool().transactionComplete(tid);
    }

    private int computeAggregate(List<Integer> values, Aggregator.Op operation) {
        if (operation == Aggregator.Op.COUNT) return values.size();

        int value = 0;
        if (operation == Aggregator.Op.MIN) value = Integer.MAX_VALUE;
        else if (operation == Aggregator.Op.MAX) value = Integer.MIN_VALUE;

        for (int v : values) {
            switch (operation) {
                case MAX:
                    if (v > value) value = v;
                    break;
                case MIN:
                    if (v < value) value = v;
                    break;
                case AVG:
                case SUM:
                    value += v;
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported operation " + operation);
            }
        }

        if (operation == Aggregator.Op.AVG) value /= values.size();
        return value;
    }

    private List<List<Integer>> aggregate(List<List<Integer>> tuples, Aggregator.Op operation, int groupColumn) {
        // Group the values
        Map<Integer, List<Integer>> values = new HashMap<>();
        for (List<Integer> t : tuples) {
            Integer key = null;
            if (groupColumn != Aggregator.NO_GROUPING) key = t.get(groupColumn);
            Integer value = t.get(1);

            if (!values.containsKey(key)) values.put(key, new ArrayList<>());
            values.get(key).add(value);
        }

        List<List<Integer>> results = new ArrayList<>();
        for (Map.Entry<Integer, List<Integer>> e : values.entrySet()) {
            List<Integer> result = new ArrayList<>();
            if (groupColumn != Aggregator.NO_GROUPING) result.add(e.getKey());
            result.add(computeAggregate(e.getValue(), operation));
            results.add(result);
        }
        return results;
    }

    private final static int ROWS = 1024;
    private final static int MAX_VALUE = 64;
    private final static int COLUMNS = 3;
    private void doAggregate(Aggregator.Op operation, int groupColumn)
            throws IOException, DbException, TransactionAbortedException {
        // Create the table
        List<List<Integer>> createdTuples = new ArrayList<>();
        HeapFile table = SystemTestUtil.createRandomHeapFile(
                COLUMNS, ROWS, MAX_VALUE, null, createdTuples);

        // Compute the expected answer
        List<List<Integer>> expected =
                aggregate(createdTuples, operation, groupColumn);

        // validate that we get the answer
        validateAggregate(table, operation, 1, groupColumn, expected);
    }

    @Test public void testSum() throws IOException, DbException, TransactionAbortedException {
        doAggregate(Aggregator.Op.SUM, 0);
    }

    @Test public void testMin() throws IOException, DbException, TransactionAbortedException {
        doAggregate(Aggregator.Op.MIN, 0);
    }

    @Test public void testMax() throws IOException, DbException, TransactionAbortedException {
        doAggregate(Aggregator.Op.MAX, 0);
    }

    @Test public void testCount() throws IOException, DbException, TransactionAbortedException {
        doAggregate(Aggregator.Op.COUNT, 0);
    }

    @Test public void testAverage() throws IOException, DbException, TransactionAbortedException {
        doAggregate(Aggregator.Op.AVG, 0);
    }

    @Test public void testAverageNoGroup()
            throws IOException, DbException, TransactionAbortedException {
        doAggregate(Aggregator.Op.AVG, Aggregator.NO_GROUPING);
    }

    /** Make test compatible with older version of ant. */
    public static junit.framework.Test suite() {
        return new junit.framework.JUnit4TestAdapter(AggregateTest.class);
    }
}
