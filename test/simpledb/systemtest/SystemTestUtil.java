package simpledb.systemtest;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import org.junit.Assert;

import simpledb.common.*;
import simpledb.execution.OpIterator;
import simpledb.execution.SeqScan;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

public class SystemTestUtil {
    public static final TupleDesc SINGLE_INT_DESCRIPTOR =
            new TupleDesc(new Type[]{Type.INT_TYPE});

    private static final int MAX_RAND_VALUE = 1 << 16;

    /** @param columnSpecification Mapping between column index and value. */
    public static HeapFile createRandomHeapFile(
            int columns, int rows, Map<Integer, Integer> columnSpecification,
            List<List<Integer>> tuples)
            throws IOException {
        return createRandomHeapFile(columns, rows, MAX_RAND_VALUE, columnSpecification, tuples);
    }

    /** @param columnSpecification Mapping between column index and value. */
    public static HeapFile createRandomHeapFile(
            int columns, int rows, int maxValue, Map<Integer, Integer> columnSpecification,
            List<List<Integer>> tuples)
            throws IOException {
        File temp = createRandomHeapFileUnopened(columns, rows, maxValue,
                columnSpecification, tuples);
        return Utility.openHeapFile(columns, temp);
    }
    
    public static HeapFile createRandomHeapFile(
            int columns, int rows, Map<Integer, Integer> columnSpecification,
            List<List<Integer>> tuples, String colPrefix)
            throws IOException {
        return createRandomHeapFile(columns, rows, MAX_RAND_VALUE, columnSpecification, tuples, colPrefix);
    }
    
    public static HeapFile createRandomHeapFile(
            int columns, int rows, int maxValue, Map<Integer, Integer> columnSpecification,
            List<List<Integer>> tuples, String colPrefix)
            throws IOException {
        File temp = createRandomHeapFileUnopened(columns, rows, maxValue,
                columnSpecification, tuples);
        return Utility.openHeapFile(columns, colPrefix, temp);
    }

    public static File createRandomHeapFileUnopened(int columns, int rows,
            int maxValue, Map<Integer, Integer> columnSpecification,
            List<List<Integer>> tuples) throws IOException {
        if (tuples != null) {
            tuples.clear();
        } else {
            tuples = new ArrayList<>(rows);
        }

        Random r = new Random();

        // Fill the tuples list with generated values
        for (int i = 0; i < rows; ++i) {
            List<Integer> tuple = new ArrayList<>(columns);
            for (int j = 0; j < columns; ++j) {
                // Generate random values, or use the column specification
                Integer columnValue = null;
                if (columnSpecification != null) columnValue = columnSpecification.get(j);
                if (columnValue == null) {
                    columnValue = r.nextInt(maxValue);
                }
                tuple.add(columnValue);
            }
            tuples.add(tuple);
        }

        // Convert the tuples list to a heap file and open it
        File temp = File.createTempFile("table", ".dat");
        temp.deleteOnExit();
        HeapFileEncoder.convert(tuples, temp, BufferPool.getPageSize(), columns);
        return temp;
    }

    public static List<Integer> tupleToList(Tuple tuple) {
        List<Integer> list = new ArrayList<>();
        for (int i = 0; i < tuple.getTupleDesc().numFields(); ++i) {
            int value = ((IntField)tuple.getField(i)).getValue();
            list.add(value);
        }
        return list;
    }

    public static void matchTuples(DbFile f, List<List<Integer>> tuples)
            throws DbException, TransactionAbortedException {
        TransactionId tid = new TransactionId();
        matchTuples(f, tid, tuples);
        Database.getBufferPool().transactionComplete(tid);
    }

    public static void matchTuples(DbFile f, TransactionId tid, List<List<Integer>> tuples)
            throws DbException, TransactionAbortedException {
        SeqScan scan = new SeqScan(tid, f.getId(), "");
        matchTuples(scan, tuples);
    }

    public static void matchTuples(OpIterator iterator, List<List<Integer>> tuples)
            throws DbException, TransactionAbortedException {
        List<List<Integer>> copy = new ArrayList<>(tuples);

        if (Debug.isEnabled()) {
            Debug.log("Expected tuples:");
            for (List<Integer> t : copy) {
                Debug.log("\t" + Utility.listToString(t));
            }
        }

        iterator.open();
        while (iterator.hasNext()) {
            Tuple t = iterator.next();
            List<Integer> list = tupleToList(t);
            boolean isExpected = copy.remove(list);
            Debug.log("scanned tuple: %s (%s)", t, isExpected ? "expected" : "not expected");
            if (!isExpected) {
                Assert.fail("expected tuples does not contain: " + t);
            }
        }
        iterator.close();

        if (!copy.isEmpty()) {
            StringBuilder msg = new StringBuilder("expected to find the following tuples:\n");
            final int MAX_TUPLES_OUTPUT = 10;
            int count = 0;
            for (List<Integer> t : copy) {
                if (count == MAX_TUPLES_OUTPUT) {
                    msg.append("[").append(copy.size() - MAX_TUPLES_OUTPUT).append(" more tuples]");
                    break;
                }
                msg.append("\t").append(Utility.listToString(t)).append("\n");
                count += 1;
            }
            Assert.fail(msg.toString());
        }
    }

    /**
     * Returns number of bytes of RAM used by JVM after calling System.gc many times.
     * @return amount of RAM (in bytes) used by JVM
     */
    public static long getMemoryFootprint() {
        // Call System.gc in a loop until it stops freeing memory. This is
        // still no guarantee that all the memory is freed, since System.gc is
        // just a "hint".
        Runtime runtime = Runtime.getRuntime();
        long memAfter = runtime.totalMemory() - runtime.freeMemory();
        long memBefore = memAfter + 1;
        while (memBefore != memAfter) {
            memBefore = memAfter;
            System.gc();
            memAfter = runtime.totalMemory() - runtime.freeMemory();
        }

        return memAfter;
    }
	
	/**
	 * Generates a unique string each time it is called.
	 * @return a new unique UUID as a string, using java.util.UUID
	 */
	public static String getUUID() {
		return UUID.randomUUID().toString();
	}
	
	private static double[] getDiff(double[] sequence) {
		double[] ret = new double[sequence.length - 1];
		for (int i = 0; i < sequence.length - 1; ++i)
			ret[i] = sequence[i + 1] - sequence[i];
		return ret;
	}
	/**
	 * Checks if the sequence represents a quadratic sequence (approximately)
	 * ret[0] is true if the sequence is quadratic
	 * ret[1] is the common difference of the sequence if ret[0] is true.
	 * @param sequence
	 * @return ret[0] = true if sequence is qudratic(or sub-quadratic or linear), ret[1] = the coefficient of n^2
	 */
	public static Object[] checkQuadratic(double[] sequence) {
		Object[] ret = checkLinear(getDiff(sequence));
		ret[1] = (Double)ret[1]/2.0;
		return ret;
	}
	
	/**
	 * Checks if the sequence represents an arithmetic sequence (approximately)
	 * ret[0] is true if the sequence is linear
	 * ret[1] is the common difference of the sequence if ret[0] is true.
	 * @param sequence
	 * @return ret[0] = true if sequence is linear, ret[1] = the common difference
	 */
	public static Object[] checkLinear(double[] sequence) {				
		return checkConstant(getDiff(sequence));
	}
	
	/**
	 * Checks if the sequence represents approximately a fixed sequence (c,c,c,c,..)
	 * ret[0] is true if the sequence is linear
	 * ret[1] is the constant of the sequence if ret[0] is true.
	 * @param sequence
	 * @return ret[0] = true if sequence is constant, ret[1] = the constant
	 */
	public static Object[] checkConstant(double[] sequence) {
		Object[] ret = new Object[2];
		//compute average
		double sum = .0;
        for (double value : sequence) sum += value;
		double av = sum/(sequence.length + .0);
		//compute standard deviation
		double sqsum = 0;
        for (double v : sequence) sqsum += (v - av) * (v - av);
		double std = Math.sqrt(sqsum/(sequence.length + .0));
		ret[0] = std < 1.0 ? Boolean.TRUE : Boolean.FALSE;
		ret[1] = av;
		return ret;
	}
}
