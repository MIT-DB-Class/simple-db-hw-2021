package simpledb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;
import simpledb.common.Database;
import simpledb.common.Utility;
import simpledb.storage.*;
import simpledb.systemtest.SystemTestUtil;
import simpledb.transaction.TransactionId;

import java.util.Arrays;

public class HeapFileWriteTest extends TestUtil.CreateHeapFile {
    private TransactionId tid;

    /**
     * Set up initial resources for each unit test.
     */
    @Before public void setUp() throws Exception {
        super.setUp();
        tid = new TransactionId();
    }

    @After public void tearDown() {
        Database.getBufferPool().transactionComplete(tid);
    }

    /**
     * Unit test for HeapFile.addTuple()
     */
    @Test public void addTuple() throws Exception {
        // we should be able to add 504 tuples on an empty page.
        for (int i = 0; i < 504; ++i) {
            empty.insertTuple(tid, Utility.getHeapTuple(i, 2));
            assertEquals(1, empty.numPages());
        }

        // the next 512 additions should live on a new page
        for (int i = 0; i < 504; ++i) {
            empty.insertTuple(tid, Utility.getHeapTuple(i, 2));
            assertEquals(2, empty.numPages());
        }

        // and one more, just for fun...
        empty.insertTuple(tid, Utility.getHeapTuple(0, 2));
        assertEquals(3, empty.numPages());
    }

    @Test
    public void testAlternateEmptyAndFullPagesThenIterate() throws Exception {
        // Create HeapFile/Table
        HeapFile smallFile = SystemTestUtil.createRandomHeapFile(2, 3, null,
                null);
        // Grab table id
        int tableId = smallFile.getId();
        int tdSize = 8;
        int numTuples = (BufferPool.getPageSize()*8) / (tdSize * 8 + 1);
        int headerSize = (int) Math.ceil(numTuples / 8.0);
        // Leave these as all zeroes so this entire page is empty
        byte[] empty = new byte[numTuples * 8 + headerSize];
        byte[] full = new byte[numTuples * 8 + headerSize];
        // Since every bit is marked as used, every tuple should be used,
        // and all should be set to -1.
        Arrays.fill(full, (byte) 0xFFFFFFFF);

        // The first two pages and the fourth page are empty and should be skipped
        // while still continuing on to read the third and fifth page.
        // Hint: You can see this in HeapFile's iterator right after assigning the
        // next HeapPage's iterator and checking if it's empty (hasNext()), making
        // sure it moves onto the next page until hitting the final page.
        smallFile.writePage(new HeapPage(new HeapPageId(tableId, 0), empty));
        smallFile.writePage(new HeapPage(new HeapPageId(tableId, 1), empty));
        smallFile.writePage(new HeapPage(new HeapPageId(tableId, 2), full));
        smallFile.writePage(new HeapPage(new HeapPageId(tableId, 3), empty));
        smallFile.writePage(new HeapPage(new HeapPageId(tableId, 4), full));
        DbFileIterator it = smallFile.iterator(tid);
        it.open();
        int count = 0;
        while (it.hasNext()) {
            Tuple t = it.next();
            assertNotNull(t);
            count += 1;
        }
        // Since we have two full pages, we should see all of 2*numTuples.
        assertEquals(2*numTuples, count);
        it.close();
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapFileWriteTest.class);
    }
}

