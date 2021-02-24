package simpledb;

import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.storage.HeapPageId;
import simpledb.storage.RecordId;
import simpledb.systemtest.SimpleDbTestBase;

import static org.junit.Assert.*;

public class RecordIdTest extends SimpleDbTestBase {

    private static RecordId hrid;
    private static RecordId hrid2;
    private static RecordId hrid3;
    private static RecordId hrid4;

    @Before public void createPids() {
        HeapPageId hpid = new HeapPageId(-1, 2);
        HeapPageId hpid2 = new HeapPageId(-1, 2);
        HeapPageId hpid3 = new HeapPageId(-2, 2);
        hrid = new RecordId(hpid, 3);
        hrid2 = new RecordId(hpid2, 3);
        hrid3 = new RecordId(hpid, 4);
        hrid4 = new RecordId(hpid3, 3);

    }

    /**
     * Unit test for RecordId.getPageId()
     */
    @Test public void getPageId() {
        HeapPageId hpid = new HeapPageId(-1, 2);
        assertEquals(hpid, hrid.getPageId());

    }

    /**
     * Unit test for RecordId.getTupleNumber()
     */
    @Test public void tupleno() {
        assertEquals(3, hrid.getTupleNumber());
    }
    
    /**
     * Unit test for RecordId.equals()
     */
    @Test public void equals() {
    	assertEquals(hrid, hrid2);
    	assertEquals(hrid2, hrid);
        assertNotEquals(hrid, hrid3);
        assertNotEquals(hrid3, hrid);
        assertNotEquals(hrid2, hrid4);
        assertNotEquals(hrid4, hrid2);
    }
    
    /**
     * Unit test for RecordId.hashCode()
     */
    @Test public void hCode() {
    	assertEquals(hrid.hashCode(), hrid2.hashCode());
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(RecordIdTest.class);
    }
}

