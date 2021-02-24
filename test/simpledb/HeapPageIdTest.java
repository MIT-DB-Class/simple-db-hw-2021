package simpledb;

import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.storage.HeapPageId;
import simpledb.systemtest.SimpleDbTestBase;

import static org.junit.Assert.*;

public class HeapPageIdTest extends SimpleDbTestBase {

    private HeapPageId pid;

    @Before public void createPid() {
        pid = new HeapPageId(1, 1);
    }

    /**
     * Unit test for HeapPageId.getTableId()
     */
    @Test public void getTableId() {
        assertEquals(1, pid.getTableId());
    }

    /**
     * Unit test for HeapPageId.pageno()
     */
    @Test public void pageno() {
        assertEquals(1, pid.getPageNumber());
    }

    /**
     * Unit test for HeapPageId.hashCode()
     */
    @Test public void testHashCode() {
        int code1, code2;

        // NOTE(ghuo): the hashCode could be anything. test determinism,
        // at least.
        pid = new HeapPageId(1, 1);
        code1 = pid.hashCode();
        assertEquals(code1, pid.hashCode());
        assertEquals(code1, pid.hashCode());

        pid = new HeapPageId(2, 2);
        code2 = pid.hashCode();
        assertEquals(code2, pid.hashCode());
        assertEquals(code2, pid.hashCode());
    }

    /**
     * Unit test for HeapPageId.equals()
     */
    @Test public void equals() {
        HeapPageId pid1 = new HeapPageId(1, 1);
        HeapPageId pid1Copy = new HeapPageId(1, 1);
        HeapPageId pid2 = new HeapPageId(2, 2);

        // .equals() with null should return false
        assertNotEquals(null, pid1);

        // .equals() with the wrong type should return false
        assertNotEquals(pid1, new Object());

        assertEquals(pid1, pid1);
        assertEquals(pid1, pid1Copy);
        assertEquals(pid1Copy, pid1);
        assertEquals(pid2, pid2);

        assertNotEquals(pid1, pid2);
        assertNotEquals(pid1Copy, pid2);
        assertNotEquals(pid2, pid1);
        assertNotEquals(pid2, pid1Copy);
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(HeapPageIdTest.class);
    }
}

