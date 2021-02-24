package simpledb;

import static org.junit.Assert.assertEquals;

import java.util.NoSuchElementException;
import java.util.Random;

import org.junit.Assert;
import junit.framework.JUnit4TestAdapter;

import org.junit.Before;
import org.junit.Test;

import simpledb.TestUtil.SkeletonFile;
import simpledb.common.Database;
import simpledb.common.Utility;
import simpledb.storage.DbFile;
import simpledb.storage.TupleDesc;
import simpledb.systemtest.SimpleDbTestBase;
import simpledb.systemtest.SystemTestUtil;

public class CatalogTest extends SimpleDbTestBase {
	private static final Random r = new Random();
    private static final String name = SystemTestUtil.getUUID();
    private static final int id1 = r.nextInt();
    private static final int id2 = r.nextInt();
	private String nameThisTestRun;
    
    @Before public void addTables() {
        Database.getCatalog().clear();
		nameThisTestRun = SystemTestUtil.getUUID();
        Database.getCatalog().addTable(new SkeletonFile(id1, Utility.getTupleDesc(2)), nameThisTestRun);
        Database.getCatalog().addTable(new SkeletonFile(id2, Utility.getTupleDesc(2)), name);
    }

    /**
     * Unit test for Catalog.getTupleDesc()
     */
    @Test public void getTupleDesc() {
        TupleDesc expected = Utility.getTupleDesc(2);
        TupleDesc actual = Database.getCatalog().getTupleDesc(id1);

        assertEquals(expected, actual);
    }

    /**
     * Unit test for Catalog.getTableId()
     */
    @Test public void getTableId() {
        assertEquals(id2, Database.getCatalog().getTableId(name));
        assertEquals(id1, Database.getCatalog().getTableId(nameThisTestRun));
        
        try {
            Database.getCatalog().getTableId(null);
            Assert.fail("Should not find table with null name");
        } catch (NoSuchElementException e) {
            // Expected to get here
        }
        
        try {
            Database.getCatalog().getTableId("foo");
            Assert.fail("Should not find table with name foo");
        } catch (NoSuchElementException e) {
            // Expected to get here
        }
    }

    /**
     * Unit test for Catalog.getDatabaseFile()
     */

    @Test public void getDatabaseFile() {
        DbFile f = Database.getCatalog().getDatabaseFile(id1);

        // NOTE(ghuo): we try not to dig too deeply into the DbFile API here; we
        // rely on HeapFileTest for that. perform some basic checks.
        assertEquals(id1, f.getId());
    }
    
    /**
     * Check that duplicate names are handled correctly
     */
    @Test public void handleDuplicateNames() {
    	int id3 = r.nextInt();
    	Database.getCatalog().addTable(new SkeletonFile(id3, Utility.getTupleDesc(2)), name);
    	assertEquals(id3, Database.getCatalog().getTableId(name));
    }
    
    /**
     * Check that duplicate file ids are handled correctly
     */
    @Test public void handleDuplicateIds() {
    	String newName = SystemTestUtil.getUUID();
    	DbFile f = new SkeletonFile(id2, Utility.getTupleDesc(2));
    	Database.getCatalog().addTable(f, newName);
    	assertEquals(newName, Database.getCatalog().getTableName(id2));
    	assertEquals(f, Database.getCatalog().getDatabaseFile(id2));
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(CatalogTest.class);
    }
}

