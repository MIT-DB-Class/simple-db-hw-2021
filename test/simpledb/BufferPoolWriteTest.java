package simpledb;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Permissions;
import simpledb.common.Utility;
import simpledb.storage.*;
import simpledb.systemtest.SystemTestUtil;
import static org.junit.Assert.*;
import junit.framework.JUnit4TestAdapter;
import simpledb.transaction.TransactionId;

public class BufferPoolWriteTest extends TestUtil.CreateHeapFile {
    private TransactionId tid;

    // class to return multiple dirty pages on insert
    static class HeapFileDuplicates extends HeapFile {

    	private final int duplicates;
    	
    	public HeapFileDuplicates(File f, TupleDesc td, int duplicates) {
    		super(f, td);
    		this.duplicates = duplicates;
    	}
    	
    	// this version of insertTuple inserts duplicate copies of the same tuple,
    	// each on a new page
    	@Override
    	public List<Page> insertTuple(TransactionId tid, Tuple t)
    			throws DbException, IOException {
    		List<Page> dirtypages = new ArrayList<>();
    		for(int i = 0; i < duplicates; i++) {
    			// create a blank page
    			BufferedOutputStream bw = new BufferedOutputStream(new FileOutputStream(super.getFile(), true));
                byte[] emptyData = HeapPage.createEmptyPageData();
                bw.write(emptyData);
                bw.close();
    			HeapPage p = new HeapPage(new HeapPageId(super.getId(), super.numPages() - 1),
    					HeapPage.createEmptyPageData());
    	        p.insertTuple(t);
    			dirtypages.add(p);
    		}
    		return dirtypages;
    	}
    }
    
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
     * Unit test for BufferPool.insertTuple()
     */
    @Test public void insertTuple() throws Exception {
        // we should be able to add 504 tuples on an empty page.
        for (int i = 0; i < 504; ++i) {
        	Tuple t = Utility.getHeapTuple(i, 2);
        	Database.getBufferPool().insertTuple(tid, empty.getId(), t);
        	HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_ONLY);
        	assertEquals(504-i-1, p.getNumEmptySlots());
        }

        // the next 504 additions should live on a new page
        for (int i = 0; i < 504; ++i) {
        	Tuple t = Utility.getHeapTuple(i, 2);
        	Database.getBufferPool().insertTuple(tid, empty.getId(), t);
        	HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_ONLY);
        	assertEquals(504-i-1, p.getNumEmptySlots());
        }
    }
    
    /**
     * Unit test for BufferPool.deleteTuple()
     */
    @Test public void deleteTuple() throws Exception {

    	// heap file should have ~10 pages
    	HeapFile hf = SystemTestUtil.createRandomHeapFile(2, 504*10, null, null);
    	DbFileIterator it = hf.iterator(tid);
    	it.open();
    	
    	List<Tuple> tuples = new ArrayList<>();
    	while(it.hasNext()) {
    		tuples.add(it.next());
    	}
    	
    	// clear the cache
    	Database.resetBufferPool(BufferPool.DEFAULT_PAGES);
        
    	// delete 504 tuples from the first page
    	for (int i = 0; i < 504; ++i) {
    		Tuple t = tuples.get(i);
        	Database.getBufferPool().deleteTuple(tid, t);
        	HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_ONLY);
        	assertEquals(i+1, p.getNumEmptySlots());
        }
    	
    	// delete 504 tuples from the second page
    	for (int i = 0; i < 504; ++i) {
    		Tuple t = tuples.get(i+504);
        	Database.getBufferPool().deleteTuple(tid, t);
        	HeapPage p = (HeapPage) Database.getBufferPool().getPage(tid, t.getRecordId().getPageId(), Permissions.READ_ONLY);
        	assertEquals(i+1, p.getNumEmptySlots());
        }
    }
    
    @Test public void handleManyDirtyPages() throws Exception {
    	HeapFileDuplicates hfd = new HeapFileDuplicates(empty.getFile(), empty.getTupleDesc(), 10);
    	Database.getCatalog().addTable(hfd, SystemTestUtil.getUUID());
    	Database.getBufferPool().insertTuple(tid, hfd.getId(), Utility.getHeapTuple(1, 2));
    	
    	// there should now be 10 tuples (on 10 different pages) in the buffer pool
    	DbFileIterator it = hfd.iterator(tid);
    	it.open();
    	
    	int count = 0;
    	while(it.hasNext()) {
    		it.next();
    		count++;
    	}
    	assertEquals(10, count);
    }

    /**
     * JUnit suite target
     */
    public static junit.framework.Test suite() {
        return new JUnit4TestAdapter(BufferPoolWriteTest.class);
    }
}

