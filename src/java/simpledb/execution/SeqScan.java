package simpledb.execution;

import simpledb.common.Catalog;
import simpledb.common.Database;
import simpledb.storage.DbFile;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private final Catalog catalog;
    public int tableId;
    private TupleDesc tupleDesc;
    private String tableName;
    private final TransactionId tid;
    private String tableAlias;
    private DbFile file;
    private DbFileIterator iterator;
    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableId
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableId, String tableAlias) {
        // some code goes here
        this.catalog = Database.getCatalog();
        this.tableId = tableId;
        this.tableName = catalog.getTableName(tableId);
        this.tableAlias = tableAlias;
        this.file = catalog.getDatabaseFile(tableId);
        this.tid = tid;
        this.tupleDesc = changeTupleDesc(catalog.getTupleDesc(tableId), tableAlias);
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }


    public TupleDesc changeTupleDesc(TupleDesc tupleDesc, String alias ){
        TupleDesc desc = new TupleDesc();
        List<TupleDesc.TDItem> tdItems = tupleDesc.tdItems;
        List<TupleDesc.TDItem> items = new ArrayList<>();
        for(TupleDesc.TDItem item: tdItems){
            TupleDesc.TDItem newItem = new TupleDesc.TDItem(item.fieldType, alias + "." + item.fieldName);
            items.add(newItem);
        }
        desc.setTdItems(items);
        return desc;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return tableName;
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias()
    {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableId
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableId, String tableAlias) {
        this.tableId = tableId;
        this.tableAlias = tableAlias;
        this.tupleDesc = changeTupleDesc(catalog.getTupleDesc(tableId), tableAlias);
        this.tableName = catalog.getTableName(tableId);
        this.file = catalog.getDatabaseFile(tableId);
        try {
            open();
        } catch (DbException | TransactionAbortedException e){
            e.printStackTrace();
        }
    }


    public void open() throws DbException, TransactionAbortedException {
        iterator = file.iterator(this.tid);
        iterator.open();
    }

    /**
     * Returns the TupleDesc with field names from the underlying HeapFile,
     * prefixed with the tableAlias string from the constructor. This prefix
     * becomes useful when joining tables containing a field(s) with the same
     * name.  The alias and name should be separated with a "." character
     * (e.g., "alias.fieldName").
     *
     * @return the TupleDesc with field names from the underlying HeapFile,
     *         prefixed with the tableAlias string from the constructor.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(iterator == null) return false;
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException,
            TransactionAbortedException, DbException {
        // some code goes here
        if(iterator == null) throw new NoSuchElementException("No next tuple");
        Tuple tuple = iterator.next();
        if(tuple == null) throw new NoSuchElementException("No next tuple");
        return tuple;
    }

    public void close() {
        iterator = null;
    }

    public void rewind() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        iterator.rewind();
    }
}
