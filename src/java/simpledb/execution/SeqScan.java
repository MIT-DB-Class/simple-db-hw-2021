package simpledb.execution;

import simpledb.common.Database;
import simpledb.storage.DbFile;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.Type;
import simpledb.common.DbException;
import simpledb.storage.DbFileIterator;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import javax.xml.crypto.Data;
import java.util.*;

/**
 * SeqScan is an implementation of a sequential scan access method that reads
 * each tuple of a table in no particular order (e.g., as they are laid out on
 * disk).
 */
public class SeqScan implements OpIterator {

    private static final long serialVersionUID = 1L;

    private TransactionId tid;
    private int tableId;
    private String tableAlias;
    private DbFileIterator iterator;
    private boolean opened;

    /**
     * Creates a sequential scan over the specified table as a part of the
     * specified transaction.
     *
     * @param tid
     *            The transaction this scan is running as a part of.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public SeqScan(TransactionId tid, int tableid, String tableAlias) {
        // some code goes here
        this.tid = tid;
        this.tableId = tableid;
        if (tableAlias == null) {
            this.tableAlias = "null";
        } else {
            this.tableAlias = tableAlias;
        }
        this.iterator = Database.getCatalog().getDatabaseFile(tableId).iterator(tid);
        this.opened = false;
    }

    /**
     * @return
     *       return the table name of the table the operator scans. This should
     *       be the actual name of the table in the catalog of the database
     * */
    public String getTableName() {
        return Database.getCatalog().getTableName(tableId);
    }

    /**
     * @return Return the alias of the table this operator scans.
     * */
    public String getAlias() {
        // some code goes here
        return tableAlias;
    }

    /**
     * Reset the tableid, and tableAlias of this operator.
     * @param tableid
     *            the table to scan.
     * @param tableAlias
     *            the alias of this table (needed by the parser); the returned
     *            tupleDesc should have fields with name tableAlias.fieldName
     *            (note: this class is not responsible for handling a case where
     *            tableAlias or fieldName are null. It shouldn't crash if they
     *            are, but the resulting name can be null.fieldName,
     *            tableAlias.null, or null.null).
     */
    public void reset(int tableid, String tableAlias) {
        // some code goes here
        iterator.close();
        this.tableId = tableid;
        if (tableAlias == null) {
            this.tableAlias = "null";
        } else {
            this.tableAlias = tableAlias;
        }
    }

    public SeqScan(TransactionId tid, int tableId) {
        this(tid, tableId, Database.getCatalog().getTableName(tableId));
    }

    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        if (opened) {
            return;
        }
        iterator.open();
        opened = true;
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
        TupleDesc baseTd = Database.getCatalog().getTupleDesc(tableId);
        int numFields = baseTd.numFields();
        Type[] typeAr = new Type[numFields];
        String[] nameAr = new String[numFields];

        for (int i = 0; i < numFields; i++) {
            typeAr[i] = baseTd.getFieldType(i);
            String fieldName = baseTd.getFieldName(i);
            fieldName = fieldName == null ? "null" : fieldName;
            nameAr[i] = tableAlias + "." + fieldName;
        }

        return new TupleDesc(typeAr, nameAr);
    }

    public boolean hasNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if (!opened) {
            return false;
        }
        return iterator.hasNext();
    }

    public Tuple next() throws NoSuchElementException, TransactionAbortedException, DbException {
        // some code goes here
        if (!opened) {
            throw new NoSuchElementException();
        }
        return iterator.next();
    }

    public void close() {
        // some code goes here
        iterator.close();
        opened = false;
    }

    public void rewind() throws DbException, NoSuchElementException, TransactionAbortedException {
        // some code goes here
        if (!opened) {
            throw new DbException("closed iterator");
        }
        iterator.rewind();
    }
}
