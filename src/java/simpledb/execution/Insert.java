package simpledb.execution;

import simpledb.common.Database;
import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;

/**
 * Inserts tuples read from the child operator into the tableId specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;
    private final TransactionId tid;
    private OpIterator child;
    private final int tableId;
    private Tuple result = null;
    private final TupleDesc td = new TupleDesc(new Type[]{Type.INT_TYPE});

    /**
     * Constructor.
     *
     * @param t       The transaction running the insert.
     * @param child   The child operator from which to read tuples to be inserted.
     * @param tableId The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to
     *                     insert.
     */
    public Insert(TransactionId t, OpIterator child, int tableId)
            throws DbException {
        // some code goes here
        tid = t;
        this.child = child;
        this.tableId = tableId;
        if (!child.getTupleDesc().equals(Database.getCatalog().getTupleDesc(tableId)))
            throw new DbException("TupleDesc of child differs from table into which insertion is carried out");
    }

    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    @Override
    public void open() throws DbException, TransactionAbortedException {
        // some code goes here
        super.open();
        child.open();
    }

    @Override
    public void close() {
        // some code goes here
        result = null;
        child.close();
        super.close();
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        result = null;
        child.rewind();
    }

    /**
     * Inserts tuples read from child into the tableId specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
     * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        int insertNum = 0;
        if (result == null) {
            while (child.hasNext())
                try {
                    Database.getBufferPool().insertTuple(tid, tableId, child.next());
                    ++insertNum;
                } catch (Exception e) {
                    e.printStackTrace();
                }
            result = new Tuple(td);
            result.setField(0, new IntField(insertNum));
            return result;
        } else
            return null;
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[]{child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }
}
