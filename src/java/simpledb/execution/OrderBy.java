package simpledb.execution;

import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;
import simpledb.storage.Field;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.util.*;

/**
 * OrderBy is an operator that implements a relational ORDER BY.
 */
public class OrderBy extends Operator {

    private static final long serialVersionUID = 1L;
    private OpIterator child;
    private final TupleDesc td;
    private final List<Tuple> childTups = new ArrayList<>();
    private final int orderByField;
    private final String orderByFieldName;
    private Iterator<Tuple> it;
    private final boolean asc;

    /**
     * Creates a new OrderBy node over the tuples from the iterator.
     * 
     * @param orderbyField
     *            the field to which the sort is applied.
     * @param asc
     *            true if the sort order is ascending.
     * @param child
     *            the tuples to sort.
     */
    public OrderBy(int orderbyField, boolean asc, OpIterator child) {
        this.child = child;
        td = child.getTupleDesc();
        this.orderByField = orderbyField;
        this.orderByFieldName = td.getFieldName(orderbyField);
        this.asc = asc;
    }
    
    public boolean isASC()
    {
	return this.asc;
    }
    
    public int getOrderByField()
    {
        return this.orderByField;
    }
    
    public String getOrderFieldName()
    {
	return this.orderByFieldName;
    }
    
    public TupleDesc getTupleDesc() {
        return td;
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        child.open();
        // load all the tuples in a collection, and sort it
        while (child.hasNext())
            childTups.add(child.next());
        childTups.sort(new TupleComparator(orderByField, asc));
        it = childTups.iterator();
        super.open();
    }

    public void close() {
        super.close();
        it = null;
    }

    public void rewind() {
        it = childTups.iterator();
    }

    /**
     * Operator.fetchNext implementation. Returns tuples from the child operator
     * in order
     * 
     * @return The next tuple in the ordering, or null if there are no more
     *         tuples
     */
    protected Tuple fetchNext() throws NoSuchElementException {
        if (it != null && it.hasNext()) {
            return it.next();
        } else
            return null;
    }

    @Override
    public OpIterator[] getChildren() {
        return new OpIterator[] { this.child };
    }

    @Override
    public void setChildren(OpIterator[] children) {
        this.child = children[0];
    }

}

class TupleComparator implements Comparator<Tuple> {
    final int field;
    final boolean asc;

    public TupleComparator(int field, boolean asc) {
        this.field = field;
        this.asc = asc;
    }

    public int compare(Tuple o1, Tuple o2) {
        Field t1 = (o1).getField(field);
        Field t2 = (o2).getField(field);
        if (t1.compare(Predicate.Op.EQUALS, t2))
            return 0;
        if (t1.compare(Predicate.Op.GREATER_THAN, t2))
            return asc ? 1 : -1;
        else
            return asc ? -1 : 1;
    }
    
}
