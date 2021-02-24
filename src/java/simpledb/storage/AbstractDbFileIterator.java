package simpledb.storage;

import simpledb.common.DbException;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;

/** Helper for implementing DbFileIterators. Handles hasNext()/next() logic. */
public abstract class AbstractDbFileIterator implements DbFileIterator {

	public boolean hasNext() throws DbException, TransactionAbortedException {
        if (next == null) next = readNext();
        return next != null;
    }

    public Tuple next() throws DbException, TransactionAbortedException,
            NoSuchElementException {
        if (next == null) {
            next = readNext();
            if (next == null) throw new NoSuchElementException();
        }

        Tuple result = next;
        next = null;
        return result;
    }

    /** If subclasses override this, they should call super.close(). */
    public void close() {
        // Ensures that a future call to next() will fail
        next = null;
    }

    /** Reads the next tuple from the underlying source.
    @return the next Tuple in the iterator, null if the iteration is finished. */
    protected abstract Tuple readNext() throws DbException, TransactionAbortedException;

    private Tuple next = null;
}
