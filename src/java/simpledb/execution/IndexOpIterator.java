package simpledb.execution;
import simpledb.transaction.TransactionAbortedException;
import simpledb.common.DbException;

import java.util.*;

/** IndexDBIterator is the interface that index access methods
    implement in SimpleDb.
*/
public interface IndexOpIterator extends OpIterator {
    /** Open the access method such that when getNext is called, it
        iterates through the tuples that satisfy ipred.
        @param ipred The predicate that is used to scan the index.
    */
    void open(IndexPredicate ipred)
        throws NoSuchElementException, DbException, TransactionAbortedException;

    /** Begin a new index scan with the specified predicate.
        @param ipred The predicate that is used to scan the index.
    */
    void rewind(IndexPredicate ipred)
        throws DbException, TransactionAbortedException;
}
