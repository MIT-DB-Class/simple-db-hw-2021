package simpledb.execution;

import simpledb.optimizer.LogicalPlan;
import simpledb.transaction.TransactionAbortedException;
import simpledb.transaction.TransactionId;
import simpledb.common.DbException;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;

import java.io.*;
import java.util.*;

/**
 * Query is a wrapper class to manage the execution of queries. It takes a query
 * plan in the form of a high level OpIterator (built by initiating the
 * constructors of query plans) and runs it as a part of a specified
 * transaction.
 * 
 * @author Sam Madden
 */

public class Query implements Serializable {

    private static final long serialVersionUID = 1L;

    transient private OpIterator op;
    transient private LogicalPlan logicalPlan;
    final TransactionId tid;
    transient private boolean started = false;

    public TransactionId getTransactionId() {
        return this.tid;
    }

    public void setLogicalPlan(LogicalPlan lp) {
        this.logicalPlan = lp;
    }

    public LogicalPlan getLogicalPlan() {
        return this.logicalPlan;
    }

    public void setPhysicalPlan(OpIterator pp) {
        this.op = pp;
    }

    public OpIterator getPhysicalPlan() {
        return this.op;
    }

    public Query(TransactionId t) {
        tid = t;
    }

    public Query(OpIterator root, TransactionId t) {
        op = root;
        tid = t;
    }

    public void start() throws DbException,
            TransactionAbortedException {
        op.open();

        started = true;
    }

    public TupleDesc getOutputTupleDesc() {
        return this.op.getTupleDesc();
    }

    /** @return true if there are more tuples remaining. */
    public boolean hasNext() throws DbException, TransactionAbortedException {
        return op.hasNext();
    }

    /**
     * Returns the next tuple, or throws NoSuchElementException if the iterator
     * is closed.
     * 
     * @return The next tuple in the iterator
     * @throws DbException
     *             If there is an error in the database system
     * @throws NoSuchElementException
     *             If the iterator has finished iterating
     * @throws TransactionAbortedException
     *             If the transaction is aborted (e.g., due to a deadlock)
     */
    public Tuple next() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        if (!started)
            throw new DbException("Database not started.");

        return op.next();
    }

    /** Close the iterator */
    public void close() {
        op.close();
        started = false;
    }

    public void execute() throws DbException, TransactionAbortedException {
        TupleDesc td = this.getOutputTupleDesc();

        StringBuilder names = new StringBuilder();
        for (int i = 0; i < td.numFields(); i++) {
            names.append(td.getFieldName(i)).append("\t");
        }
        System.out.println(names);
        for (int i = 0; i < names.length() + td.numFields() * 4; i++) {
            System.out.print("-");
        }
        System.out.println();

        this.start();
        int cnt = 0;
        while (this.hasNext()) {
            Tuple tup = this.next();
            System.out.println(tup);
            cnt++;
        }
        System.out.println("\n " + cnt + " rows.");
        this.close();
    }
}
