package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private HashMap<Field, HashMap<String, Integer>> aggregatedValue;
    private int gbfield;
    private int afield;
    private Op op;
    private Type gbfieldType;
    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldType = gbfieldtype;
        this.afield = afield;
        this.op = what;
        aggregatedValue = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field tupGbField = tup.getField(gbfield);
        String newValue = ((StringField)tup.getField(afield)).getValue();
        switch (op){
            case COUNT:
                if(aggregatedValue.containsKey(tupGbField)){
                    HashMap<String, Integer> gbMap = aggregatedValue.get(tupGbField);
                    gbMap.put(newValue, 1);
                }else{
                    HashMap<String, Integer> gbMap = new HashMap<>();
                    gbMap.put(newValue, 1);
                    aggregatedValue.put(tupGbField, gbMap);
                }
            break;
        }

    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public OpIterator iterator() {
        // some code goes here

        return new OpIterator() {
            Tuple[] tuples;
            int curIdx;
            boolean isOpen;
            TupleDesc td;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                isOpen = true;
                curIdx = 0;
                tuples = new Tuple[aggregatedValue.size()];
                int i = 0;
                td = new TupleDesc(new Type[]{gbfieldType, Type.INT_TYPE});
                for(Map.Entry<Field, HashMap<String, Integer>> entrySet: aggregatedValue.entrySet()){
                    Tuple tuple = new Tuple(td);
                    tuple.setField(0, entrySet.getKey());
                    tuple.setField(1, new IntField(entrySet.getValue().size()));
                    tuples[i++] = tuple;

                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(!isOpen)
                    throw new DbException("Not open yet");

                return curIdx < tuples.length;
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(!isOpen )
                    throw new DbException("Not open yey");
                if(curIdx >= tuples.length)
                    throw new NoSuchElementException("Index out of bound");
                return tuples[curIdx++];
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                curIdx = 0;
                isOpen = true;
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                isOpen = false;
            }
        };

    }

}
