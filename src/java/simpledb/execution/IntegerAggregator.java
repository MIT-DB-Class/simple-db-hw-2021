package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.IntField;
import simpledb.storage.Tuple;
import simpledb.storage.Field;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final int afield;
    private final Type gbfieldType;
    private final Op op;
    private HashMap<Field, Integer> aggregatedValueMap;
    private HashMap<Field, Integer> groupbyCounter;
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfieldType = gbfieldtype;
        this.gbfield = gbfield;
        this.afield = afield;
        this.op = what;
        aggregatedValueMap = new HashMap<>();
        if(gbfield != Aggregator.NO_GROUPING){
            groupbyCounter = new HashMap<>();
        }
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        Field gbField = (gbfield == Aggregator.NO_GROUPING) ? null : tup.getField(gbfield);
        Integer newValue = ((IntField) tup.getField(afield)).getValue();
        switch (op){
            case MIN:
                aggregatedValueMap.merge(gbField, newValue, Math::min);
                break;
            case MAX:
                aggregatedValueMap.merge(gbField, newValue, (oldMax, val) -> Math.max(oldMax, val));
                break;
            case SUM:
                aggregatedValueMap.merge(gbField, newValue, Integer::sum);
                break;
            case COUNT:
                aggregatedValueMap.merge(gbField, 1, Integer::sum);
                break;
            case AVG:
                aggregatedValueMap.merge(gbField, newValue, Integer::sum);
                groupbyCounter.merge(gbField, 1, Integer::sum);
                break;

        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        return new OpIterator() {
            private int curIdx = 0;
            private boolean isOpen = false;
            private TupleDesc td;
            private Tuple[] tuples;
            @Override
            public void open() throws DbException, TransactionAbortedException {
                tuples = new Tuple[aggregatedValueMap.size()];
                isOpen = true;
                if(gbfield == Aggregator.NO_GROUPING){
                    td = new TupleDesc(new Type[] {Type.INT_TYPE});
                    tuples[0] = new Tuple(td);
                    IntField value = new IntField((op == Op.AVG) ? aggregatedValueMap.get(null) / groupbyCounter.get(null) : aggregatedValueMap.get(null));
                    tuples[0].setField(0, value);
                }else{
                    td = new TupleDesc(new Type[] {gbfieldType, Type.INT_TYPE});
                    int i = 0;
                    for(Map.Entry<Field, Integer> entrySet: aggregatedValueMap.entrySet()){
                        tuples[i] = new Tuple(td);
                        IntField value = new IntField((op == Op.AVG) ? entrySet.getValue() / groupbyCounter.get(entrySet.getKey()) : entrySet.getValue());
                        tuples[i].setField(0, entrySet.getKey());
                        tuples[i].setField(1, value);
                        i++;
                    }
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if(isOpen)
                    return curIdx < aggregatedValueMap.size();
                throw new DbException("Operator not open yet");
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if(curIdx >= tuples.length){
                    throw new NoSuchElementException("Index beyond the size of map");
                }else{
                    return tuples[curIdx++];
                }
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if(isOpen)
                    curIdx = 0;
                else
                    throw new DbException("Operator not open yet");
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
