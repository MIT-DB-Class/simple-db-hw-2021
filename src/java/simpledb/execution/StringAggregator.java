package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private HashMap<Field, Integer> counts;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) throws IllegalArgumentException {
        // some code goes here
        if (what != Op.COUNT) {
            throw new IllegalArgumentException();
        }
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        counts = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gField = new StringField("", Type.STRING_LEN);
        if (gbfield != Aggregator.NO_GROUPING) {
            gField = tup.getField(gbfield);
        }

        if (!counts.containsKey(gField)) {
            counts.put(gField, 0);
        }
        counts.put(gField, counts.get(gField) + 1);
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
        ArrayList<Type> types = new ArrayList<>();
        ArrayList<String> names = new ArrayList<>();
        if (gbfield != Aggregator.NO_GROUPING) {
            types.add(gbfieldtype);
            names.add(gbfieldtype.toString());
        }
        types.add(Type.INT_TYPE);
        names.add(what.toString());

        TupleDesc td = new TupleDesc(types.toArray(new Type[0]), names.toArray(new String[0]));
        ArrayList<Tuple> tuples = new ArrayList<>();
        counts.forEach((key, value) -> {
            Tuple tuple = new Tuple(td);
            if (gbfield != Aggregator.NO_GROUPING) {
                tuple.setField(0, key);
                tuple.setField(1, new IntField(value));
            } else {
                tuple.setField(0, new IntField(value));
            }
            tuples.add(tuple);
        });

        return new TupleIterator(td, tuples);
    }

}
