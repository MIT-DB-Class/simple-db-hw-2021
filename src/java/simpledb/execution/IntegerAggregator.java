package simpledb.execution;

import simpledb.common.Type;
import simpledb.storage.*;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op what;
    private HashMap<Field, Integer> counts;
    private HashMap<Field, Integer> aggregates;

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
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        counts = new HashMap<>();
        aggregates = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field gField = new IntField(Aggregator.NO_GROUPING);
        IntField aField;
        if (gbfield != Aggregator.NO_GROUPING) {
            gField = tup.getField(gbfield);
        }
        aField = (IntField) tup.getField(afield);

        if (what == Op.SUM || what == Op.AVG) {
            if (!aggregates.containsKey(gField)) {
                aggregates.put(gField, 0);
            }
            aggregates.put(gField, aggregates.get(gField) + aField.getValue());
        }
        if (what == Op.COUNT || what == Op.AVG) {
            if (!counts.containsKey(gField)) {
                counts.put(gField, 0);
            }
            counts.put(gField, counts.get(gField) + 1);
        }
        if (what == Op.MIN) {
            if (!aggregates.containsKey(gField)) {
                aggregates.put(gField, Integer.MAX_VALUE);
            }
            aggregates.put(gField, Math.min(aggregates.get(gField), aField.getValue()));
        } else if (what == Op.MAX) {
            if (!aggregates.containsKey(gField)) {
                aggregates.put(gField, Integer.MIN_VALUE);
            }
            aggregates.put(gField, Math.max(aggregates.get(gField), aField.getValue()));
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
        if (what == Op.MAX || what == Op.MIN || what == Op.SUM) {
            aggregates.forEach((key, value) -> {
                Tuple tuple = new Tuple(td);
                if (gbfield != Aggregator.NO_GROUPING) {
                    tuple.setField(0, key);
                    tuple.setField(1, new IntField(value));
                } else {
                    tuple.setField(0, new IntField(value));
                }
                tuples.add(tuple);
            });
        } else if (what == Op.COUNT) {
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
        } else if (what == Op.AVG) {
            aggregates.forEach((key, value) -> {
                Tuple tuple = new Tuple(td);
                if (gbfield != Aggregator.NO_GROUPING) {
                    tuple.setField(0, key);
                    tuple.setField(1, new IntField(value/counts.get(key)));
                } else {
                    tuple.setField(0, new IntField(value/counts.get(key)));
                }
                tuples.add(tuple);
            });
        }

        return new TupleIterator(td, tuples);
    }

}
