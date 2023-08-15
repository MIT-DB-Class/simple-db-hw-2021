package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.Tuple;
import simpledb.storage.TupleDesc;
import simpledb.transaction.TransactionAbortedException;

import java.util.NoSuchElementException;


/**
 * The Aggregation operator that computes an aggregate (e.g., sum, avg, max,
 * min). Note that we only support aggregates over a single column, grouped by a
 * single column.
 */
public class Aggregate extends Operator {

    private static final long serialVersionUID = 1L;
    private final int afield;
    private final int gfield;
    private final Aggregator.Op aop;
    private final Aggregator agg;
    private OpIterator child;
    private TupleDesc td;
    private OpIterator it;

    /**
     * Constructor.
     * <p>
     * Implementation hint: depending on the type of afield, you will want to
     * construct an {@link IntegerAggregator} or {@link StringAggregator} to help
     * you with your implementation of readNext().
     *
     * @param child  The OpIterator that is feeding us tuples.
     * @param afield The column over which we are computing an aggregate.
     * @param gfield The column over which we are grouping the result, or -1 if
     *               there is no grouping
     * @param aop    The aggregation operator to use
     */
    public Aggregate(OpIterator child, int afield, int gfield, Aggregator.Op aop) {
        // some code goes here
        this.child = child;
        this.afield = afield;
        this.gfield = gfield;
        this.aop = aop;
        Type gbFieldType;
        Type afieldType;
        if(gfield == Aggregator.NO_GROUPING){
            gbFieldType = null;
        }else{
            gbFieldType = child.getTupleDesc().getFieldType(gfield);
        }
        afieldType = child.getTupleDesc().getFieldType(afield);
        if(afieldType == Type.INT_TYPE){
            agg = new IntegerAggregator(gfield, gbFieldType, afield, aop);

        }else{
            agg = new StringAggregator(gfield, gbFieldType, afield, aop);
        }
        td = (gfield == Aggregator.NO_GROUPING) ?
                new TupleDesc(new Type[]{afieldType},
                        new String[]{"(" + nameOfAggregatorOp(aop) + ") " + "(" + aggregateFieldName() + ")"}) :
                new TupleDesc(new Type[]{gbFieldType, afieldType},
                        new String[]{groupFieldName(),
                                "(" + nameOfAggregatorOp(aop) + ") " + "(" + aggregateFieldName() + ")"});
    }

    /**
     * @return If this aggregate is accompanied by a groupby, return the groupby
     * field index in the <b>INPUT</b> tuples. If not, return
     * {@link Aggregator#NO_GROUPING}
     */
    public int groupField() {
        // some code goes here
        if (gfield != -1)
            return gfield;
        else
            return Aggregator.NO_GROUPING;
    }

    /**
     * @return If this aggregate is accompanied by a group by, return the name
     * of the groupby field in the <b>OUTPUT</b> tuples. If not, return
     * null;
     */
    public String groupFieldName() {
        // some code goes here
        if (gfield != -1)
            return child.getTupleDesc().getFieldName(gfield);
        else
            return null;
    }

    /**
     * @return the aggregate field
     */
    public int aggregateField() {
        // some code goes here
        return afield;
    }

    /**
     * @return return the name of the aggregate field in the <b>OUTPUT</b>
     * tuples
     */
    public String aggregateFieldName() {
        // some code goes here
        return child.getTupleDesc().getFieldName(afield);
    }

    /**
     * @return return the aggregate operator
     */
    public Aggregator.Op aggregateOp() {
        // some code goes here
        return aop;
    }

    public static String nameOfAggregatorOp(Aggregator.Op aop) {
        return aop.toString();
    }

    @Override
    public void open() throws NoSuchElementException, DbException,
            TransactionAbortedException {
        super.open();
        child.open();
        while(child.hasNext()){
            agg.mergeTupleIntoGroup(child.next());
        }
        it = agg.iterator();
        it.open();
    }

    /**
     * Returns the next tuple. If there is a group by field, then the first
     * field is the field by which we are grouping, and the second field is the
     * result of computing the aggregate. If there is no group by field, then
     * the result tuple should contain one field representing the result of the
     * aggregate. Should return null if there are no more tuples.
     */
    @Override
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        if(it.hasNext()) return it.next();
        return null;
    }

    @Override
    public void rewind() throws DbException, TransactionAbortedException {
        it.rewind();
    }

    /**
     * Returns the TupleDesc of this Aggregate. If there is no group by field,
     * this will have one field - the aggregate column. If there is a group by
     * field, the first field will be the group by field, and the second will be
     * the aggregate value column.
     * <p>
     * The name of an aggregate column should be informative. For example:
     * "aggName(aop) (child_td.getFieldName(afield))" where aop and afield are
     * given in the constructor, and child_td is the TupleDesc of the child
     * iterator.
     */
    @Override
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    public String getAggOpName(Aggregator.Op aop){
        return aop.toString();
    }

    public void close() {
        it.close();
    }

    @Override
    public OpIterator[] getChildren() {
        // some code goes here
        return new OpIterator[] {child};
    }

    @Override
    public void setChildren(OpIterator[] children) {
        // some code goes here
        this.child = children[0];
    }

}
