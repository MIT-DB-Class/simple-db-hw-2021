package simpledb.optimizer;

import simpledb.execution.Predicate;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {

    public BinaryIndexedTree bit;

    public int min;
    public int max;
    public int numOfTuples;

    /**
     * Create a new IntHistogram.
     * 
     * This IntHistogram should maintain a histogram of integer values that it receives.
     * It should split the histogram into "buckets" buckets.
     * 
     * The values that are being histogrammed will be provided one-at-a-time through the "addValue()" function.
     * 
     * Your implementation should use space and have execution time that are both
     * constant with respect to the number of values being histogrammed.  For example, you shouldn't 
     * simply store every value that you see in a sorted list.
     * 
     * @param buckets The number of buckets to split the input value into.
     * @param min The minimum integer value that will ever be passed to this class for histogramming
     * @param max The maximum integer value that will ever be passed to this class for histogramming
     */

    public IntHistogram(int buckets, int min, int max) {
    	// some code goes here
        int n = max - min;
        bit = new BinaryIndexedTree(n);
        this.min = min;
        this.max = max;
        this.numOfTuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
        bit.add(v - min + 1, 1);
        this.numOfTuples++;
    }

    /**
     * Estimate the selectivity of a particular predicate and operand on this table.
     * 
     * For example, if "op" is "GREATER_THAN" and "v" is 5, 
     * return your estimate of the fraction of elements that are greater than 5.
     * 
     * @param op Operator
     * @param v Value
     * @return Predicted selectivity of this particular operator and value
     */
    public double estimateSelectivity(Predicate.Op op, int v) {

    	// some code goes here
        double res = -1.0;
        switch (op){
            case EQUALS:
                return (double) (bit.query(v - min + 1) - bit.query(v - min)) / numOfTuples;
            case GREATER_THAN:
                return (double) (numOfTuples - bit.query(v - min + 1)) / numOfTuples;
            case GREATER_THAN_OR_EQ:
                return (double) (numOfTuples - bit.query(v - min )) / numOfTuples;
            case LESS_THAN:
                res = (double) (bit.query(v - min )) / numOfTuples;
                return res;
            case LESS_THAN_OR_EQ:
                return (double) (bit.query(v - min + 1)) / numOfTuples;
            case NOT_EQUALS:
                return (double) (numOfTuples - bit.query(v - min + 1) + bit.query(v - min)) / numOfTuples;
        }


        return res;
    }
    
    /**
     * @return
     *     the average selectivity of this histogram.
     *     
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity()
    {
        // some code goes here
        return 1.0;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {
        // some code goes here
        return null;
    }
}
