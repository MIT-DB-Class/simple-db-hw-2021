package simpledb.optimizer;

import simpledb.execution.Predicate;

import java.util.Arrays;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram implements Histogram<Integer> {

    private int   maxVal;
    private int   minVal;
    private int   buckets;
    private int   totalTuples;
    private int   width;
    private int   lastBucketWidth;
    private BinaryIndexedTree bit;

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
        this.minVal = min;
        this.maxVal = max;
        this.buckets = buckets;
        this.bit = new BinaryIndexedTree(buckets);
        int total = max - min + 1;
        this.width = Math.max(total / buckets, 1);
        this.lastBucketWidth = total - (this.width * (buckets - 1));
        this.totalTuples = 0;
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(Integer v) {
        // some code goes here
        if (v < this.minVal || v > this.maxVal) {
            return;
        }

        int bucketIndex = (v - this.minVal) / this.width;
        if (bucketIndex >= this.buckets) {
            return;
        }
        this.totalTuples++;
        this.bit.add(bucketIndex + 1, 1);
    }

    private double estimateGreater(int bucketIndex, int predicateValue, int bucketWidth) {
        if (predicateValue < this.minVal) {
            return 1.0;
        }
        if (predicateValue >= this.maxVal) {
            return 0;
        }

        // As the lab3 doc, result = ((right - val) / bucketWidth) * (bucketTuples / totalTuples)
        int bucketRight = bucketIndex * this.width + this.minVal;
        double bucketRatio = (bucketRight - predicateValue) * 1.0 / bucketWidth;
        double result = bucketRatio * ((bit.index(bucketIndex + 1) * 1.0) / totalTuples);
        int sum = (int) (bit.cnt - bit.query(bucketIndex + 1));
        return (sum * 1.0) / this.totalTuples + result;
    }

    private double estimateEqual(int bucketIndex, int predicateValue, int bucketWidth) {
        if (predicateValue < this.minVal || predicateValue > this.maxVal) {
            return 0;
        }
        // As the lab3 doc, result = (bucketHeight / bucketWidth) / totalTuples
        double result = bit.index(bucketIndex + 1);
        result = result / bucketWidth;
        result = result / this.totalTuples;
        return result;
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
    public double estimateSelectivity(Predicate.Op op, Integer v) {
        final int bucketIndex = Math.min((v - this.minVal) / this.width, this.buckets - 1);
        final int bucketWidth = bucketIndex < this.buckets - 1 ? this.width : this.lastBucketWidth;
        double ans;
        switch (op) {
            case GREATER_THAN:
                ans = estimateGreater(bucketIndex, v, bucketWidth);
                break;
            case EQUALS:
                ans = estimateEqual(bucketIndex, v, bucketWidth);
                break;
            case LESS_THAN:
                ans = 1.0 - estimateGreater(bucketIndex, v, bucketWidth) - estimateEqual(bucketIndex, v, bucketWidth);
                break;
            case LESS_THAN_OR_EQ:
                ans = 1.0 - estimateGreater(bucketIndex, v, bucketWidth);
                break;
            case GREATER_THAN_OR_EQ:
                ans = estimateEqual(bucketIndex, v, bucketWidth) + estimateGreater(bucketIndex, v, bucketWidth);
                break;
            case NOT_EQUALS:
                ans = 1.0 - estimateEqual(bucketIndex, v, bucketWidth);
                break;
            default:
                return -1;
        }
        return ans;
    }

    /**
     * @return
     *     the average selectivity of this histogram.
     *
     *     This is not an indispensable method to implement the basic
     *     join optimization. It may be needed if you want to
     *     implement a more efficient optimization
     * */
    public double avgSelectivity() {
        // some code goes here
        return 1.0;
    }

    /**
     * @return A string describing this histogram, for debugging purposes
     */
    @Override
    public String toString() {
        return "IntHistogram{" + "maxVal=" + maxVal + ", minVal=" + minVal + ", heights="
                + ", buckets=" + buckets + ", totalTuples=" + totalTuples + ", width=" + width + ", lastBucketWidth="
                + lastBucketWidth + '}';
    }
}
