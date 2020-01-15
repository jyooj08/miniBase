package minibase;

import java.util.*;

/** A class to represent a fixed-width histogram over a single integer-based field.
 */
public class IntHistogram {
	
	private class Bucket {
		public int min;
		public int max;
		public int height;
		public int width;
		
		public Bucket(int min, int max) {
			this.min = min;
			this.max = max;
			this.height = 0;
			this.width = max - min + 1;
		}
		
	}
	
	private int numBuckets, count, min, max, floorWidth, ceilingWidth, firstCeiling;
	private Vector<Bucket> buckets;
	
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
    	this.min = min;
    	this.max = max;
    	this.numBuckets = buckets;
    	this.count = 0;
    	double numBucs = this.numBuckets;
    	double numNums = this.max - this.min + 1;
    	this.buckets = new Vector<Bucket>();
    	if (numBucs > numNums) {
    		this.numBuckets = (int)numNums;
    		numBucs = numNums;
    	}
    	int cursor = this.min;
    	this.floorWidth = (int) Math.floor(numNums/numBucs);
    	this.ceilingWidth = (int) Math.ceil(numNums/numBucs);
    	this.firstCeiling = 0;
    	boolean detect = false;
    	
    	while(numBucs > 0){
    		if(!detect && numNums % numBucs == 0){
    			this.firstCeiling = this.buckets.size();
    			detect = true;
    		}
    		
    		int width = (int)numNums / (int)numBucs;
    		numNums -= width;
    		numBucs--;
    		
    		this.buckets.add(new Bucket(cursor, cursor+width-1));
    		cursor+=width;
    	}
    }

    /**
     * Add a value to the set of values that you are keeping a histogram of.
     * @param v Value to add to the histogram
     */
    public void addValue(int v) {
    	// some code goes here
    	int idx = -1;
    	int offset = v - min;
    	if(offset < floorWidth*firstCeiling)
    		idx = offset/floorWidth;
    	else
    		idx = firstCeiling+(offset-firstCeiling*floorWidth)/ceilingWidth;
    	
    	buckets.get(idx).height++;
    	count++;
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
    	if(this.count==0) return Double.MAX_VALUE;
    	
    	double ntups = count;
    	double equal=0, greaterThan=0;
    	
    	// find bucket
    	int idx = -1;
    	int offset = v - min;
    	if(offset < floorWidth*firstCeiling)
    		idx = offset/floorWidth;
    	else
    		idx = firstCeiling+(offset-firstCeiling*floorWidth)/ceilingWidth;
    		
    	//get equal
    	if(v>=min && v<=max){
    		Bucket b = buckets.get(idx);
    		equal = b.height/b.width/ ntups;
    	}
    	
    	//get greaterThan
    	if(v<min) greaterThan=1;
    	else if(v>=max) greaterThan=0;
    	else{
    		Bucket b=null;
    		for(int i=0;i<numBuckets;i++){
    			b=buckets.get(i);
    			if(v>=b.min && v<=b.max){
    				idx=i; break;
    			}
    		}
    		
    		double acc= (b.max-v)/b.width*b.height/ntups;
    		idx++;
    		while(idx<numBuckets){
    			acc += buckets.get(idx++).height/ntups;
    		}
    		greaterThan = acc;
    	}
    	
    	if(op.equals(Predicate.Op.EQUALS)){
    		return equal;
    	} else if(op.equals(Predicate.Op.NOT_EQUALS)){
    		return 1 - equal;
    	} else if(op.equals(Predicate.Op.GREATER_THAN)){
    		return greaterThan;
    	} else if(op.equals(Predicate.Op.GREATER_THAN_OR_EQ)){
    		return equal + greaterThan;
    	} else if(op.equals(Predicate.Op.LESS_THAN)){
    		return 1 - equal - greaterThan;
    	} else if(op.equals(Predicate.Op.LESS_THAN_OR_EQ)){
    		return 1 - greaterThan;
    	}
    	
    	
        return -1.0;
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
    	double acc = 0;
        for (int i = 0; i < numBuckets; i++) {
        	acc += buckets.get(i).height;
        }
        
        return acc / (double)count;
    }
    
    /**
     * @return A string describing this histogram, for debugging purposes
     */
    public String toString() {

        // some code goes here
        return "";
    }
}
