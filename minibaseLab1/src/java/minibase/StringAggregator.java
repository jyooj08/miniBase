package minibase;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield, afield;
    private Type gbfieldtype;
    private Op op;
    private HashMap<Field, Integer> count;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here        
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        this.count = new HashMap<Field,Integer>();
        
        if(what != Op.COUNT) throw new IllegalArgumentException("support only COUNT");
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field f = null;
        if(gbfield != Aggregator.NO_GROUPING) f = tup.getField(gbfield);
        
        if(!count.containsKey(f)) count.put(f,0);
        	
        int currentCount = count.get(f);
        count.put(f, currentCount+1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc td; String[] names; Type[] types;
        Tuple t;
        

        if(gbfield == Aggregator.NO_GROUPING){
        	names = new String[] {"aggregateValue"};
        	types = new Type[] {Type.INT_TYPE};
        	td = new TupleDesc(types, names);
        	
        	Iterator itr = count.keySet().iterator();
        	while(itr.hasNext()){
        		Field group = (Field)itr.next();
        		int aggregateVal = count.get(group);
        		t = new Tuple(td);
        		t.setField(0, new IntField(aggregateVal));
        		tuples.add(t);
        	}
        
        } else {
        	names = new String[] {"groupValue", "aggregateValue"};
        	types = new Type[] {gbfieldtype, Type.INT_TYPE};
        	td = new TupleDesc(types, names);
        	
        	Iterator itr = count.keySet().iterator();
        	while(itr.hasNext()){
        		Field group = (Field)itr.next();
        		int aggregateVal = count.get(group);
        		t = new Tuple(td);
        		t.setField(0, group);
        		t.setField(1, new IntField(aggregateVal));
        		tuples.add(t);
        	}
        }
        
        return new TupleIterator(td, tuples);
    }

}
