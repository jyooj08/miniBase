package minibase;

/**
 * Inserts tuples read from the child operator into the tableid specified in the
 * constructor
 */
public class Insert extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor.
     * 
     * @param t
     *            The transaction running the insert.
     * @param child
     *            The child operator from which to read tuples to be inserted.
     * @param tableid
     *            The table in which to insert tuples.
     * @throws DbException
     *             if TupleDesc of child differs from table into which we are to
     *             insert.
     */
    private TransactionId tid;
    private DbIterator itr;
    private int tableId; 
    private TupleDesc td;
    private boolean isInserted; 
    private BufferPool buf;
    
    public Insert(TransactionId t,DbIterator child, int tableid)
            throws DbException {
        // TODO: some code goes here
        this.tid=t;
        this.itr=child;
        this.tableId=tableid;
        this.isInserted = false;
        this.buf=Database.getBufferPool();
        this.td = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO some code goes here
	// hint: you have to consider parent class as well
		super.open();
		itr.open();
    }

    public void close() {
        // TODO: some code goes here
        itr.close();
        super.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        itr.rewind();
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool. An
     * instances of BufferPool is available via Database.getBufferPool(). Note
     * that insert DOES NOT need check to see if a particular tuple is a
     * duplicate before inserting it.
     * 
     * @return A 1-field tuple containing the number of inserted records, or
     *         null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
	// hint: insert Tuples passed by iterator (child) to Buffer.
        if(isInserted) return null;
        
        int count=0;
        while(itr.hasNext()){
        	try{
        		Tuple tuple = itr.next();
        		buf.insertTuple(tid, tableId, tuple); 			
    			count++;
    		}catch(Exception e){}
        }
        Tuple result = new Tuple(td);
        result.setField(0,new IntField(count));
        isInserted=true;
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        // TODO: some code goes here
	// hint! there is only one element you can pass through DbIterator[]
        return new DbIterator[] {itr};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // TODO: some code goes here
        itr = children[0];
    }
}
