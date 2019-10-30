package minibase;

/**
 * The delete operator. Delete reads tuples from its child operator and removes
 * them from the table they belong to.
 */
public class Delete extends Operator {

    private static final long serialVersionUID = 1L;

    // hint: implementation of Delete.java is not that much different from implementing Insert.java.
    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * 
     * @param t
     *            The transaction this delete runs in
     * @param child
     *            The child operator from which to read tuples for deletion
     */
     private TransactionId tid;
     private DbIterator itr;
     private TupleDesc td;
     private boolean isDeleted;
     private BufferPool buf;
     
    public Delete(TransactionId t, DbIterator child) {
        // TODO: some code goes here
        this.tid=t;
        this.itr=child;
        this.buf=Database.getBufferPool();
        isDeleted=false;
        td = child.getTupleDesc();
    }

    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return td;
    }

    public void open() throws DbException, TransactionAbortedException {
        // TODO: some code goes here
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
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * 
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // TODO: some code goes here
        if(isDeleted) return null;
        int count = 0;
        while(itr.hasNext()){
        	try{
        		Tuple tuple = itr.next();
        		buf.deleteTuple(tid, tuple);    			
    			count++;
    		} catch(Exception e) {}
        }
        Tuple result = new Tuple(td);
        result.setField(0,new IntField(count));
        isDeleted=true;
        return result;
    }

    @Override
    public DbIterator[] getChildren() {
        // TODO: some code goes here
        return new DbIterator[]{itr};
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // TODO: some code goes here
        itr = children[0];
    }

}
