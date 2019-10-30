package minibase;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 * 
 * @see minibase.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {
	private File file;
	private TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     * 
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */

    public HeapFile(File f, TupleDesc td) {
        // TODO: some code goes here
        file = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     * 
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // TODO: some code goes here
        return file;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     * 
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // TODO: some code goes here
        return file.getAbsolutePath().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * 
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // TODO: some code goes here
        return td;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        // TODO: some code goes here
	// hint!! to read specific page at arbitrary offset you need random access to the file
       byte[] buf = new byte[BufferPool.PAGE_SIZE];
       HeapPage p = null;
       RandomAccessFile f;
       
       long position = pid.pageNumber()*BufferPool.PAGE_SIZE;
       if(position < 0 || position >= file.length())
           throw new IllegalArgumentException();
       
       try{
           f = new RandomAccessFile(file,"r");
           f.seek(position);
           f.read(buf);
           f.close();
           p = new HeapPage((HeapPageId)pid, buf);
       }catch(IOException e){}
       
       return p;
    }

    // see DbFile.java for javadocs
    //lab3
    public void writePage(Page page) throws IOException {
        // TODO: some code goes here
        RandomAccessFile f = new RandomAccessFile(this.file, "rw");
        PageId pid = page.getId();
        long position = pid.pageNumber()*BufferPool.PAGE_SIZE;
        f.seek(position);
        f.write(page.getPageData(),0,BufferPool.PAGE_SIZE);
        f.close();
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // TODO: some code goes here
	// hint!! you can calculate number of pages as you know PAGE_SIZE
        return (int) file.length() / BufferPool.PAGE_SIZE;
    }

    // see DbFile.java for javadocs
    //lab3
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t)
            throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
        ArrayList<Page> pageList = new ArrayList<Page>();
        HeapPage hpage; HeapPageId pid; int np = numPages();
        
        for(int i=0;i<np;i++){
        	pid = new HeapPageId(this.getId(), i);
        	hpage = ((HeapPage)Database.getBufferPool().getPage(tid, pid, Permissions.READ_ONLY));
        	
        	if(hpage.getNumEmptySlots() > 0){
        		hpage.insertTuple(t);
        		pageList.add(hpage);
        		break;
        	}
        }
        
        if(pageList.isEmpty()){
            pid = new HeapPageId(this.getId(), np);
            hpage = new HeapPage(pid, HeapPage.createEmptyPageData());
            hpage.insertTuple(t);
            this.writePage(hpage);
            pageList.add(hpage);
        }
        return pageList;
    }

    // see DbFile.java for javadocs
    //lab3
    public Page deleteTuple(TransactionId tid, Tuple t) throws DbException,
            TransactionAbortedException {
        // TODO: some code goes here      
        PageId pid = t.getRecordId().getPageId();
        HeapPage hpage = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);     
        hpage.deleteTuple(t);       
        return hpage;
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // TODO: some code goes here
        return new HeapFileIterator(tid, getId(), numPages());
    }

    // TODO: make HeapFileIterator class, you can freely add new methods, variable
    /**
     * Class for iterating over all tuples of this file
     *
     * @see minibase.DbFileIterator
     */
    private class HeapFileIterator implements DbFileIterator {
    	private static final long serialVersionUID = 1L;
    	
    	private int curPage=-1;
    	private Iterator<Tuple> itr = null;
    	private TransactionId tid;
    	private int tableId, numPages;
    
	    /**
	     * Constructor for iterator
	     *
	     * @param tid Transactional of requesting transaction
	     * @param tableId of the HeapFile
	     * @param numPages the number of pages in file
	     */
	    public HeapFileIterator(TransactionId tid, int tableId, int numPages) {
	    	// hint: you can get tuple iterator from HeapPage
	    	 this.tid=tid;
	    	 this.tableId = tableId;
	    	 this.numPages = numPages;
	    }

	    /**
	     * Open it iterator for iteration
	     *
	     * @throws DbException
	     * @throws TransactionAbortedException
	     */
	    public void open() throws DbException, TransactionAbortedException {
	    	curPage++;
	    	if(curPage < numPages){	
	    		itr = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, curPage),Permissions.READ_ONLY)).iterator();
	    		
	    		while(!itr.hasNext() && ++curPage < numPages)
	    		itr = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, curPage),Permissions.READ_ONLY)).iterator();
	    	
	    	}
	    }

	    /**
	     * Check if iterator has next tuple
	     *
	     * @return boolean true if exists
	     * @throws DbException
	     * @throws TransactionAbortedException
	     */
	    public boolean hasNext() throws DbException, TransactionAbortedException {
	    	if(curPage==-1)
	    		return false;
	    	return curPage < numPages;
	    }

	    /**
	     * Get next tuple in this file
	     *
	     * @return
	     * @throws DbException
	     * @throws TrnasactionAbortedException
	     * @throws NoSuchElementException
	     */
	    public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
	    	Tuple result = null;
	    	if(curPage != -1 && hasNext())
	    		result = itr.next();
	    	else
	    		throw new NoSuchElementException();
	    	
	    	while(!itr.hasNext() && ++curPage < numPages)
	    		itr = ((HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(tableId, curPage),Permissions.READ_ONLY)).iterator();
	    	
	    	return result;
	    }

	    /**
	     * Rewind iterator to the start of file
	     *
	     * @throws DbException
	     * @throws TransactionAbortedException
	     */
	    public void rewind() throws DbException, TransactionAbortedException {
	    	close();
	    	open();
	    }

	    /**
	     * Close the iterator
	     */
	    public void close() {
	    	itr=null;
	    	curPage=-1;	
	    }
    }
}

