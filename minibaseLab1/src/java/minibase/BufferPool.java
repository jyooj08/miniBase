package minibase;

import java.io.*;
import java.util.*;

/**
 * BufferPool manages the reading and writing of pages into memory from
 * disk. Access methods call into it to retrieve pages, and it fetches
 * pages from the appropriate location.
 * <p>
 * The BufferPool is also responsible for locking;  when a transaction fetches
 * a page, BufferPool checks that the transaction has the appropriate
 * locks to read/write the page.
 */
public class BufferPool {
    /** Bytes per page, including header. */
    public static final int PAGE_SIZE = 4096;

    /** Default number of pages passed to the constructor. This is used by
    other classes. BufferPool should use the numPages argument to the
    constructor instead. */
    public static final int DEFAULT_PAGES = 50;

    // TODO : define instance variable
    // hint!! we need to match pid and page, So that we need additional data structure.
    private int NP;
    private HashMap<PageId, Page> buffer;
    private HashMap<PageId, TransactionId> Lock;

    /**
     * Creates a BufferPool that caches up to numPages pages.
     *
     * @param numPages maximum number of pages in this buffer pool.
     */
    public BufferPool(int numPages) {
        // TODO: some code goes here
        NP = numPages;
        buffer = new HashMap<PageId, Page>();
        Lock = new HashMap<PageId, TransactionId>();
    }

    /**
     * Retrieve the specified page with the associated permissions.
     * Will acquire a lock and may block if that lock is held by another
     * transaction.
     * <p>
     * The retrieved page should be looked up in the buffer pool.  If it
     * is present, it should be returned.  If it is not present, it should
     * be added to the buffer pool and returned.  If there is insufficient
     * space in the buffer pool, an page should be evicted and the new page
     * should be added in its place.
     *
     * @param tid the ID of the transaction requesting the page
     * @param pid the ID of the requested page
     * @param perm the requested permissions on the page, see Permissions.java
     */
    public Page getPage(TransactionId tid, PageId pid, Permissions perm)
        throws TransactionAbortedException, DbException {
        // TODO: some code goes here
	// hint, reture value can't be null as if there is no matching page, we will add new page to the buffer pool.
        Page page = buffer.get(pid);
        if(page == null){
        	Page newPage = Database.getCatalog().getDbFile(pid.getTableId()).readPage(pid);
        	buffer.put(pid, newPage);
        	return newPage;
        }
        else
        	return page;
        	

	// + you don't need to implement eviction function for this lab
    }

    /**
     * Releases the lock on a page.
     * Calling this is very risky, and may result in wrong behavior. Think hard
     * about who needs to call this and why, and why they can run the risk of
     * calling it.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param pid the ID of the page to unlock
     */
    public  void releasePage(TransactionId tid, PageId pid) {
        // some code goes here
        // not necessary for proj3
        if(Lock.containsKey(pid)) Lock.remove(pid);
    }

    /**
     * Release all locks associated with a given transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     */
    public void transactionComplete(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj3
        transactionComplete(tid, true);
    }

    /** Return true if the specified transaction has a lock on the specified page */
    public boolean holdsLock(TransactionId tid, PageId p) {
        // some code goes here
        // not necessary for proj3
        if(!Lock.containsKey(p))
        	return false;
        if(Lock.get(p) != tid)
        	return false;
        
        return true;
    }

    /**
     * Commit or abort a given transaction; release all locks associated to
     * the transaction.
     *
     * @param tid the ID of the transaction requesting the unlock
     * @param commit a flag indicating whether we should commit or abort
     */
    public void transactionComplete(TransactionId tid, boolean commit)
        throws IOException {
        // some code goes here
        // not necessary for proj3
        Iterator<PageId> itr = buffer.keySet().iterator(); //buffer? Lock?
        PageId pid;
        if(commit){
        	while(itr.hasNext()){
        		pid = itr.next();
        		if(buffer.get(pid)==tid) flushPage(pid);
        	}
        } else{
        	while(itr.hasNext()){
        		pid=itr.next();
        		if(buffer.get(pid)==tid) discardPage(pid);
        	}
        }
        
        //release all locks associated to tid
        itr = Lock.keySet().iterator();
        while(itr.hasNext()){
        	pid = itr.next();
        	if(Lock.get(pid)==tid) Lock.remove(pid);
        }
    }

    /**
     * Add a tuple to the specified table behalf of transaction tid.  Will
     * acquire a write lock on the page the tuple is added to(Lock 
     * acquisition is not needed for lab3). May block if the lock cannot 
     * be acquired.
     * 
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit, and updates cached versions of any pages that have 
     * been dirtied so that future requests see up-to-date pages. 
     *
     * @param tid the transaction adding the tuple
     * @param tableId the table to add the tuple to
     * @param t the tuple to add
     */
    public void insertTuple(TransactionId tid, int tableId, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // TODO: some code goes here
	// hint: you also have to call insertTuple function of HeapFile,
	// hint2: you don't have to consider about transaction ID write right now, (this maybe needed when implementing lab5 or lab6)
		DbFile file = Database.getCatalog().getDbFile(tableId);
		ArrayList<Page> pageList = file.insertTuple(tid,t);
		Iterator itr = pageList.iterator();
		while(itr.hasNext()){
			Page page = (Page)itr.next();
			buffer.put(page.getId(),page);
		}
    }

    /**
     * Remove the specified tuple from the buffer pool.
     * Will acquire a write lock on the page the tuple is removed from. May block if
     * the lock cannot be acquired.
     *
     * Marks any pages that were dirtied by the operation as dirty by calling
     * their markDirty bit.  Does not need to update cached versions of any pages that have 
     * been dirtied, as it is not possible that a new page was created during the deletion
     * (note difference from addTuple).
     *
     * @param tid the transaction adding the tuple.
     * @param t the tuple to add
     */
    public void deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // TODO: some code goes here
        DbFile file = Database.getCatalog().getDbFile(t.getRecordId().getPageId().getTableId());
        Page page = file.deleteTuple(tid,t);
        buffer.put(page.getId(),page);
    }

    /**
     * Flush all dirty pages to disk.
     * NB: Be careful using this routine -- it writes dirty data to disk so will
     *     break minibase if running in NO STEAL mode.
     */
    public synchronized void flushAllPages() throws IOException {
        // some code goes here
        // not necessary for proj3
        Iterator<PageId> itr = buffer.keySet().iterator(); //buffer? Lock?
        while(itr.hasNext()){
        	PageId pid = itr.next();
        	flushPage(pid);
        }
        	
    }

    /** Remove the specific page id from the buffer pool.
        Needed by the recovery manager to ensure that the
        buffer pool doesn't keep a rolled back page in its
        cache.
    */
    public synchronized void discardPage(PageId pid) {
        // some code goes here
	// not necessary for proj3
		buffer.remove(pid);
    }

    /**
     * Flushes a certain page to disk
     * @param pid an ID indicating the page to flush
     */
    private synchronized  void flushPage(PageId pid) throws IOException {
        // some code goes here
        // not necessary for proj3
        DbFile file = Database.getCatalog().getDbFile(pid.getTableId());
        if(!buffer.containsKey(pid)) return;
        
        Page page = buffer.get(pid);
        TransactionId t = page.isDirty();
        if(t == null) return;
        
        Database.getLogFile().logWrite(t, page.getBeforeImage(),page);
        Database.getLogFile().force();
        file.writePage(page);
        page.setBeforeImage();
        page.markDirty(false, null);
    }

    /** Write all pages of the specified transaction to disk.
     */
    public synchronized  void flushPages(TransactionId tid) throws IOException {
        // some code goes here
        // not necessary for proj3
        Iterator<PageId> itr = buffer.keySet().iterator(); //buffer? Lock?
        while(itr.hasNext()){
        	PageId pid = itr.next();
        	if(buffer.get(pid)==tid) flushPage(pid);
        }
    }

    /**
     * Discards a page from the buffer pool.
     * Flushes the page to disk to ensure dirty pages are updated on disk.
     */
    private synchronized  void evictPage() throws DbException {
        // some code goes here
        // not necessary for proj3
        Iterator<PageId> itr = buffer.keySet().iterator();
        while(itr.hasNext()){
        	PageId pid = itr.next();
        	if(buffer.get(pid).isDirty() != null) continue;
        	
        	try{
        		flushPage(pid);
        	} catch(Exception e) {}
        	
        	buffer.remove(pid);
        	return;
        }
        throw new DbException("");
    }

}
